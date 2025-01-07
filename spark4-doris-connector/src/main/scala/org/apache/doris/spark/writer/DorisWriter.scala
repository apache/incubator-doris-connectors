// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.spark.writer

import org.apache.doris.spark.cfg.{ConfigurationOptions, SparkSettings}
import org.apache.doris.spark.load.{CommitMessage, CopyIntoLoader, Loader, StreamLoader}
import org.apache.doris.spark.sql.Utils
import org.apache.doris.spark.txn.TransactionHandler
import org.apache.doris.spark.txn.listener.DorisTransactionListener
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.CollectionAccumulator
import org.slf4j.{Logger, LoggerFactory}

import java.time.Duration
import java.util.Objects
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success}

class DorisWriter(settings: SparkSettings,
                  txnAcc: CollectionAccumulator[CommitMessage],
                  isStreaming: Boolean) extends Serializable {

  private val logger: Logger = LoggerFactory.getLogger(classOf[DorisWriter])

  private val sinkTaskPartitionSize: Integer = settings.getIntegerProperty(ConfigurationOptions.DORIS_SINK_TASK_PARTITION_SIZE)
  private val loadMode: String = settings.getProperty(ConfigurationOptions.LOAD_MODE,ConfigurationOptions.DEFAULT_LOAD_MODE)
  private val sinkTaskUseRepartition: Boolean = settings.getProperty(ConfigurationOptions.DORIS_SINK_TASK_USE_REPARTITION,
    ConfigurationOptions.DORIS_SINK_TASK_USE_REPARTITION_DEFAULT.toString).toBoolean

  private val maxRetryTimes: Int = settings.getIntegerProperty(ConfigurationOptions.DORIS_SINK_MAX_RETRIES,
    ConfigurationOptions.SINK_MAX_RETRIES_DEFAULT)
  private val batchSize: Integer = settings.getIntegerProperty(ConfigurationOptions.DORIS_SINK_BATCH_SIZE,
    ConfigurationOptions.SINK_BATCH_SIZE_DEFAULT)
  private val batchInterValMs: Integer = settings.getIntegerProperty(ConfigurationOptions.DORIS_SINK_BATCH_INTERVAL_MS,
    ConfigurationOptions.DORIS_SINK_BATCH_INTERVAL_MS_DEFAULT)

  if (maxRetryTimes > 0) {
    logger.info(s"batch retry enabled, size is $batchSize, interval is $batchInterValMs")
  }

  private val enable2PC: Boolean = settings.getBooleanProperty(ConfigurationOptions.DORIS_SINK_ENABLE_2PC,
    ConfigurationOptions.DORIS_SINK_ENABLE_2PC_DEFAULT)

  private val loader: Loader = generateLoader

  private val sinkTxnRetries = settings.getIntegerProperty(ConfigurationOptions.DORIS_SINK_TXN_RETRIES,
    ConfigurationOptions.DORIS_SINK_TXN_RETRIES_DEFAULT)

  private val sinkTxnIntervalMs = settings.getIntegerProperty(ConfigurationOptions.DORIS_SINK_TXN_INTERVAL_MS,
    ConfigurationOptions.DORIS_SINK_TXN_INTERVAL_MS_DEFAULT)

  private val txnHandler: TransactionHandler = TransactionHandler(loader, sinkTxnRetries, sinkTxnIntervalMs)

  /**
   * write data in batch mode
   *
   * @param dataFrame source dataframe
   */
  def write(dataFrame: DataFrame): Unit = {
    doWrite(dataFrame, loader.load)
  }

  private def doWrite(dataFrame: DataFrame, loadFunc: (Iterator[InternalRow], StructType) => Option[CommitMessage]): Unit = {
    // do not add spark listener when job is streaming mode
    if (enable2PC && !isStreaming) {
      dataFrame.sparkSession.sparkContext.addSparkListener(new DorisTransactionListener(txnAcc, txnHandler))
    }
    var resultDataFrame = dataFrame
    if (Objects.nonNull(sinkTaskPartitionSize)) {
      resultDataFrame = if (sinkTaskUseRepartition) dataFrame.repartition(sinkTaskPartitionSize) else dataFrame.coalesce(sinkTaskPartitionSize)
    }

    val resultRdd = resultDataFrame.queryExecution.toRdd
    val schema = resultDataFrame.schema

    resultRdd.foreachPartition(iterator => {
      while (iterator.hasNext) {
        val batchIterator = new BatchIterator(iterator, batchSize, maxRetryTimes > 0)
        val retry = Utils.retry[Option[CommitMessage], Exception](maxRetryTimes, Duration.ofMillis(batchInterValMs.toLong), logger) _
        retry(loadFunc(batchIterator, schema))(batchIterator.reset()) match {
          case Success(msg) =>
            if (enable2PC) handleLoadSuccess(msg, txnAcc)
            batchIterator.close()
          case Failure(e) =>
            if (enable2PC) handleLoadFailure(txnAcc)
            batchIterator.close()
            throw e
        }
      }

    })

  }

  private def handleLoadSuccess(msg: Option[CommitMessage], acc: CollectionAccumulator[CommitMessage]): Unit = {
    acc.add(msg.get)
  }

  private def handleLoadFailure(acc: CollectionAccumulator[CommitMessage]): Unit = {
    // if task run failed, acc value will not be returned to driver,
    // should abort all pre committed transactions inside the task
    logger.info("load task failed, start aborting previously pre-committed transactions")
    if (acc.isZero) {
      logger.info("no pre-committed transactions, skip abort")
      return
    }

    try txnHandler.abortTransactions(acc.value.asScala.toList)
    catch {
      case e: Exception => throw e
    }
    finally acc.reset()
  }

  /**
   * iterator for batch load
   * if retry time is greater than zero, enable batch retry and put batch data into buffer
   *
   * @param iterator         parent iterator
   * @param batchSize        batch size
   * @param batchRetryEnable whether enable batch retry
   */
  private class BatchIterator(iterator: Iterator[InternalRow], batchSize: Int, batchRetryEnable: Boolean) extends Iterator[InternalRow] {

    private val buffer: ArrayBuffer[InternalRow] =
      if (batchRetryEnable) new ArrayBuffer[InternalRow](batchSize) else ArrayBuffer.empty[InternalRow]

    private var recordCount = 0

    private var isReset = false

    override def hasNext: Boolean = {
      if (recordCount < batchSize) {
        if (isReset) {
          recordCount < buffer.size
        } else {
          iterator.hasNext
        }
      } else {
        false
      }
    }

    override def next(): InternalRow = {
      recordCount += 1
      if (batchRetryEnable) {
        if (isReset) {
          readBuffer()
        } else {
          writeBufferAndReturn()
        }
      } else {
        iterator.next
      }
    }

    /**
     * reset record count for re-read
     */
    def reset(): Unit = {
      recordCount = 0
      isReset = buffer.nonEmpty
      if (isReset) {
        logger.info("buffer is not empty and batch iterator is reset")
      }
    }

    /**
     * clear buffer when buffer is not empty
     */
    def close(): Unit = {
      if (buffer.nonEmpty) {
        buffer.clear()
        logger.info("buffer is cleared and batch iterator is closed")
      }
    }

    private def readBuffer(): InternalRow = {
      if (recordCount == buffer.size) {
        logger.debug("read buffer end, recordCount:{}, bufferSize: {}", recordCount, buffer.size)
        isReset = false
      }
      buffer(recordCount - 1)
    }

    private def writeBufferAndReturn(): InternalRow = {
      val elem = iterator.next.copy
      buffer += elem
      elem
    }

  }

  @throws[IllegalArgumentException]
  private def generateLoader: Loader = {
    loadMode match {
      case "stream_load" => new StreamLoader(settings, isStreaming)
      case "copy_into" => new CopyIntoLoader(settings, isStreaming)
      case _ => throw new IllegalArgumentException(s"Unsupported load mode: $loadMode")
    }
  }

  def getTransactionHandler: TransactionHandler = txnHandler


}
