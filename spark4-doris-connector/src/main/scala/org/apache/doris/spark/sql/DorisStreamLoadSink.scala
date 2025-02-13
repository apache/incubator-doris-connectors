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

package org.apache.doris.spark.sql

import org.apache.doris.spark.cfg.SparkSettings
import org.apache.doris.spark.load.CommitMessage
import org.apache.doris.spark.txn.listener.DorisTxnStreamingQueryListener
import org.apache.doris.spark.writer.DorisWriter
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.slf4j.{Logger, LoggerFactory}

private[sql] class DorisStreamLoadSink(sqlContext: SQLContext, settings: SparkSettings) extends Sink with Serializable {

  private val logger: Logger = LoggerFactory.getLogger(classOf[DorisStreamLoadSink].getName)
  @volatile private var latestBatchId = -1L

  // accumulator for transaction handling
  private val acc = sqlContext.sparkContext.collectionAccumulator[CommitMessage]("StreamTxnAcc")
  private val writer = new DorisWriter(settings, acc, true)

  // add listener for structured streaming
  sqlContext.streams.addListener(new DorisTxnStreamingQueryListener(acc, writer.getTransactionHandler))

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    if (batchId <= latestBatchId) {
      logger.info(s"Skipping already committed batch $batchId")
    } else {
      writer.write(data)
      latestBatchId = batchId
    }
  }

  override def toString: String = "DorisStreamLoadSink"
}
