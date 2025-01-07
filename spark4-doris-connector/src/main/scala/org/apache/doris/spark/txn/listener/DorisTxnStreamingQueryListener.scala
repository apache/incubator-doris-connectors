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

package org.apache.doris.spark.txn.listener

import org.apache.doris.spark.cfg.SparkSettings
import org.apache.doris.spark.load.CommitMessage
import org.apache.doris.spark.txn.TransactionHandler
import org.apache.spark.internal.Logging
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.util.CollectionAccumulator

import scala.collection.JavaConverters._
import scala.collection.mutable

class DorisTxnStreamingQueryListener(txnAcc: CollectionAccumulator[CommitMessage], txnHandler: TransactionHandler)
  extends StreamingQueryListener with Logging {

  override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {}

  override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
    // do commit transaction when each batch ends
    val messages: mutable.Buffer[CommitMessage] = txnAcc.value.asScala
    if (messages.isEmpty) {
      log.warn("job run succeed, but there is no pre-committed txn")
      return
    }
    log.info(s"batch[${event.progress.batchId}] run succeed, start committing transactions")
    try txnHandler.commitTransactions(messages.toList)
    catch {
      case e: Exception => throw e
    } finally txnAcc.reset()
    log.info(s"batch[${event.progress.batchId}] commit transaction success")
  }


  override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
    val messages: mutable.Buffer[CommitMessage] = txnAcc.value.asScala
    // if job failed, abort all pre committed transactions
    if (event.exception.nonEmpty) {
      if (messages.isEmpty) {
        log.warn("job run failed, but there is no pre-committed txn")
        return
      }
      log.info("job run failed, start aborting transactions")
      try txnHandler.abortTransactions(messages.toList)
      catch {
        case e: Exception => throw e
      } finally txnAcc.reset()
      log.info("abort transaction success")
    }
  }

}
