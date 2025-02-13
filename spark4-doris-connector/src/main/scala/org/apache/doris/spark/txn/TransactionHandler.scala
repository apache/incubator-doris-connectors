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

package org.apache.doris.spark.txn

import org.apache.doris.spark.load.{CommitMessage, Loader}
import org.apache.doris.spark.sql.Utils
import org.apache.spark.internal.Logging

import java.time.Duration
import scala.collection.mutable
import scala.util.{Failure, Success}

/**
 * load transaction handler
 *
 * @param loader loader
 * @param retries max retry times
 * @param interval retry interval ms
 */
class TransactionHandler(loader: Loader, retries: Int, interval: Int) extends Logging with Serializable {

  /**
   * commit transactions
   *
   * @param messages commit message list
   */
  def commitTransactions(messages: List[CommitMessage]): Unit = {
    log.debug(s"start to commit transactions, count ${messages.size}")
    val (failedTxnIds, ex) = messages.map(commitTransaction).filter(_._1.nonEmpty)
      .map(e => (e._1.get, e._2.get))
      .aggregate((mutable.Buffer[Any](), new Exception))(
        (z, r) => ((z._1 += r._1).asInstanceOf[mutable.Buffer[Any]], r._2), (r1, r2) => (r1._1 ++ r2._1, r2._2))
    if (failedTxnIds.nonEmpty) {
      log.error("uncommitted txn ids: {}", failedTxnIds.mkString("[", ",", "]"))
      throw ex
    }
  }

  /**
   * commit single transaction
   *
   * @param msg commit message
   * @return
   */
  private def commitTransaction(msg: CommitMessage): (Option[Any], Option[Exception]) = {
    Utils.retry(retries, Duration.ofMillis(interval), log) {
      loader.commit(msg)
    }() match {
      case Success(_) => (None, None)
      case Failure(e: Exception) => (Option(msg.value), Option(e))
    }
  }

  /**
   * abort transactions
   *
   * @param messages commit message list
   */
  def abortTransactions(messages: List[CommitMessage]): Unit = {
    log.debug(s"start to abort transactions, count ${messages.size}")
    var ex: Option[Exception] = None
    val failedTxnIds = messages.map(msg =>
      Utils.retry(retries, Duration.ofMillis(interval), log) {
        loader.abort(msg)
      }() match {
        case Success(_) => None
        case Failure(e: Exception) =>
          ex = Option(e)
          Option(msg.value)
      }).filter(_.nonEmpty).map(_.get)
    if (failedTxnIds.nonEmpty) {
      log.error("not aborted txn ids: {}", failedTxnIds.mkString("[", ",", "]"))
    }
  }

}

object TransactionHandler {
  def apply(loader: Loader, retries: Int, interval: Int): TransactionHandler =
    new TransactionHandler(loader, retries, interval)
}
