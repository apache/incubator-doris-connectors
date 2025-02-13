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

package org.apache.doris.spark.load

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType

/**
 * Loader, interface class for write data to doris
 */
trait Loader extends Serializable {

  /**
   * execute load
   *
   * @param iterator row data iterator
   * @param schema row data schema
   * @return commit message
   */
  def load(iterator: Iterator[InternalRow], schema: StructType): Option[CommitMessage]

  /**
   * commit transaction
   *
   * @param msg commit message
   */
  def commit(msg: CommitMessage): Unit

  /**
   * abort transaction
   *
   * @param msg commit message
   */
  def abort(msg: CommitMessage): Unit

}

/**
 * Commit message class
 *
 * @param value message value
 */
case class CommitMessage(value: Any) extends Serializable