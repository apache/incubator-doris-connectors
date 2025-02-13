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

package org.apache.doris.spark.read

import org.apache.doris.spark.client.entity.DorisReaderPartition
import org.apache.doris.spark.client.read.{DorisFlightSqlReader, DorisReader, DorisThriftReader}
import org.apache.doris.spark.config.DorisConfig
import org.apache.doris.spark.util.RowConvertors
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader}
import org.apache.spark.sql.types.StructType

import scala.language.implicitConversions

class DorisPartitionReader(inputPartition: InputPartition, schema: StructType, mode: ScanMode, config: DorisConfig)
  extends PartitionReader[InternalRow] {

  private implicit def toReaderPartition(inputPart: DorisInputPartition): DorisReaderPartition = {
    val tablets = inputPart.tablets.map(java.lang.Long.valueOf)
    new DorisReaderPartition(inputPart.database, inputPart.table, inputPart.backend, tablets,
      inputPart.opaquedQueryPlan, inputPart.readCols, inputPart.predicates, inputPart.limit, config, inputPart.datetimeJava8ApiEnabled)
  }

  private lazy val reader: DorisReader = {
    mode match {
      case ScanMode.THRIFT => new DorisThriftReader(inputPartition.asInstanceOf[DorisInputPartition])
      case ScanMode.ARROW => new DorisFlightSqlReader(inputPartition.asInstanceOf[DorisInputPartition])
      case _ => throw new UnsupportedOperationException()
    }
  }

  private val datetimeJava8ApiEnabled: Boolean = inputPartition.asInstanceOf[DorisInputPartition].datetimeJava8ApiEnabled

  override def next(): Boolean = reader.hasNext

  override def get(): InternalRow = {
    val values = reader.next().asInstanceOf[Array[Any]]
    val row = new GenericInternalRow(schema.length)
    if (values.nonEmpty) {
      values.zipWithIndex.foreach {
        case (value, index) =>
          if (value == null) row.setNullAt(index)
          else row.update(index, RowConvertors.convertValue(value, schema.fields(index).dataType, datetimeJava8ApiEnabled))
      }
    }
    row
  }

  override def close(): Unit = {
    if (reader != null) {
      reader.close()
    }
  }

}
