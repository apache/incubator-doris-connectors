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

import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.junit.{Assert, Test}

import java.sql.{Date, Timestamp}

class SchemaUtilsTest {

  @Test
  def rowColumnValueTest(): Unit = {

    val row = InternalRow(
      1,
      CatalystTypeConverters.convertToCatalyst(Date.valueOf("2023-09-08")),
      CatalystTypeConverters.convertToCatalyst(Timestamp.valueOf("2023-09-08 17:00:00")),
      ArrayData.toArrayData(Array(1, 2, 3)),
      CatalystTypeConverters.convertToCatalyst(Map[String, String]("a" -> "1")),
      InternalRow(UTF8String.fromString("a"), 1)
    )

    val schema = new StructType().add("c1", IntegerType)
      .add("c2", DateType)
      .add("c3", TimestampType)
      .add("c4", ArrayType.apply(IntegerType))
      .add("c5", MapType.apply(StringType, StringType))
      .add("c6", StructType.apply(Seq(StructField("a", StringType), StructField("b", IntegerType))))

    Assert.assertEquals(1, SchemaUtils.rowColumnValue(row, 0, schema.fields(0).dataType))
    Assert.assertEquals("2023-09-08", SchemaUtils.rowColumnValue(row, 1, schema.fields(1).dataType))
    Assert.assertEquals("2023-09-08 17:00:00.0", SchemaUtils.rowColumnValue(row, 2, schema.fields(2).dataType))
    Assert.assertEquals("[1,2,3]", SchemaUtils.rowColumnValue(row, 3, schema.fields(3).dataType))
    Assert.assertEquals("{\"a\":\"1\"}", SchemaUtils.rowColumnValue(row, 4, schema.fields(4).dataType))
    Assert.assertEquals("{\"a\":\"a\",\"b\":1}", SchemaUtils.rowColumnValue(row, 5, schema.fields(5).dataType))

  }

}
