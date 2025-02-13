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

package org.apache.doris.spark.util

import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.types.{ArrayType, DataTypes, Decimal, DecimalType, MapType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String
import org.junit.Assert
import org.junit.jupiter.api.Test

import java.sql.{Date, Timestamp}
import java.time.LocalDate

class RowConvertorsTest {

  @Test def convertToCsv(): Unit = {

    val row = InternalRow(
      1,
      2.3.toFloat,
      4.5,
      6.toShort,
      7L,
      Decimal(BigDecimal(8910.11), 20, 4),
      CatalystTypeConverters.convertToCatalyst(Date.valueOf("2024-01-01")),
      CatalystTypeConverters.convertToCatalyst(Timestamp.valueOf("2024-01-01 12:34:56")),
      ArrayData.toArrayData(Array(1, 2, 3)),
      CatalystTypeConverters.convertToCatalyst(Map[String, String]("a" -> "1")),
      InternalRow(UTF8String.fromString("a"), 1),
      UTF8String.fromString("test")
    )

    val schema = StructType(Seq(
      StructField("c0", DataTypes.IntegerType),
      StructField("c1", DataTypes.FloatType),
      StructField("c2", DataTypes.DoubleType),
      StructField("c3", DataTypes.ShortType),
      StructField("c4", DataTypes.LongType),
      StructField("c5", DecimalType(20,4)),
      StructField("c6", DataTypes.DateType),
      StructField("c7", DataTypes.TimestampType),
      StructField("c8", ArrayType(DataTypes.IntegerType)),
      StructField("c9", MapType(DataTypes.StringType, DataTypes.StringType)),
      StructField("c10", StructType(Seq(StructField("a", DataTypes.StringType), StructField("b", DataTypes.IntegerType)))),
      StructField("c11", DataTypes.StringType)
    ))
    val res = RowConvertors.convertToCsv(row, schema, ",")
    Assert.assertEquals("1,2.3,4.5,6,7,8910.1100,2024-01-01,2024-01-01 12:34:56.0,[1,2,3],{\"a\":\"1\"},{\"a\":\"a\",\"b\":1},test", res)
  }

  @Test def convertToJson(): Unit = {

    val row = InternalRow(
      1,
      2.3.toFloat,
      4.5,
      6.toShort,
      7L,
      Decimal(BigDecimal(8910.11), 20, 4),
      CatalystTypeConverters.convertToCatalyst(Date.valueOf("2024-01-01")),
      CatalystTypeConverters.convertToCatalyst(Timestamp.valueOf("2024-01-01 12:34:56")),
      ArrayData.toArrayData(Array(1, 2, 3)),
      CatalystTypeConverters.convertToCatalyst(Map[String, String]("a" -> "1")),
      InternalRow(UTF8String.fromString("a"), 1),
      UTF8String.fromString("test")
    )

    val schema = StructType(Seq(
      StructField("c0", DataTypes.IntegerType),
      StructField("c1", DataTypes.FloatType),
      StructField("c2", DataTypes.DoubleType),
      StructField("c3", DataTypes.ShortType),
      StructField("c4", DataTypes.LongType),
      StructField("c5", DecimalType(20,4)),
      StructField("c6", DataTypes.DateType),
      StructField("c7", DataTypes.TimestampType),
      StructField("c8", ArrayType(DataTypes.IntegerType)),
      StructField("c9", MapType(DataTypes.StringType, DataTypes.StringType)),
      StructField("c10", StructType(Seq(StructField("a", DataTypes.StringType), StructField("b", DataTypes.IntegerType)))),
      StructField("c11", DataTypes.StringType)
    ))
    val res = RowConvertors.convertToJson(row, schema)
    Assert.assertEquals("{\"c0\":1,\"c1\":2.3,\"c10\":\"{\\\"a\\\":\\\"a\\\",\\\"b\\\":1}\",\"c11\":\"test\"," +
      "\"c2\":4.5,\"c3\":6,\"c4\":7,\"c5\":8910.1100,\"c6\":\"2024-01-01\"," +
      "\"c7\":\"2024-01-01 12:34:56.0\",\"c8\":\"[1,2,3]\",\"c9\":\"{\\\"a\\\":\\\"1\\\"}\"}", res)

  }

  @Test def convertValue(): Unit = {

    Assert.assertTrue(RowConvertors.convertValue(1, DataTypes.IntegerType, false).isInstanceOf[Int])
    Assert.assertTrue(RowConvertors.convertValue(2.3.toFloat, DataTypes.FloatType, false).isInstanceOf[Float])
    Assert.assertTrue(RowConvertors.convertValue(4.5, DataTypes.DoubleType, false).isInstanceOf[Double])
    Assert.assertTrue(RowConvertors.convertValue(6.toShort, DataTypes.ShortType, false).isInstanceOf[Short])
    Assert.assertTrue(RowConvertors.convertValue(7L, DataTypes.LongType, false).isInstanceOf[Long])
    Assert.assertTrue(RowConvertors.convertValue(Decimal(BigDecimal(8910.11), 20, 4), DecimalType(20, 4), false).isInstanceOf[Decimal])
    Assert.assertTrue(RowConvertors.convertValue(Date.valueOf("2024-01-01"), DataTypes.DateType, false).isInstanceOf[Int])
    Assert.assertTrue(RowConvertors.convertValue(LocalDate.now(), DataTypes.DateType, true).isInstanceOf[Int])
    Assert.assertTrue(RowConvertors.convertValue("2024-01-01 12:34:56", DataTypes.TimestampType, false).isInstanceOf[Long])
    Assert.assertTrue(RowConvertors.convertValue(Map[String, String]("a" -> "1"), MapType(DataTypes.StringType, DataTypes.StringType), false).isInstanceOf[MapData])
    Assert.assertTrue(RowConvertors.convertValue("test", DataTypes.StringType, false).isInstanceOf[UTF8String])

  }

}
