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

package org.apache.doris.spark.testcase

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

import java.sql.{Date, Timestamp}
import scala.collection.mutable.ListBuffer

// This object is used to test writing from spark into doris with arrow format,
// And it will be executed in doris's pipeline.
object TestStreamLoadForArrowType {
  val spark: SparkSession = SparkSession.builder().master("local[1]").getOrCreate()
  var dorisFeNodes = "127.0.0.1:8030"
  var dorisUser = "root"
  val dorisPwd = ""
  var databaseName = ""

  def main(args: Array[String]): Unit = {

    dorisFeNodes = args(0)
    dorisUser = args(1)
    databaseName = args(2)

    testDataframeWritePrimitiveType()
    testDataframeWriteArrayTypes()
    testDataframeWriteMapType()
    testDataframeWriteStructType()

    spark.stop()
  }


  def testDataframeWritePrimitiveType(): Unit = {
    /*

  CREATE TABLE `spark_connector_primitive` (
        `id` int(11) NOT NULL,
        `c_bool` boolean NULL,
        `c_tinyint` tinyint NULL,
        `c_smallint` smallint NULL,
        `c_int` int NULL,
        `c_bigint` bigint NULL,
        `c_largeint` largeint NULL,
        `c_float` float NULL,
        `c_double` double NULL,
        `c_decimal` DECIMAL(10, 5) NULL,
        `c_date` date NULL,
        `c_datetime` datetime(6) NULL,
        `c_char` char(10) NULL,
        `c_varchar` varchar(10) NULL,
        `c_string` string NULL
      ) ENGINE=OLAP
      DUPLICATE KEY(`id`)
      COMMENT 'OLAP'
      DISTRIBUTED BY HASH(`id`) BUCKETS 1
      PROPERTIES (
      "replication_allocation" = "tag.location.default: 1"
      );

    +------------+----------------+------+-------+---------+-------+
    | Field      | Type           | Null | Key   | Default | Extra |
    +------------+----------------+------+-------+---------+-------+
    | id         | INT            | No   | true  | NULL    |       |
    | c_bool     | BOOLEAN        | Yes  | false | NULL    | NONE  |
    | c_tinyint  | TINYINT        | Yes  | false | NULL    | NONE  |
    | c_smallint | SMALLINT       | Yes  | false | NULL    | NONE  |
    | c_int      | INT            | Yes  | false | NULL    | NONE  |
    | c_bigint   | BIGINT         | Yes  | false | NULL    | NONE  |
    | c_largeint | LARGEINT       | Yes  | false | NULL    | NONE  |
    | c_float    | FLOAT          | Yes  | false | NULL    | NONE  |
    | c_double   | DOUBLE         | Yes  | false | NULL    | NONE  |
    | c_decimal  | DECIMAL(10, 5) | Yes  | false | NULL    | NONE  |
    | c_date     | DATE           | Yes  | false | NULL    | NONE  |
    | c_datetime | DATETIME(6)    | Yes  | false | NULL    | NONE  |
    | c_char     | CHAR(10)       | Yes  | false | NULL    | NONE  |
    | c_varchar  | VARCHAR(10)    | Yes  | false | NULL    | NONE  |
    | c_string   | TEXT           | Yes  | false | NULL    | NONE  |
    +------------+----------------+------+-------+---------+-------+

     */

    val schema = new StructType()
      .add("id", IntegerType)
      .add("c_bool", BooleanType)
      .add("c_tinyint", ByteType)
      .add("c_smallint", ShortType)
      .add("c_int", IntegerType)
      .add("c_bigint", LongType)
      .add("c_largeint", StringType)
      .add("c_float", FloatType)
      .add("c_double", DoubleType)
      .add("c_decimal", DecimalType.apply(10, 5))
      .add("c_date", DateType)
      .add("c_datetime", TimestampType)
      .add("c_char", StringType)
      .add("c_varchar", StringType)
      .add("c_string", StringType)

    val row = Row(
      1,
      true,
      1.toByte,
      2.toShort,
      3,
      4.toLong,
      "123456789",
      6.6.floatValue(),
      7.7.doubleValue(),
      Decimal.apply(3.12),
      Date.valueOf("2023-09-08"),
      Timestamp.valueOf("2023-09-08 17:12:34.123456"),
      "char",
      "varchar",
      "string"
    )


    val inputList = ListBuffer[Row]()
    for (a <- 0 until 7) {
      inputList.append(row)
    }

    val rdd = spark.sparkContext.parallelize(inputList.toSeq, 1)
    val df = spark.createDataFrame(rdd, schema).toDF()

    df.write
      .format("doris")
      .option("doris.fenodes", dorisFeNodes)
      .option("user", dorisUser)
      .option("password", dorisPwd)
      .option("doris.table.identifier", s"$databaseName.spark_connector_primitive")
      .option("doris.sink.batch.size", 3)
      .option("doris.sink.properties.format", "arrow")
      .option("doris.sink.max-retries", 0)
      .save()
  }

  def testDataframeWriteArrayTypes(): Unit = {
    /*

  CREATE TABLE `spark_connector_array` (
        `id` int(11) NOT NULL,
        `c_array_boolean` ARRAY<boolean> NULL,
        `c_array_tinyint` ARRAY<tinyint> NULL,
        `c_array_smallint` ARRAY<smallint> NULL,
        `c_array_int` ARRAY<int> NULL,
        `c_array_bigint` ARRAY<bigint> NULL,
        `c_array_largeint` ARRAY<largeint> NULL,
        `c_array_float` ARRAY<float> NULL,
        `c_array_double` ARRAY<double> NULL,
        `c_array_decimal` ARRAY<DECIMAL(10, 5)> NULL,
        `c_array_date` ARRAY<date> NULL,
        `c_array_datetime` ARRAY<datetime(6)> NULL,
        `c_array_char` ARRAY<char(10)> NULL,
        `c_array_varchar` ARRAY<varchar(10)> NULL,
        `c_array_string` ARRAY<string> NULL
      ) ENGINE=OLAP
      DUPLICATE KEY(`id`)
      COMMENT 'OLAP'
      DISTRIBUTED BY HASH(`id`) BUCKETS 1
      PROPERTIES (
      "replication_allocation" = "tag.location.default: 1"
      );

    +------------------+-------------------------+------+-------+---------+-------+
    | Field            | Type                    | Null | Key   | Default | Extra |
    +------------------+-------------------------+------+-------+---------+-------+
    | id               | INT                     | No   | true  | NULL    |       |
    | c_array_boolean  | ARRAY<BOOLEAN>          | Yes  | false | []      | NONE  |
    | c_array_tinyint  | ARRAY<TINYINT>          | Yes  | false | []      | NONE  |
    | c_array_smallint | ARRAY<SMALLINT>         | Yes  | false | []      | NONE  |
    | c_array_int      | ARRAY<INT>              | Yes  | false | []      | NONE  |
    | c_array_bigint   | ARRAY<BIGINT>           | Yes  | false | []      | NONE  |
    | c_array_largeint | ARRAY<LARGEINT>         | Yes  | false | []      | NONE  |
    | c_array_float    | ARRAY<FLOAT>            | Yes  | false | []      | NONE  |
    | c_array_double   | ARRAY<DOUBLE>           | Yes  | false | []      | NONE  |
    | c_array_decimal  | ARRAY<DECIMALV3(10, 5)> | Yes  | false | []      | NONE  |
    | c_array_date     | ARRAY<DATEV2>           | Yes  | false | []      | NONE  |
    | c_array_datetime | ARRAY<DATETIMEV2(6)>    | Yes  | false | []      | NONE  |
    | c_array_char     | ARRAY<CHAR(10)>         | Yes  | false | []      | NONE  |
    | c_array_varchar  | ARRAY<VARCHAR(10)>      | Yes  | false | []      | NONE  |
    | c_array_string   | ARRAY<TEXT>             | Yes  | false | []      | NONE  |
    +------------------+-------------------------+------+-------+---------+-------+

     */

    val schema = new StructType()
      .add("id", IntegerType)
      .add("c_array_boolean", ArrayType(BooleanType))
      .add("c_array_tinyint", ArrayType(ByteType))
      .add("c_array_smallint", ArrayType(ShortType))
      .add("c_array_int", ArrayType(IntegerType))
      .add("c_array_bigint", ArrayType(LongType))
      .add("c_array_largeint", ArrayType(StringType))
      .add("c_array_float", ArrayType(FloatType))
      .add("c_array_double", ArrayType(DoubleType))
      .add("c_array_decimal", ArrayType(DecimalType.apply(10, 5)))
      .add("c_array_date", ArrayType(DateType))
      .add("c_array_datetime", ArrayType(TimestampType))
      .add("c_array_char", ArrayType(StringType))
      .add("c_array_varchar", ArrayType(StringType))
      .add("c_array_string", ArrayType(StringType))

    val row = Row(
      1,
      Array(true, false, false, true, true),
      Array(1.toByte, 2.toByte, 3.toByte),
      Array(2.toShort, 12.toShort, 32.toShort),
      Array(3, 4, 5, 6),
      Array(4.toLong, 5.toLong, 6.toLong),
      Array("123456789", "987654321", "123789456"),
      Array(6.6.floatValue(), 6.7.floatValue(), 7.8.floatValue()),
      Array(7.7.doubleValue(), 8.8.doubleValue(), 8.9.floatValue()),
      Array(Decimal.apply(3.12), Decimal.apply(1.12345)),
      Array(Date.valueOf("2023-09-08"), Date.valueOf("2027-10-28")),
      Array(Timestamp.valueOf("2023-09-08 17:12:34.123456"), Timestamp.valueOf("2024-09-08 18:12:34.123456")),
      Array("char", "char2"),
      Array("varchar", "varchar2"),
      Array("string", "string2")
    )


    val inputList = ListBuffer[Row]()
    for (a <- 0 until 7) {
      inputList.append(row)
    }

    val rdd = spark.sparkContext.parallelize(inputList.toSeq, 1)
    val df = spark.createDataFrame(rdd, schema).toDF()

    df.write
      .format("doris")
      .option("doris.fenodes", dorisFeNodes)
      .option("user", dorisUser)
      .option("password", dorisPwd)
      .option("doris.table.identifier", s"$databaseName.spark_connector_array")
      .option("doris.sink.batch.size", 30)
      .option("doris.sink.properties.format", "arrow")
      .option("doris.sink.max-retries", 0)
      .save()
  }

  def testDataframeWriteMapType(): Unit = {
    /*

  CREATE TABLE `spark_connector_map` (
        `id` int(11) NOT NULL,
        `c_map_bool` Map<boolean,boolean> NULL,
        `c_map_tinyint` Map<tinyint,tinyint> NULL,
        `c_map_smallint` Map<smallint,smallint> NULL,
        `c_map_int` Map<int,int> NULL,
        `c_map_bigint` Map<bigint,bigint> NULL,
        `c_map_largeint` Map<largeint,largeint> NULL,
        `c_map_float` Map<float,float> NULL,
        `c_map_double` Map<double,double> NULL,
        `c_map_decimal` Map<DECIMAL(10, 5),DECIMAL(10, 5)> NULL,
        `c_map_date` Map<date,date> NULL,
        `c_map_datetime` Map<datetime(6),datetime(6)> NULL,
        `c_map_char` Map<char(10),char(10)> NULL,
        `c_map_varchar` Map<varchar(10),varchar(10)> NULL,
        `c_map_string` Map<string,string> NULL
      ) ENGINE=OLAP
      DUPLICATE KEY(`id`)
      COMMENT 'OLAP'
      DISTRIBUTED BY HASH(`id`) BUCKETS 1
      PROPERTIES (
      "replication_allocation" = "tag.location.default: 1"
      );

    +----------------+----------------------------------------+------+-------+---------+-------+
    | Field          | Type                                   | Null | Key   | Default | Extra |
    +----------------+----------------------------------------+------+-------+---------+-------+
    | id             | INT                                    | No   | true  | NULL    |       |
    | c_map_bool     | MAP<BOOLEAN,BOOLEAN>                   | Yes  | false | NULL    | NONE  |
    | c_map_tinyint  | MAP<TINYINT,TINYINT>                   | Yes  | false | NULL    | NONE  |
    | c_map_smallint | MAP<SMALLINT,SMALLINT>                 | Yes  | false | NULL    | NONE  |
    | c_map_int      | MAP<INT,INT>                           | Yes  | false | NULL    | NONE  |
    | c_map_bigint   | MAP<BIGINT,BIGINT>                     | Yes  | false | NULL    | NONE  |
    | c_map_largeint | MAP<LARGEINT,LARGEINT>                 | Yes  | false | NULL    | NONE  |
    | c_map_float    | MAP<FLOAT,FLOAT>                       | Yes  | false | NULL    | NONE  |
    | c_map_double   | MAP<DOUBLE,DOUBLE>                     | Yes  | false | NULL    | NONE  |
    | c_map_decimal  | MAP<DECIMALV3(10, 5),DECIMALV3(10, 5)> | Yes  | false | NULL    | NONE  |
    | c_map_date     | MAP<DATEV2,DATEV2>                     | Yes  | false | NULL    | NONE  |
    | c_map_datetime | MAP<DATETIMEV2(6),DATETIMEV2(6)>       | Yes  | false | NULL    | NONE  |
    | c_map_char     | MAP<CHAR(10),CHAR(10)>                 | Yes  | false | NULL    | NONE  |
    | c_map_varchar  | MAP<VARCHAR(10),VARCHAR(10)>           | Yes  | false | NULL    | NONE  |
    | c_map_string   | MAP<TEXT,TEXT>                         | Yes  | false | NULL    | NONE  |
    +----------------+----------------------------------------+------+-------+---------+-------+

     */

    val schema = new StructType()
      .add("id", IntegerType)
      .add("c_map_bool", MapType(BooleanType, BooleanType))
      .add("c_map_tinyint", MapType(ByteType, ByteType))
      .add("c_map_smallint", MapType(ShortType, ShortType))
      .add("c_map_int", MapType(IntegerType, IntegerType))
      .add("c_map_bigint", MapType(LongType, LongType))
      .add("c_map_largeint", MapType(StringType, StringType))
      .add("c_map_float", MapType(FloatType, FloatType))
      .add("c_map_double", MapType(DoubleType, DoubleType))
      .add("c_map_decimal", MapType(DecimalType.apply(10, 5), DecimalType.apply(10, 5)))
      .add("c_map_date", MapType(DateType, DateType))
      .add("c_map_datetime", MapType(TimestampType, TimestampType))
      .add("c_map_char", MapType(StringType, StringType))
      .add("c_map_varchar", MapType(StringType, StringType))
      .add("c_map_string", MapType(StringType, StringType))

    val row = Row(
      1,
      Map(true -> false, false -> true, true -> true),
      Map(1.toByte -> 2.toByte, 3.toByte -> 4.toByte),
      Map(2.toShort -> 4.toShort, 5.toShort -> 6.toShort),
      Map(3 -> 4, 7 -> 8),
      Map(4.toLong -> 5.toLong, 1.toLong -> 2.toLong),
      Map("123456789" -> "987654321", "789456123" -> "456789123"),
      Map(6.6.floatValue() -> 8.8.floatValue(), 9.9.floatValue() -> 10.1.floatValue()),
      Map(7.7.doubleValue() -> 1.1.doubleValue(), 2.2 -> 3.3.doubleValue()),
      Map(Decimal.apply(3.12) -> Decimal.apply(1.23), Decimal.apply(2.34) -> Decimal.apply(5.67)),
      Map(Date.valueOf("2023-09-08") -> Date.valueOf("2024-09-08"), Date.valueOf("1023-09-08") -> Date.valueOf("2023-09-08")),
      Map(Timestamp.valueOf("1023-09-08 17:12:34.123456") -> Timestamp.valueOf("2023-09-08 17:12:34.123456"), Timestamp.valueOf("3023-09-08 17:12:34.123456") -> Timestamp.valueOf("4023-09-08 17:12:34.123456")),
      Map("char" -> "char2", "char2" -> "char3"),
      Map("varchar" -> "varchar2", "varchar3" -> "varchar4"),
      Map("string" -> "string2", "string3" -> "string4")
    )


    val inputList = ListBuffer[Row]()
    for (a <- 0 until 7) {
      inputList.append(row)
    }

    val rdd = spark.sparkContext.parallelize(inputList.toSeq, 1)
    val df = spark.createDataFrame(rdd, schema).toDF()

    df.write
      .format("doris")
      .option("doris.fenodes", dorisFeNodes)
      .option("user", dorisUser)
      .option("password", dorisPwd)
      .option("doris.table.identifier", s"$databaseName.spark_connector_map")
      .option("doris.sink.batch.size", 3)
      .option("doris.sink.properties.format", "arrow")
      .option("doris.sink.max-retries", 0)
      .save()
  }

  def testDataframeWriteStructType(): Unit = {
    /*

CREATE TABLE `spark_connector_struct` (
          `id` int NOT NULL,
          `st` STRUCT<
              `c_bool`:boolean,
              `c_tinyint`:tinyint(4),
              `c_smallint`:smallint(6),
              `c_int`:int(11),
              `c_bigint`:bigint(20),
              `c_largeint`:largeint(40),
              `c_float`:float,
              `c_double`:double,
              `c_decimal`:DECIMAL(10, 5),
              `c_date`:date,
              `c_datetime`:datetime(6),
              `c_char`:char(10),
              `c_varchar`:varchar(10),
              `c_string`:string
            > NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );

    +-------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------+-------+---------+-------+
    | Field | Type                                                                                                                                                                                                                                                           | Null | Key   | Default | Extra |
    +-------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------+-------+---------+-------+
    | id    | INT                                                                                                                                                                                                                                                            | No   | true  | NULL    |       |
    | st    | STRUCT<c_bool:BOOLEAN,c_tinyint:TINYINT,c_smallint:SMALLINT,c_int:INT,c_bigint:BIGINT,c_largeint:LARGEINT,c_float:FLOAT,c_double:DOUBLE,c_decimal:DECIMALV3(10, 5),c_date:DATEV2,c_datetime:DATETIMEV2(6),c_char:CHAR(10),c_varchar:VARCHAR(10),c_string:TEXT> | Yes  | false | NULL    | NONE  |
    +-------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------+-------+---------+-------+

     */

    val st = new StructType()
      .add("c_bool", BooleanType)
      .add("c_tinyint", ByteType)
      .add("c_smallint", ShortType)
      .add("c_int", IntegerType)
      .add("c_bigint", LongType)
      .add("c_largeint", StringType)
      .add("c_float", FloatType)
      .add("c_double", DoubleType)
      .add("c_decimal", DecimalType.apply(10, 5))
      .add("c_date", DateType)
      .add("c_datetime", TimestampType)
      .add("c_char", StringType)
      .add("c_varchar", StringType)
      .add("c_string", StringType)

    val schema = new StructType()
      .add("id", IntegerType)
      .add("st", st)

    val row = Row(
      1,
      Row(true,
        1.toByte,
        2.toShort,
        3,
        4.toLong,
        "123456789",
        6.6.floatValue(),
        7.7.doubleValue(),
        Decimal.apply(3.12),
        Date.valueOf("2023-09-08"),
        Timestamp.valueOf("2023-09-08 17:12:34.123456"),
        "char",
        "varchar",
        "string")
    )


    val inputList = ListBuffer[Row]()
    for (a <- 0 until 7) {
      inputList.append(row)
    }

    val rdd = spark.sparkContext.parallelize(inputList.toSeq, 1)
    val df = spark.createDataFrame(rdd, schema).toDF()

    df.write
      .format("doris")
      .option("doris.fenodes", dorisFeNodes)
      .option("user", dorisUser)
      .option("password", dorisPwd)
      .option("doris.table.identifier", s"$databaseName.spark_connector_struct")
      .option("doris.sink.batch.size", 3)
      .option("doris.sink.properties.format", "arrow")
      .option("doris.sink.max-retries", 0)
      .save()
  }
}
