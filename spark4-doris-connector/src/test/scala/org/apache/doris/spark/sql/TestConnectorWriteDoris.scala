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

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{ArrayType, IntegerType, MapType, StringType, StructField, StructType}
import org.junit.{Ignore, Test}

// This test need real connect info to run.
// Set the connect info before comment out this @Ignore
@Ignore
class TestConnectorWriteDoris {

  val dorisFeNodes = "127.0.0.1:8030"
  val dorisUser = "root"
  val dorisPwd = ""
  val dorisTable = "test.test_order"

  val kafkaServers = "127.0.0.1:9093"
  val kafkaTopics = "test_spark"

  @Test
  def listDataWriteTest(): Unit = {
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    val df = spark.createDataFrame(Seq(
      ("1", 100, "待付款"),
      ("2", 200, "待发货"),
      ("3", 300, "已收货")
    )).toDF("order_id", "order_amount", "order_status")
    df.write
      .format("doris")
      .option("doris.fenodes", dorisFeNodes)
      .option("doris.table.identifier", dorisTable)
      .option("user", dorisUser)
      .option("password", dorisPwd)
      .option("sink.batch.size", 2)
      .option("sink.max-retries", 2)
      .save()
    spark.stop()
  }


  @Test
  def csvDataWriteTest(): Unit = {
    val csvFile =
      Thread.currentThread().getContextClassLoader.getResource("data.csv").toString
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    val df = spark.read
      .option("header", "true") // uses the first line as names of columns
      .option("inferSchema", "true") // infers the input schema automatically from data
      .csv(csvFile)
    df.createTempView("tmp_tb")
    val doris = spark.sql(
      s"""
        |CREATE TEMPORARY VIEW test_lh
        |USING doris
        |OPTIONS(
        | "table.identifier"="test.test_lh",
        | "fenodes"="${dorisFeNodes}",
        | "user"="${dorisUser}",
        | "password"="${dorisPwd}"
        |);
        |""".stripMargin)
    spark.sql(
      """
        |insert into test_lh select  name,gender,age from tmp_tb ;
        |""".stripMargin)
    spark.stop()
  }

  @Test
  def structuredStreamingWriteTest(): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .getOrCreate()
    val df = spark.readStream
      .option("kafka.bootstrap.servers", kafkaServers)
      .option("startingOffsets", "latest")
      .option("subscribe", kafkaTopics)
      .format("kafka")
      .option("failOnDataLoss", false)
      .load()

    df.selectExpr("CAST(value AS STRING)")
      .writeStream
      .format("doris")
      .option("checkpointLocation", "/tmp/test")
      .option("doris.table.identifier", dorisTable)
      .option("doris.fenodes", dorisFeNodes)
      .option("user", dorisUser)
      .option("password", dorisPwd)
      .option("sink.batch.size", 2)
      .option("sink.max-retries", 2)
      .start().awaitTermination()
  }

  @Test
  def jsonWriteTest(): Unit = {
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    val df = spark.createDataFrame(Seq(
      ("1", 100, "待付款"),
      ("2", 200, "待发货"),
      ("3", 300, "已收货")
    )).toDF("order_id", "order_amount", "order_status")
    df.write
      .format("doris")
      .option("doris.fenodes", dorisFeNodes)
      .option("doris.table.identifier", dorisTable)
      .option("user", dorisUser)
      .option("password", dorisPwd)
      .option("sink.batch.size", 2)
      .option("sink.max-retries", 2)
      .option("sink.properties.format", "json")
      .save()
    spark.stop()
  }


  /**
   * correct data in doris
   * +------+--------------+-------------+------------+
   * | id   | a            | m           | s          |
   * +------+--------------+-------------+------------+
   * |    1 | [1, 2, 3]    | {"k1":1}    | {10, "ab"} |
   * |    2 | [4, 5, 6]    | {"k2":3}    | NULL       |
   * |    3 | [7, 8, 9]    | NULL        | {20, "cd"} |
   * |    4 | NULL         | {"k3":5}    | {30, "ef"} |
   * |    5 | [10, 11, 12] | {"k4":7}    | {40, NULL} |
   * |    6 | [13, 14, 15] | {"k5":NULL} | {50, NULL} |
   * |    7 | []           | {}          | {60, "gh"} |
   * +------+--------------+-------------+------------+
   */
  @Test
  def complexWriteTest(): Unit = {
    val spark = SparkSession.builder().master("local[1]").getOrCreate()

    val data = Array(
      Row(1, Array(1,2,3), Map("k1" -> 1), Row(10, "ab")),
      Row(2, Array(4,5,6), Map("k2" -> 3), null),
      Row(3, Array(7,8,9), null, Row(20, "cd")),
      Row(4, null, Map("k3" -> 5), Row(30, "ef")),
      Row(5, Array(10,11,12), Map("k4" -> 7), Row(40, null)),
      Row(6, Array(13,14,15), Map("k5" -> null), Row(50, "{10, \"ab\"}")),
      Row(7, Array(), Map(), Row(60, "gh"))
    )

    val schema = StructType(
      Array(
        StructField("id", IntegerType),
        StructField("a", ArrayType(IntegerType)),
        StructField("m", MapType(StringType, IntegerType)),
        StructField("s", StructType(Seq(StructField("a", IntegerType),StructField("b", StringType))))
      )
    )

    val rdd = spark.sparkContext.parallelize(data)
    val df = spark.createDataFrame(rdd, schema)
    df.printSchema()
    df.show(false)
    df.write
      .format("doris")
      .option("doris.fenodes", dorisFeNodes)
      .option("doris.table.identifier", dorisTable)
      .option("user", dorisUser)
      .option("password", dorisPwd)
      .option("sink.batch.size", 2)
      .option("sink.max-retries", 0)
      .option("sink.properties.format", "json")
      .save()
    spark.stop()
  }
}
