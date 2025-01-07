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

import org.apache.doris.spark.DorisTestBase
import org.apache.spark.sql.SparkSession
import org.junit.Test

import java.sql.{ResultSet, Statement}
import scala.collection.mutable.ListBuffer

class DorisWriterITCase extends DorisTestBase {

  val DATABASE: String = "test"
  val TABLE_CSV: String = "tbl_csv"
  val TABLE_JSON: String = "tbl_json"
  val TABLE_JSON_TBL: String = "tbl_json_tbl"

  @Test
  @throws[Exception]
  def testSinkCsvFormat(): Unit = {
    initializeTable(TABLE_CSV)
    val session = SparkSession.builder().master("local[*]").getOrCreate()
    val df = session.createDataFrame(Seq(
      ("doris_csv", 1),
      ("spark_csv", 2)
    )).toDF("name", "age")
    df.write
      .format("doris")
      .option("doris.fenodes", DorisTestBase.getFenodes)
      .option("doris.table.identifier", DATABASE + "." + TABLE_CSV)
      .option("user", DorisTestBase.USERNAME)
      .option("password", DorisTestBase.PASSWORD)
      .option("sink.properteis.column_separator", ",")
      .option("sink.properteis.line_delimiter", "\n")
      .option("sink.properteis.format", "csv")
      .save()
    session.stop()

    Thread.sleep(10000)
    val actual = queryResult(TABLE_CSV);
    val expected = ListBuffer(List("doris_csv", 1), List("spark_csv", 2))
    assert(expected.equals(actual))
  }

  @Test
  @throws[Exception]
  def testSinkJsonFormat(): Unit = {
    initializeTable(TABLE_JSON)
    val session = SparkSession.builder().master("local[*]").getOrCreate()
    val df = session.createDataFrame(Seq(
      ("doris_json", 1),
      ("spark_json", 2)
    )).toDF("name", "age")
    df.write
      .format("doris")
      .option("doris.fenodes", DorisTestBase.getFenodes)
      .option("doris.table.identifier", DATABASE + "." + TABLE_JSON)
      .option("user", DorisTestBase.USERNAME)
      .option("password", DorisTestBase.PASSWORD)
      .option("sink.properteis.read_json_by_line", "true")
      .option("sink.properteis.format", "json")
      .save()
    session.stop()

    Thread.sleep(10000)
    val actual = queryResult(TABLE_JSON);
    val expected = ListBuffer(List("doris_json", 1), List("spark_json", 2))
    assert(expected.equals(actual))
  }

  @Test
  @throws[Exception]
  def testSQLSinkFormat(): Unit = {
    initializeTable(TABLE_JSON_TBL)
    val session = SparkSession.builder().master("local[*]").getOrCreate()
    val df = session.createDataFrame(Seq(
      ("doris_tbl", 1),
      ("spark_tbl", 2)
    )).toDF("name", "age")
    df.createTempView("mock_source")
    session.sql(
      s"""
         |CREATE TEMPORARY VIEW test_sink
         |USING doris
         |OPTIONS(
         | "table.identifier"="${DATABASE + "." + TABLE_JSON_TBL}",
         | "fenodes"="${DorisTestBase.getFenodes}",
         | "user"="${DorisTestBase.USERNAME}",
         | "password"="${DorisTestBase.PASSWORD}"
         |);
         |""".stripMargin)
    session.sql(
      """
        |insert into test_sink select  name,age from mock_source ;
        |""".stripMargin)
    session.stop()

    Thread.sleep(10000)
    val actual = queryResult(TABLE_JSON_TBL);
    val expected = ListBuffer(List("doris_tbl", 1), List("spark_tbl", 2))
    assert(expected.equals(actual))
  }

  private def queryResult(table: String): ListBuffer[Any] = {
    val actual = new ListBuffer[Any]
    try {
      val sinkStatement: Statement = DorisTestBase.connection.createStatement
      try {
        val sinkResultSet: ResultSet = sinkStatement.executeQuery(String.format("select name,age from %s.%s order by 1", DATABASE, table))
        while (sinkResultSet.next) {
          val row = List(sinkResultSet.getString("name"), sinkResultSet.getInt("age"))
          actual += row
        }
      } finally if (sinkStatement != null) sinkStatement.close()
    }
    actual
  }

  @throws[Exception]
  private def initializeTable(table: String): Unit = {
    try {
      val statement: Statement = DorisTestBase.connection.createStatement
      try {
        statement.execute(String.format("CREATE DATABASE IF NOT EXISTS %s", DATABASE))
        statement.execute(String.format("DROP TABLE IF EXISTS %s.%s", DATABASE, table))
        statement.execute(String.format(
          "CREATE TABLE %s.%s ( \n" + "`name` varchar(256),\n" + "`age` int\n" + ") " +
          "DISTRIBUTED BY HASH(`name`) BUCKETS 1\n" +
          "PROPERTIES (\n" +
          "\"replication_num\" = \"1\"\n" + ")\n", DATABASE, table))
      } finally if (statement != null) statement.close()
    }
  }

}
