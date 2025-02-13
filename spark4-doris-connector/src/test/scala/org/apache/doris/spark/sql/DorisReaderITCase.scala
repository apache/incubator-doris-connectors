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
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

import java.sql.Statement

class DorisReaderITCase extends DorisTestBase {

  val DATABASE: String = "test"
  val TABLE_READ: String = "tbl_read"
  val TABLE_READ_TBL: String = "tbl_read_tbl"

  @Test
  @throws[Exception]
  def testRddSource(): Unit = {
    initializeTable(TABLE_READ)

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("rddSource")
    val sc = new SparkContext(sparkConf)
    import org.apache.doris.spark._
    val dorisSparkRDD = sc.dorisRDD(
      tableIdentifier = Some(DATABASE + "." + TABLE_READ),
      cfg = Some(Map(
        "doris.fenodes" -> DorisTestBase.getFenodes,
        "doris.request.auth.user" -> DorisTestBase.USERNAME,
        "doris.request.auth.password" -> DorisTestBase.PASSWORD
      ))
    )
    import scala.collection.JavaConverters._
    val result = dorisSparkRDD.collect().toList.asJava
    sc.stop()

    assert(List(List("doris", 18).asJava, List("spark", 10).asJava).asJava.equals(result))
  }

  @Test
  @throws[Exception]
  def testDataFrameSource(): Unit = {
    initializeTable(TABLE_READ_TBL)

    val session = SparkSession.builder().master("local[*]").getOrCreate()
    val dorisSparkDF = session.read
      .format("doris")
      .option("doris.fenodes", DorisTestBase.getFenodes)
      .option("doris.table.identifier", DATABASE + "." + TABLE_READ_TBL)
      .option("user", DorisTestBase.USERNAME)
      .option("password", DorisTestBase.PASSWORD)
      .load()

    val result = dorisSparkDF.collect().toList.toString()
    session.stop()
    assert("List([doris,18], [spark,10])".equals(result))
  }

  @Test
  @throws[Exception]
  def testSQLSource(): Unit = {
    initializeTable(TABLE_READ_TBL)
    val session = SparkSession.builder().master("local[*]").getOrCreate()
    session.sql(
      s"""
         |CREATE TEMPORARY VIEW test_source
         |USING doris
         |OPTIONS(
         | "table.identifier"="${DATABASE + "." + TABLE_READ_TBL}",
         | "fenodes"="${DorisTestBase.getFenodes}",
         | "user"="${DorisTestBase.USERNAME}",
         | "password"="${DorisTestBase.PASSWORD}"
         |);
         |""".stripMargin)

    val result = session.sql(
      """
        |select  name,age from test_source;
        |""".stripMargin).collect().toList.toString()
    session.stop()

    assert("List([doris,18], [spark,10])".equals(result))
  }

  @throws[Exception]
  private def initializeTable(table: String): Unit = {
    try {
      val statement: Statement = DorisTestBase.connection.createStatement
      try {
        statement.execute(String.format("CREATE DATABASE IF NOT EXISTS %s", DATABASE))
        statement.execute(String.format("DROP TABLE IF EXISTS %s.%s", DATABASE, table))
        statement.execute(String.format("CREATE TABLE %s.%s ( \n" +
          "`name` varchar(256),\n" +
          "`age` int\n" +
          ") DISTRIBUTED BY HASH(`name`) BUCKETS 1\n" +
          "PROPERTIES (\n" +
          "\"replication_num\" = \"1\"\n" +
          ")\n", DATABASE, table))
        statement.execute(String.format("insert into %s.%s  values ('doris',18)", DATABASE, table))
        statement.execute(String.format("insert into %s.%s  values ('spark',10)", DATABASE, table))
      } finally {
        if (statement != null) statement.close()
      }
    }
  }


}
