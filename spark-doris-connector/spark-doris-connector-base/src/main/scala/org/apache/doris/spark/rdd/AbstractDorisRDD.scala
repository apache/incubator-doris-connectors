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

package org.apache.doris.spark.rdd

import org.apache.doris.spark.client.entity.DorisReaderPartition
import org.apache.doris.spark.client.read.ReaderPartitionGenerator
import org.apache.doris.spark.config.DorisConfig
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext}

import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.reflect.ClassTag

protected[spark] abstract class AbstractDorisRDD[T: ClassTag](
    @transient private var sc: SparkContext,
    val params: Map[String, String] = Map.empty)
    extends RDD[T](sc, Nil) {

  override def getPartitions: Array[Partition] = {
    dorisPartitions.zipWithIndex.map { case (dorisPartition, idx) =>
      new DorisPartition(id, idx, dorisPartition)
    }.toArray
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    val dorisSplit = split.asInstanceOf[DorisPartition]
    Seq(dorisSplit.dorisPartition.getBackend.hostRpcPortString())
  }

  override def checkpoint(): Unit = {
    // Do nothing. Doris RDD should not be checkpointed.
  }

  /**
   * doris configuration get from rdd parameters and spark conf.
   */
  @transient private[spark] lazy val dorisCfg = DorisConfig.fromMap(sc.getConf.getAll.toMap.asJava, params.asJava, false)

  @transient private[spark] lazy val dateTimeJava8ApiEnabled = sc.getConf.get("spark.sql.datetime.java8API.enabled", "false").toBoolean

  @transient private[spark] lazy val dorisPartitions = ReaderPartitionGenerator.generatePartitions(dorisCfg, dateTimeJava8ApiEnabled)
}

private[spark] class DorisPartition(rddId: Int, idx: Int, val dorisPartition: DorisReaderPartition)
    extends Partition {

  override def hashCode(): Int = 31 * (31 * (31 + rddId) + idx) + dorisPartition.hashCode()

  override val index: Int = idx
}
