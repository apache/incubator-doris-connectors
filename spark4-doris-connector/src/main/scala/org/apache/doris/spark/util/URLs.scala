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

object URLs {

  private val HTTP_SCHEMA = "http"

  private val HTTPS_SCHEMA = "https"

  private def schema(enableHttps: Boolean): String = if (enableHttps) HTTPS_SCHEMA else HTTP_SCHEMA

  def aliveBackend(feNode: String, enableHttps: Boolean = false) = s"${schema(enableHttps)}://$feNode/api/backends?is_alive=true"

  def tableSchema(feNode: String, database: String, table: String, enableHttps: Boolean = false) =
    s"${schema(enableHttps)}://$feNode/api/$database/$table/_schema"

  def queryPlan(feNode: String, database: String, table: String, enableHttps: Boolean = false) =
    s"${schema(enableHttps)}://$feNode/api/$database/$table/_query_plan"

  def streamLoad(feNode: String, database: String, table: String, enableHttps: Boolean = false) =
    s"${schema(enableHttps)}://$feNode/api/$database/$table/_stream_load"

  def streamLoad2PC(feNode: String, database: String, enableHttps: Boolean = false) =
    s"${schema(enableHttps)}://$feNode/api/$database/_stream_load_2pc"

}
