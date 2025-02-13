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

package org.apache.spark.sql.doris.spark

import org.apache.arrow.vector.types.pojo.Schema
import org.apache.spark.sql.types.StructType

@deprecated(since = "1.4.0")
object ArrowSchemaUtils {
  var classArrowUtils: Option[Class[_]] = None: Option[Class[_]]

  def tryLoadArrowUtilsClass(): Unit = {
    if (classArrowUtils.isEmpty) {
      // for spark3.x
      classArrowUtils = classArrowUtils.orElse(tryLoadClass("org.apache.spark.sql.util.ArrowUtils"))
      // for spark2.x
      classArrowUtils = classArrowUtils.orElse(tryLoadClass("org.apache.spark.sql.execution.arrow.ArrowUtils"))
      if (classArrowUtils.isEmpty) {
        throw new ClassNotFoundException("can't load class for ArrowUtils")
      }
    }
  }

  def tryLoadClass(className: String): Option[Class[_]] = {
    try {
      Some(Class.forName(className))
    } catch {
      case e: ClassNotFoundException =>
        None
    }
  }

  def toArrowSchema(schema: StructType, timeZoneId: String): Schema = {
    tryLoadArrowUtilsClass()
    val toArrowSchema = classArrowUtils.get.getMethod("toArrowSchema", classOf[StructType], classOf[String])
    toArrowSchema.invoke(null, schema, timeZoneId).asInstanceOf[Schema]
  }
}
