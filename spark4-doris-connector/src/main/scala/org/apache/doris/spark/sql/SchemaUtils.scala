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

import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.doris.sdk.thrift.TScanColumnDesc
import org.apache.doris.spark.cfg.ConfigurationOptions.{DORIS_IGNORE_TYPE, DORIS_READ_FIELD}
import org.apache.doris.spark.cfg.Settings
import org.apache.doris.spark.exception.DorisException
import org.apache.doris.spark.rest.RestService
import org.apache.doris.spark.rest.models.{Field, Schema}
import org.apache.doris.spark.util.DataUtil
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

import scala.collection.convert.ImplicitConversions.{`collection AsScalaIterable`, `seq AsJavaList`}
import scala.jdk.CollectionConverters._
import scala.collection.mutable

private[spark] object SchemaUtils {
  private val logger = LoggerFactory.getLogger(SchemaUtils.getClass.getSimpleName.stripSuffix("$"))
  private val MAPPER = JsonMapper.builder().addModule(DefaultScalaModule).build()

  /**
   * discover Doris table schema from Doris FE.
   *
   * @param cfg configuration
   * @return Spark Catalyst StructType
   */
  def discoverSchema(cfg: Settings): StructType = {
    val schema = discoverSchemaFromFe(cfg)
    val dorisReadField = cfg.getProperty(DORIS_READ_FIELD)
    val ignoreColumnType = cfg.getProperty(DORIS_IGNORE_TYPE)
    convertToStruct(schema, dorisReadField, ignoreColumnType)
  }

  /**
   * discover Doris table schema from Doris FE.
   *
   * @param cfg configuration
   * @return inner schema struct
   */
  def discoverSchemaFromFe(cfg: Settings): Schema = {
    RestService.getSchema(cfg, logger)
  }

  /**
   * convert inner schema struct to Spark Catalyst StructType
   *
   * @param schema inner schema
   * @return Spark Catalyst StructType
   */
  def convertToStruct(schema: Schema, dorisReadFields: String, ignoredTypes: String): StructType = {
    val fieldList = if (dorisReadFields != null && dorisReadFields.nonEmpty) {
      dorisReadFields.split(",")
    } else {
      Array.empty[String]
    }
    val ignoredTypeList = if (ignoredTypes != null && ignoredTypes.nonEmpty) {
      ignoredTypes.split(",").map(t => t.trim.toUpperCase)
    } else {
      Array.empty[String]
    }
    val fields = schema.getProperties
      .filter(x => (fieldList.contains(x.getName) || fieldList.isEmpty)
        && !ignoredTypeList.contains(x.getType))
      .map(f =>
        DataTypes.createStructField(
          f.getName,
          getCatalystType(f.getType, f.getPrecision, f.getScale),
          true
        )
      )
    DataTypes.createStructType(fields.toList)
  }

  /**
   * translate Doris Type to Spark Catalyst type
   *
   * @param dorisType Doris type
   * @param precision decimal precision
   * @param scale     decimal scale
   * @return Spark Catalyst type
   */
  def getCatalystType(dorisType: String, precision: Int, scale: Int): DataType = {
    dorisType match {
      case "NULL_TYPE"       => DataTypes.NullType
      case "BOOLEAN"         => DataTypes.BooleanType
      case "TINYINT"         => DataTypes.ByteType
      case "SMALLINT"        => DataTypes.ShortType
      case "INT"             => DataTypes.IntegerType
      case "BIGINT"          => DataTypes.LongType
      case "FLOAT"           => DataTypes.FloatType
      case "DOUBLE"          => DataTypes.DoubleType
      case "DATE"            => DataTypes.DateType
      case "DATEV2"          => DataTypes.DateType
      case "DATETIME"        => DataTypes.StringType
      case "DATETIMEV2"      => DataTypes.StringType
      case "BINARY"          => DataTypes.BinaryType
      case "DECIMAL"         => DecimalType(precision, scale)
      case "CHAR"            => DataTypes.StringType
      case "LARGEINT"        => DecimalType(38,0)
      case "VARCHAR"         => DataTypes.StringType
      case "JSON"            => DataTypes.StringType
      case "JSONB"           => DataTypes.StringType
      case "DECIMALV2"       => DecimalType(precision, scale)
      case "DECIMAL32"       => DecimalType(precision, scale)
      case "DECIMAL64"       => DecimalType(precision, scale)
      case "DECIMAL128"      => DecimalType(precision, scale)
      case "TIME"            => DataTypes.DoubleType
      case "STRING"          => DataTypes.StringType
      case "ARRAY"           => DataTypes.StringType
      case "MAP"             => MapType(DataTypes.StringType, DataTypes.StringType)
      case "STRUCT"          => DataTypes.StringType
      case "VARIANT"         => DataTypes.StringType
      case "IPV4"            => DataTypes.StringType
      case "IPV6"            => DataTypes.StringType
      case "HLL"             =>
        throw new DorisException("Unsupported type " + dorisType)
      case _                             =>
        throw new DorisException("Unrecognized Doris type " + dorisType)
    }
  }

  /**
   * convert Doris return schema to inner schema struct.
   *
   * @param tscanColumnDescs Doris BE return schema
   * @return inner schema struct
   */
  def convertToSchema(tscanColumnDescs: Seq[TScanColumnDesc]): Schema = {
    val schema = new Schema(tscanColumnDescs.length)
    tscanColumnDescs.foreach(desc => schema.put(new Field(desc.getName, desc.getType.name, "", 0, 0, "")))
    schema
  }

  def rowColumnValue(row: SpecializedGetters, ordinal: Int, dataType: DataType): Any = {

    if (row.isNullAt(ordinal)) null
    else {
      dataType match {
        case NullType => DataUtil.NULL_VALUE
        case BooleanType => row.getBoolean(ordinal)
        case ByteType => row.getByte(ordinal)
        case ShortType => row.getShort(ordinal)
        case IntegerType => row.getInt(ordinal)
        case LongType => row.getLong(ordinal)
        case FloatType => row.getFloat(ordinal)
        case DoubleType => row.getDouble(ordinal)
        case StringType => Option(row.getUTF8String(ordinal)).map(_.toString).getOrElse(DataUtil.NULL_VALUE)
        case TimestampType =>
          DateTimeUtils.toJavaTimestamp(row.getLong(ordinal)).toString
        case DateType => DateTimeUtils.toJavaDate(row.getInt(ordinal)).toString
        case BinaryType => row.getBinary(ordinal)
        case dt: DecimalType => row.getDecimal(ordinal, dt.precision, dt.scale).toJavaBigDecimal
        case at: ArrayType =>
          val arrayData = row.getArray(ordinal)
          if (arrayData == null) DataUtil.NULL_VALUE
          else {
            (0 until arrayData.numElements()).map(i => {
              if (arrayData.isNullAt(i)) null else rowColumnValue(arrayData, i, at.elementType)
            }).mkString("[", ",", "]")
          }
        case mt: MapType =>
          val mapData = row.getMap(ordinal)
          if (mapData.numElements() == 0) "{}"
          else {
            val keys = mapData.keyArray()
            val values = mapData.valueArray()
            val map = mutable.HashMap[Any, Any]()
            var i = 0
            while (i < keys.numElements()) {
              map += rowColumnValue(keys, i, mt.keyType) -> rowColumnValue(values, i, mt.valueType)
              i += 1
            }
            MAPPER.writeValueAsString(map)
          }
        case st: StructType =>
          val structData = row.getStruct(ordinal, st.length)
          val map = new java.util.TreeMap[String, Any]()
          var i = 0
          while (i < structData.numFields) {
            val field = st.get(i)
            map.put(field.name, rowColumnValue(structData, i, field.dataType))
            i += 1
          }
          MAPPER.writeValueAsString(map)
        case _ => throw new DorisException(s"Unsupported spark type: ${dataType.typeName}")
      }
    }

  }

}
