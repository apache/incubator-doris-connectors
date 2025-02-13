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

import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, DateTimeUtils}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import java.sql.{Date, Timestamp}
import java.time.LocalDate
import scala.collection.mutable

object RowConvertors {

  private val MAPPER = JsonMapper.builder().addModule(DefaultScalaModule)
    .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true).build()

  private val NULL_VALUE = "\\N"

  def convertToCsv(row: InternalRow, schema: StructType, sep: String): String = {
    (0 until schema.length).map(i => {
      asScalaValue(row, schema.fields(i).dataType, i)
    }).mkString(sep)
  }

  def convertToJson(row: InternalRow, schema: StructType): String = {
    MAPPER.writeValueAsString(
      (0 until schema.length).map(i => {
        schema.fields(i).name -> asScalaValue(row, schema.fields(i).dataType, i)
      }).toMap
    )
  }

  private def asScalaValue(row: SpecializedGetters, dataType: DataType, ordinal: Int): Any = {
    if (row.isNullAt(ordinal)) null
    else {
      dataType match {
        case NullType => NULL_VALUE
        case BooleanType => row.getBoolean(ordinal)
        case ByteType => row.getByte(ordinal)
        case ShortType => row.getShort(ordinal)
        case IntegerType => row.getInt(ordinal)
        case LongType => row.getLong(ordinal)
        case FloatType => row.getFloat(ordinal)
        case DoubleType => row.getDouble(ordinal)
        case StringType => Option(row.getUTF8String(ordinal)).map(_.toString).getOrElse(NULL_VALUE)
        case TimestampType =>
          DateTimeUtils.toJavaTimestamp(row.getLong(ordinal)).toString
        case DateType => DateTimeUtils.toJavaDate(row.getInt(ordinal)).toString
        case BinaryType => row.getBinary(ordinal)
        case dt: DecimalType => row.getDecimal(ordinal, dt.precision, dt.scale).toJavaBigDecimal
        case at: ArrayType =>
          val arrayData = row.getArray(ordinal)
          if (arrayData == null) NULL_VALUE
          else {
            (0 until arrayData.numElements()).map(i => {
              if (arrayData.isNullAt(i)) null else asScalaValue(arrayData, at.elementType, i)
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
              map += asScalaValue(keys, mt.keyType, i) -> asScalaValue(values, mt.valueType, i)
              i += 1
            }
            MAPPER.writeValueAsString(map)
          }
        case st: StructType =>
          val structData = row.getStruct(ordinal, st.length)
          val map = new java.util.TreeMap[String, Any]()
          var i = 0
          while (i < structData.numFields) {
            val field = st.fields(i)
            map.put(field.name, asScalaValue(structData, field.dataType, i))
            i += 1
          }
          MAPPER.writeValueAsString(map)
        case _ => throw new Exception(s"Unsupported spark type: ${dataType.typeName}")
      }
    }
  }

  def convertValue(v: Any, dataType: DataType, datetimeJava8ApiEnabled: Boolean): Any = {
    dataType match {
      case StringType => UTF8String.fromString(v.asInstanceOf[String])
      case TimestampType => DateTimeUtils.fromJavaTimestamp(Timestamp.valueOf(v.asInstanceOf[String]))
      case DateType if datetimeJava8ApiEnabled => v.asInstanceOf[LocalDate].toEpochDay.toInt
      case DateType => DateTimeUtils.fromJavaDate(v.asInstanceOf[Date])
      case _: MapType =>
        val map = v.asInstanceOf[Map[String, String]]
        val keys = map.keys.toArray
        val values = map.values.toArray
        ArrayBasedMapData(keys, values)
      case NullType | BooleanType | ByteType | ShortType | IntegerType | LongType | FloatType | DoubleType | BinaryType | _:DecimalType => v
      case _ => throw new Exception(s"Unsupported spark type: ${dataType.typeName}")
    }
  }

}
