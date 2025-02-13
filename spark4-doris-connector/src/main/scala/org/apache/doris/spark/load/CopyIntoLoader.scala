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

package org.apache.doris.spark.load

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.doris.spark.cfg.{ConfigurationOptions, SparkSettings}
import org.apache.doris.spark.exception.{CopyIntoException, StreamLoadException}
import org.apache.doris.spark.rest.models.RespContent
import org.apache.doris.spark.util.{CopySQLBuilder, HttpPostBuilder, HttpPutBuilder}
import org.apache.hadoop.util.StringUtils.escapeString
import org.apache.http.{HttpEntity, HttpStatus}
import org.apache.http.client.methods.CloseableHttpResponse
import org.apache.http.entity.{BufferedHttpEntity, ByteArrayEntity, InputStreamEntity, StringEntity}
import org.apache.http.impl.client.{CloseableHttpClient, HttpClients}
import org.apache.http.util.EntityUtils

import scala.collection.JavaConverters._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import org.slf4j.{Logger, LoggerFactory}

import java.io.{ByteArrayOutputStream, IOException}
import java.nio.charset.StandardCharsets
import java.util.zip.GZIPOutputStream
import java.util.{Base64, Properties, UUID}
import scala.util.{Failure, Success, Try}

case class CopyIntoResponse(code: Int, msg: String, content: String)

class CopyIntoLoader(settings: SparkSettings, isStreaming: Boolean) extends Loader {

  private final val LOG: Logger = LoggerFactory.getLogger(classOf[CopyIntoLoader])

  private val hostPort:String = settings.getProperty(ConfigurationOptions.DORIS_FENODES)

  private val tableIdentifier: String = settings.getProperty(ConfigurationOptions.DORIS_TABLE_IDENTIFIER)

  private val OBJECT_MAPPER = new ObjectMapper

  private val copyIntoProps: Properties = getCopyIntoProps

  private val format: DataFormat = DataFormat.valueOf(copyIntoProps.getOrDefault("format", "csv").toString.toUpperCase)

  private val LOAD_URL_PATTERN = "http://%s/copy/upload"

  private val COMMIT_PATTERN = "http://%s/copy/query"

  private val authEncoded: String = getAuthEncoded


  /**
   * execute load
   *
   * @param iterator row data iterator
   * @param schema   row data schema
   * @return commit message
   */
  override def load(iterator: Iterator[InternalRow], schema: StructType): Option[CommitMessage] = {

    var msg: Option[CommitMessage] = None

    val fileName: String = UUID.randomUUID().toString
    val client: CloseableHttpClient = getHttpClient
    val currentLoadUrl: String = String.format(LOAD_URL_PATTERN, hostPort)

    Try {
      val uploadAddressRequest = buildUploadAddressRequest(currentLoadUrl,fileName)
      val uploadAddressReponse = client.execute(uploadAddressRequest.build())
      val uploadAddress = handleGetUploadAddressResponse(uploadAddressReponse)
      val uploadFileRequest = buildUpLoadFileRequest(uploadAddress, iterator, schema)
      val uploadFileReponse = client.execute(uploadFileRequest.build())
      handleUploadFileResponse(uploadFileReponse)
      val copyIntoRequest = executeCopyInto(s"${fileName}%s")
      val copyIntoReponse = client.execute(copyIntoRequest.build())
      handleExecuteCopyintoResponse(copyIntoReponse)
      msg = Some(CommitMessage(fileName))
    } match {
      case Success(_) => client.close()
      case Failure(e) =>
        LOG.error(s"Copy into failed, err: ${ExceptionUtils.getStackTrace(e)}")
        if (e.isInstanceOf[CopyIntoException]) throw e
        throw new CopyIntoException(s"failed to load data on $currentLoadUrl", e)
    }
    msg
  }

  /**
   * handle execute copy into response
   *
   * @param copyIntoReponse row data iterator
   */
  private def handleExecuteCopyintoResponse(copyIntoReponse: CloseableHttpResponse) = {
    val code = copyIntoReponse.getStatusLine.getStatusCode
    val msg = copyIntoReponse.getStatusLine.getReasonPhrase
    val content = EntityUtils.toString(new BufferedHttpEntity(copyIntoReponse.getEntity), StandardCharsets.UTF_8)
    val loadResponse: CopyIntoResponse = CopyIntoResponse(code, msg, content)
    if (loadResponse.code != HttpStatus.SC_OK) {
      LOG.error(s"Execute copy sql status is not OK, status: ${loadResponse.code}, response: $loadResponse")
      throw new StreamLoadException(String.format("Execute copy sql, http status:%d, response:%s",
        new Integer(loadResponse.code), loadResponse))
    } else {
      try {
        val respContent = OBJECT_MAPPER.readValue(loadResponse.content, classOf[RespContent])
        if (!respContent.isCopyIntoSuccess) {
          LOG.error(s"Execute copy sql status is not success, status:${respContent.getStatus}, response:$loadResponse")
          throw new StreamLoadException(String.format("Execute copy sql error, load status:%s, response:%s", respContent.getStatus, loadResponse))
        }
        LOG.info("Execute copy sql Response:{}", loadResponse)
      } catch {
        case e: IOException =>
          throw new StreamLoadException(e)
      }
    }
  }

  private def handleUploadFileResponse(uploadFileReponse: CloseableHttpResponse) = {
    val code = uploadFileReponse.getStatusLine.getStatusCode
    val msg = uploadFileReponse.getStatusLine.getReasonPhrase
    val content = EntityUtils.toString(new BufferedHttpEntity(uploadFileReponse.getEntity), StandardCharsets.UTF_8)
    val loadResponse: CopyIntoResponse = CopyIntoResponse(code, msg, content)
    if (loadResponse.code != HttpStatus.SC_OK) {
      LOG.error(s"Upload file status is not OK, status: ${loadResponse.code}, response: $loadResponse")
      throw new CopyIntoException(s"Upload file error, http status:${loadResponse.code}, response:$loadResponse")
    } else {
      LOG.info(s"Upload file success,status: ${loadResponse.code}, response: $loadResponse")
    }
  }

  private def buildUpLoadFileRequest(uploadAddress: String, iterator: Iterator[InternalRow], schema: StructType): HttpPutBuilder = {
    val builder = new HttpPutBuilder().setUrl(uploadAddress).addCommonHeader().setEntity(generateHttpEntity(iterator, schema))
    builder
  }

  /**
   * commit transaction
   *
   * @param msg commit message
   */
  override def commit(msg: CommitMessage): Unit = ???


  /**
   * abort transaction
   *
   * @param msg commit message
   */
  override def abort(msg: CommitMessage): Unit = ???

  private def executeCopyInto(fileName: String): HttpPostBuilder = {

    val copySQLBuilder: CopySQLBuilder = new CopySQLBuilder(format.toString, copyIntoProps, tableIdentifier, fileName)
    val copySql: String = copySQLBuilder.buildCopySQL()
    LOG.info(s"build copy sql is $copySql")
    val objectNode: ObjectNode = OBJECT_MAPPER.createObjectNode()
    objectNode.put("sql", copySql)

    val postBuilder: HttpPostBuilder = new HttpPostBuilder()
    postBuilder.setUrl(String.format(COMMIT_PATTERN, hostPort)).baseAuth(authEncoded)
      .setEntity(new StringEntity(OBJECT_MAPPER.writeValueAsString(objectNode)))
  }

  private def buildUploadAddressRequest(url: String, fileName: String): HttpPutBuilder = {
    val builder: HttpPutBuilder = new HttpPutBuilder().setUrl(url).addCommonHeader().addFileName(fileName).setEntity(new StringEntity("")).baseAuth(authEncoded)
    builder
  }

  private def getAuthEncoded: String = {
    val user = settings.getProperty(ConfigurationOptions.DORIS_REQUEST_AUTH_USER)
    val passwd = settings.getProperty(ConfigurationOptions.DORIS_REQUEST_AUTH_PASSWORD)
    Base64.getEncoder.encodeToString(s"$user:$passwd".getBytes(StandardCharsets.UTF_8))
  }

  private def generateHttpEntity(iterator: Iterator[InternalRow], schema: StructType): HttpEntity = {

    var entity: Option[HttpEntity] = None

    val compressType = copyIntoProps.getProperty("compression")
    val columnSeparator = escapeString(copyIntoProps.getOrDefault("file.column_separator", "\t").toString)
    val lineDelimiter = escapeString(copyIntoProps.getOrDefault("file.line_delimiter", "\n").toString)
    val addDoubleQuotes = copyIntoProps.getOrDefault("add_double_quotes", "false").toString.toBoolean
    val streamingPassthrough: Boolean = isStreaming && settings.getBooleanProperty(
      ConfigurationOptions.DORIS_SINK_STREAMING_PASSTHROUGH,
      ConfigurationOptions.DORIS_SINK_STREAMING_PASSTHROUGH_DEFAULT)

    val recordBatchString = new RecordBatchString(RecordBatch.newBuilder(iterator.asJava)
      .format(format)
      .sep(columnSeparator)
      .delim(lineDelimiter)
      .schema(schema)
      .addDoubleQuotes(addDoubleQuotes).build, streamingPassthrough)
    val content = recordBatchString.getContent

    if (compressType !=null && !compressType.isEmpty) {
      if ("gz".equalsIgnoreCase(compressType) && format == DataFormat.CSV) {
        val compressedData = compressByGZ(content)
        entity = Some(new ByteArrayEntity(compressedData))
      }
      else {
        val msg = s"Not support the compress type [$compressType] for the format [$format]"
        throw new CopyIntoException(msg)
      }
    }
    else {
      entity = Some(new ByteArrayEntity(content.getBytes(StandardCharsets.UTF_8)))
    }

    entity.get

  }

  private def getCopyIntoProps: Properties = {
    val props = settings.asProperties().asScala.filter(_._1.startsWith(ConfigurationOptions.STREAM_LOAD_PROP_PREFIX))
      .map { case (k,v) => (k.substring(ConfigurationOptions.STREAM_LOAD_PROP_PREFIX.length), v)}
    if (props.getOrElse("add_double_quotes", "false").toBoolean) {
      LOG.info("set add_double_quotes for csv mode, add trim_double_quotes to true for prop.")
      props.put("trim_double_quotes", "true")
    }
    props.remove("columns")
    val properties = new Properties()
    properties.putAll(props.mapValues(_.toString).toMap.asJava)
    properties
  }


  @throws[IOException]
  def compressByGZ(content: String): Array[Byte] = {
    var compressedData: Array[Byte] = null
    try {
      val baos = new ByteArrayOutputStream
      val gzipOutputStream = new GZIPOutputStream(baos)
      try {
        gzipOutputStream.write(content.getBytes("UTF-8"))
        gzipOutputStream.finish()
        compressedData = baos.toByteArray
      } finally {
        if (baos != null) baos.close()
        if (gzipOutputStream != null) gzipOutputStream.close()
      }
    }
    compressedData
  }

  private def getHttpClient: CloseableHttpClient = {
    HttpClients.custom().disableRedirectHandling().build()
  }

  @throws[CopyIntoException]
  private def handleGetUploadAddressResponse(response: CloseableHttpResponse): String = {
    val code = response.getStatusLine.getStatusCode
    val msg = response.getStatusLine.getReasonPhrase
    val content = EntityUtils.toString(new BufferedHttpEntity(response.getEntity), StandardCharsets.UTF_8)
    val loadResponse: CopyIntoResponse = CopyIntoResponse(code, msg, content)
    if (loadResponse.code == 307) {
      val uploadAddress:String = response.getFirstHeader("location").getValue
      LOG.info(s"Get upload address Response:$loadResponse")
      LOG.info(s"Redirect to s3: $uploadAddress")
      uploadAddress
    } else {
      LOG.error(s"Failed get the redirected address, status ${loadResponse.code}, reason ${loadResponse.msg}, response ${loadResponse.content}")
      throw new RuntimeException("Could not get the redirected address.")
    }
  }
}
