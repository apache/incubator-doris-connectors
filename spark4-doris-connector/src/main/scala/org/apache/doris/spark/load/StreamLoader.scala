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

import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.json.JsonMapper
import org.apache.commons.io.IOUtils
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.doris.spark.cfg.{ConfigurationOptions, SparkSettings}
import org.apache.doris.spark.exception.{IllegalArgumentException, StreamLoadException}
import org.apache.doris.spark.rest.RestService
import org.apache.doris.spark.rest.models.BackendV2.BackendRowV2
import org.apache.doris.spark.rest.models.RespContent
import org.apache.doris.spark.sql.Utils
import org.apache.doris.spark.util.{HttpUtil, ResponseUtil, URLs}
import org.apache.http.client.methods.{CloseableHttpResponse, HttpPut, HttpRequestBase, HttpUriRequest}
import org.apache.http.entity.{BufferedHttpEntity, ByteArrayEntity, InputStreamEntity}
import org.apache.http.impl.client.{CloseableHttpClient, DefaultRedirectStrategy, HttpClients}
import org.apache.http.util.EntityUtils
import org.apache.http.{HttpEntity, HttpHeaders, HttpStatus}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import org.slf4j.{Logger, LoggerFactory}

import java.io.{ByteArrayOutputStream, IOException, InputStream}
import java.net.{HttpURLConnection, URL}
import java.nio.charset.StandardCharsets
import java.util
import java.util.concurrent.ExecutionException
import java.util.zip.GZIPOutputStream
import java.util.{Base64, Calendar, Collections, UUID}
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

case class StreamLoadResponse(code: Int, msg: String, content: String)

class StreamLoader(settings: SparkSettings, isStreaming: Boolean) extends Loader {

  private final val LOG: Logger = LoggerFactory.getLogger(classOf[StreamLoader])

  private final val MAPPER: ObjectMapper = JsonMapper.builder().build()

  private val database: String = settings.getProperty(ConfigurationOptions.DORIS_TABLE_IDENTIFIER).split("\\.")(0)

  private val table: String = settings.getProperty(ConfigurationOptions.DORIS_TABLE_IDENTIFIER).split("\\.")(1)

  private val authEncoded: String = getAuthEncoded

  private val enableTwoPhaseCommit = settings.getBooleanProperty(ConfigurationOptions.DORIS_SINK_ENABLE_2PC,
    ConfigurationOptions.DORIS_SINK_ENABLE_2PC_DEFAULT)

  private val streamLoadProps: Map[String, String] = getStreamLoadProps

  private val format: DataFormat = DataFormat.valueOf(streamLoadProps.getOrElse("format", "csv").toUpperCase)

  private var currentLoadUrl: String = _

  private val autoRedirect: Boolean = settings.getBooleanProperty(ConfigurationOptions.DORIS_SINK_AUTO_REDIRECT,
    ConfigurationOptions.DORIS_SINK_AUTO_REDIRECT_DEFAULT)

  require(if (settings.getBooleanProperty(ConfigurationOptions.DORIS_ENABLE_HTTPS,
    ConfigurationOptions.DORIS_ENABLE_HTTPS_DEFAULT)) autoRedirect else true, "https must open with auto redirect")

  private val enableHttps: Boolean = settings.getBooleanProperty(ConfigurationOptions.DORIS_ENABLE_HTTPS,
    ConfigurationOptions.DORIS_ENABLE_HTTPS_DEFAULT) && autoRedirect

  /**
   * execute stream load
   *
   * @param iterator row data iterator
   * @param schema   row schema
   * @throws stream load exception
   * @return transaction id
   */
  @throws[StreamLoadException]
  override def load(iterator: Iterator[InternalRow], schema: StructType): Option[CommitMessage] = {

    var msg: Option[CommitMessage] = None

    val client: CloseableHttpClient = HttpUtil.getHttpClient(settings)
    val label: String = generateLoadLabel()

    Try {
      val request = buildLoadRequest(iterator, schema, label)
      val response = client.execute(request)
      val txnId = handleStreamLoadResponse(response)
      msg = Some(CommitMessage(txnId))
    } match {
      case Success(_) => client.close()
      case Failure(e) =>
        LOG.error(s"stream load failed, err: ${ExceptionUtils.getStackTrace(e)}")
        if (enableTwoPhaseCommit) abortByLabel(label)
        if (e.isInstanceOf[StreamLoadException]) throw e
        throw new StreamLoadException(s"failed to load data on $currentLoadUrl", e)
    }

    msg

  }

  /**
   * commit transaction
   *
   * @param msg commit message with transaction id
   * @throws stream load exception
   */
  @throws[StreamLoadException]
  override def commit(msg: CommitMessage): Unit = {

    val client = getHttpClient

    Try {

      val node = getNode
      val abortUrl = URLs.streamLoad2PC(node, database, enableHttps)
      val httpPut = new HttpPut(abortUrl)
      addCommonHeader(httpPut)
      httpPut.setHeader("txn_operation", "commit")
      httpPut.setHeader("txn_id", String.valueOf(msg.value))

      val response = client.execute(httpPut)
      var statusCode = response.getStatusLine.getStatusCode
      if (statusCode != 200 || response.getEntity == null) {
        LOG.warn("commit transaction response: " + response.getStatusLine.toString)
        throw new StreamLoadException("Fail to commit transaction " + msg.value + " with url " + abortUrl)
      }

      statusCode = response.getStatusLine.getStatusCode
      val reasonPhrase = response.getStatusLine.getReasonPhrase
      if (statusCode != 200) {
        LOG.warn(s"commit failed with $node, reason $reasonPhrase")
        throw new StreamLoadException("stream load error: " + reasonPhrase)
      }

      if (response.getEntity != null) {
        val loadResult = EntityUtils.toString(response.getEntity)
        val res = MAPPER.readValue(loadResult, new TypeReference[util.HashMap[String, String]]() {})
        if (res.get("status") == "Success" || ResponseUtil.isCommitted(res.get("msg"))) LOG.info("commit transaction {} succeed, load result: {}.", msg.value, loadResult)
        else {
          LOG.error("commit transaction {} failed. load result: {}", msg.value, loadResult)
          throw new StreamLoadException("Commit failed " + loadResult)
        }
      }
    } match {
      case Success(_) => client.close()
      case Failure(e) =>
        client.close()
        LOG.error("commit transaction failed, {}", ExceptionUtils.getStackTrace(e))
        if (e.isInstanceOf[StreamLoadException]) throw e
        throw new StreamLoadException(e)
    }

  }

  /**
   * abort transaction
   *
   * @param msg commit message with transaction id
   * @throws stream load exception
   */
  override def abort(msg: CommitMessage): Unit = {
    doAbort(_.setHeader("txn_id", String.valueOf(msg.value)))
  }

  private def abortByLabel(label: String): Unit = {
    Try {
      doAbort(_.setHeader("label", label))
    } match {
      case Success(_) => // do nothing
      case Failure(e) =>
        LOG.warn(s"abort by label failed, label: $label, err: ${ExceptionUtils.getStackTrace(e)}")
    }
  }

  /**
   * get stream load properties from settings
   *
   * @return map data of stream load properties
   */
  private def getStreamLoadProps: Map[String, String] = {
    val props = settings.asProperties().asScala.filter(_._1.startsWith(ConfigurationOptions.STREAM_LOAD_PROP_PREFIX))
      .map { case (k, v) => (k.substring(ConfigurationOptions.STREAM_LOAD_PROP_PREFIX.length), v) }
    if (props.getOrElse("add_double_quotes", "false").toBoolean) {
      LOG.info("set add_double_quotes for csv mode, add trim_double_quotes to true for prop.")
      props.put("trim_double_quotes", "true")
    }
    if ("json".equalsIgnoreCase(props.getOrElse("format", "csv"))) {
      props += "read_json_by_line" -> "true"
      props.remove("strip_outer_array")
    }
    props.remove("columns")
    props.toMap
  }

  /**
   * get http client
   *
   * @return http client
   */
  private def getHttpClient: CloseableHttpClient = {
    HttpClients.custom().setRedirectStrategy(new DefaultRedirectStrategy() {
      override def isRedirectable(method: String): Boolean = true
    }).build()
  }

  /**
   * add some common header for doris http request
   *
   * @param req http request
   */
  private def addCommonHeader(req: HttpRequestBase): Unit = {
    req.setHeader(HttpHeaders.AUTHORIZATION, "Basic " + authEncoded)
    req.setHeader(HttpHeaders.EXPECT, "100-continue")
    req.setHeader(HttpHeaders.CONTENT_TYPE, "text/plain; charset=UTF-8")
  }

  /**
   * build load request, set params as request header
   *
   * @param iterator row data iterator
   * @param schema   row data schema
   * @param label    load label
   * @return http request
   */
  private def buildLoadRequest(iterator: Iterator[InternalRow], schema: StructType, label: String): HttpUriRequest = {

    currentLoadUrl = URLs.streamLoad(getNode, database, table, enableHttps)
    val put = new HttpPut(currentLoadUrl)
    addCommonHeader(put)

    put.setHeader("label", label)

    val columns = settings.getProperty(ConfigurationOptions.DORIS_WRITE_FIELDS)
    if (StringUtils.isNotBlank(columns)) {
      put.setHeader("columns", columns)
    } else if (schema != null && schema.nonEmpty) {
      put.setHeader("columns", schema.fieldNames.map(Utils.quote).mkString(","))
    }

    val maxFilterRatio = settings.getProperty(ConfigurationOptions.DORIS_MAX_FILTER_RATIO)
    if (StringUtils.isNotBlank(maxFilterRatio)) put.setHeader("max_filter_ratio", maxFilterRatio)

    if (enableTwoPhaseCommit) put.setHeader("two_phase_commit", "true")

    if (streamLoadProps != null && streamLoadProps.nonEmpty) {
      streamLoadProps.foreach(prop => put.setHeader(prop._1, prop._2))
    }

    put.setEntity(generateHttpEntity(iterator, schema))

    put

  }

  /**
   * get load address
   * if enable auto redirect, return fe address,
   * or be node is configured, return one be address randomly.
   * otherwise, request fe to get alive be node, and return address.
   *
   * if load data to be directly, check node available will be done before return.
   *
   * @throws [ [ org.apache.doris.spark.exception.StreamLoadException]]
   * @return address
   */
  @throws[StreamLoadException]
  private def getNode: String = {

    var address: Option[String] = None

    Try {

      if (autoRedirect) {
        val feNodes = settings.getProperty(ConfigurationOptions.DORIS_FENODES)
        Some(RestService.randomEndpoint(feNodes, LOG))
      } else {
        val backends = RestService.getBackendRows(settings, LOG)
        val iter = backends.iterator()
        while (iter.hasNext) {
          if (!checkAvailable(iter.next())) {
            iter.remove()
          }
        }
        if (backends.isEmpty) throw new StreamLoadException("no backend alive")
        Collections.shuffle(backends)
        val backend = backends.get(0)
        Some(backend.getIp + ":" + backend.getHttpPort)
      }

    } match {
      case Success(node) => address = node
      case Failure(e: ExecutionException) => throw new StreamLoadException("get backends info fail", e)
      case Failure(e: IllegalArgumentException) => throw new StreamLoadException("get frontend info fail", e)
    }

    address.get

  }

  /**
   * check be is alive or not
   *
   * @param backend backend
   * @return is alive or not
   */
  private def checkAvailable(backend: BackendRowV2): Boolean = {
    Try {
      val url = new URL(s"http://${backend.getIp}:${backend.getHttpPort}")
      val connection = url.openConnection.asInstanceOf[HttpURLConnection]
      connection.setConnectTimeout(60 * 1000)
      connection.connect()
      connection.disconnect()
    } match {
      case Success(_) => true
      case Failure(e) =>
        LOG.warn(s"Failed to connect to backend: ${backend.getIp}:${backend.getHttpPort}", e)
        false
    }

  }

  /**
   * authorization info after base 64 encoded
   *
   * @return auth info
   */
  private def getAuthEncoded: String = {
    val user = settings.getProperty(ConfigurationOptions.DORIS_REQUEST_AUTH_USER)
    val passwd = settings.getProperty(ConfigurationOptions.DORIS_REQUEST_AUTH_PASSWORD)
    Base64.getEncoder.encodeToString(s"$user:$passwd".getBytes(StandardCharsets.UTF_8))
  }

  /**
   * generate load label
   *
   * spark_streamload_YYYYMMDD_HHMMSS_{UUID}
   *
   * @return load label
   */
  private def generateLoadLabel(): String = {
    val calendar = Calendar.getInstance
    "spark_streamload_" +
      f"${calendar.get(Calendar.YEAR)}${calendar.get(Calendar.MONTH) + 1}%02d${calendar.get(Calendar.DAY_OF_MONTH)}%02d" +
      f"_${calendar.get(Calendar.HOUR_OF_DAY)}%02d${calendar.get(Calendar.MINUTE)}%02d${calendar.get(Calendar.SECOND)}%02d" +
      f"_${UUID.randomUUID.toString.replaceAll("-", "")}"
  }

  private def generateHttpEntity(iterator: Iterator[InternalRow], schema: StructType): HttpEntity = {

    var entity: Option[HttpEntity] = None

    val compressType = streamLoadProps.get("compress_type")
    val columnSeparator = escapeString(streamLoadProps.getOrElse("column_separator", "\t"))
    val lineDelimiter = escapeString(streamLoadProps.getOrElse("line_delimiter", "\n"))
    val addDoubleQuotes = streamLoadProps.getOrElse("add_double_quotes", "false").toBoolean
    val streamingPassthrough: Boolean = isStreaming && settings.getBooleanProperty(
      ConfigurationOptions.DORIS_SINK_STREAMING_PASSTHROUGH,
      ConfigurationOptions.DORIS_SINK_STREAMING_PASSTHROUGH_DEFAULT)

    if (compressType.nonEmpty) {
      if ("gz".equalsIgnoreCase(compressType.get) && format == DataFormat.CSV) {
        val recodeBatchInputStream = new RecordBatchInputStream(RecordBatch.newBuilder(iterator.asJava)
          .format(format)
          .sep(columnSeparator)
          .delim(lineDelimiter)
          .schema(schema)
          .addDoubleQuotes(addDoubleQuotes).build, streamingPassthrough)
        val compressedData = compressByGZ(recodeBatchInputStream)
        entity = Some(new ByteArrayEntity(compressedData))
      }
      else {
        val msg = s"Not support the compress type [$compressType] for the format [$format]"
        throw new StreamLoadException(msg)
      }
    }
    else {
      val recodeBatchInputStream = new RecordBatchInputStream(RecordBatch.newBuilder(iterator.asJava)
        .format(format)
        .sep(columnSeparator)
        .delim(lineDelimiter)
        .schema(schema)
        .addDoubleQuotes(addDoubleQuotes).build, streamingPassthrough)
      entity = Some(new InputStreamEntity(recodeBatchInputStream))
    }

    entity.get

  }

  /**
   * Escape special characters
   *
   * @param hexData origin string
   * @return escaped string
   */
  private def escapeString(hexData: String): String = {
    if (hexData.startsWith("\\x") || hexData.startsWith("\\X")) {
      try {
        val tmp = hexData.substring(2)
        val stringBuilder = new StringBuilder
        var i = 0
        while (i < tmp.length) {
          val hexByte = tmp.substring(i, i + 2)
          val decimal = Integer.parseInt(hexByte, 16)
          val character = decimal.toChar
          stringBuilder.append(character)
          i += 2
        }
        return stringBuilder.toString
      } catch {
        case e: Exception =>
          throw new RuntimeException("escape column_separator or line_delimiter error.{}", e)
      }
    }
    hexData
  }

  /**
   * compress data by gzip
   *
   * @param content data content
   * @throws
   * @return compressed byte array data
   */
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

  /**
   * compress data by gzip
   *
   * @param contentInputStream data content
   * @throws
   * @return compressed byte array data
   */
  @throws[IOException]
  def compressByGZ(contentInputStream: InputStream): Array[Byte] = {
    var compressedData: Array[Byte] = null
    try {
      val baos = new ByteArrayOutputStream
      val gzipOutputStream = new GZIPOutputStream(baos)
      try {
        IOUtils.copy(contentInputStream, gzipOutputStream)
        gzipOutputStream.finish()
        compressedData = baos.toByteArray
      } finally {
        if (baos != null) baos.close()
        if (gzipOutputStream != null) gzipOutputStream.close()
      }
    }
    compressedData
  }

  /**
   * handle stream load response
   *
   * @param response http response
   * @throws
   * @return transaction id
   */
  @throws[StreamLoadException]
  private def handleStreamLoadResponse(response: CloseableHttpResponse): Long = {

    val code = response.getStatusLine.getStatusCode
    val msg = response.getStatusLine.getReasonPhrase
    val content = EntityUtils.toString(new BufferedHttpEntity(response.getEntity), StandardCharsets.UTF_8)
    val loadResponse: StreamLoadResponse = StreamLoadResponse(code, msg, content)

    if (loadResponse.code != HttpStatus.SC_OK) {
      LOG.error(s"Stream load http status is not OK, status: ${loadResponse.code}, response: $loadResponse")
      throw new StreamLoadException(String.format("stream load error, http status:%d, response:%s",
        new Integer(loadResponse.code), loadResponse))
    } else {
      try {
        val respContent = MAPPER.readValue(loadResponse.content, classOf[RespContent])
        if (!respContent.isSuccess) {
          LOG.error(s"Stream load status is not success, status:${respContent.getStatus}, response:$loadResponse")
          throw new StreamLoadException(String.format("stream load error, load status:%s, response:%s", respContent.getStatus, loadResponse))
        }
        LOG.info("Stream load Response:{}", loadResponse)
        respContent.getTxnId
      } catch {
        case e: IOException =>
          throw new StreamLoadException(e)
      }
    }

  }

  /**
   * execute abort
   *
   * @param f function to set header
   * @throws
   */
  @throws[StreamLoadException]
  private def doAbort(f: HttpRequestBase => Unit): Unit = {

    val client = getHttpClient

    Try {

      val abortUrl = URLs.streamLoad2PC(getNode, database, enableHttps)
      val httpPut = new HttpPut(abortUrl)
      addCommonHeader(httpPut)
      httpPut.setHeader("txn_operation", "abort")
      f(httpPut)

      val response = client.execute(httpPut)
      val statusCode = response.getStatusLine.getStatusCode
      if (statusCode != HttpStatus.SC_OK || response.getEntity == null) {
        LOG.error("abort transaction response: " + response.getStatusLine.toString)
        throw new StreamLoadException("Fail to abort transaction with url " + abortUrl)
      }

      val loadResult = EntityUtils.toString(response.getEntity)
      val res = MAPPER.readValue(loadResult, new TypeReference[util.HashMap[String, String]]() {})
      if (!"Success".equalsIgnoreCase(res.get("status"))) {
        if (ResponseUtil.isCommitted(res.get("msg"))) throw new IOException("try abort committed transaction")
        LOG.error(s"Fail to abort transaction. error: ${res.get("msg")}")
        throw new StreamLoadException(String.format("Fail to abort transaction. error: %s", res.get("msg")))
      }

    } match {
      case Success(_) => client.close()
      case Failure(e) =>
        client.close()
        LOG.error(s"abort transaction failed, ${ExceptionUtils.getStackTrace(e)}")
        if (e.isInstanceOf[StreamLoadException]) throw e
        throw new StreamLoadException(e)
    }

  }

}
