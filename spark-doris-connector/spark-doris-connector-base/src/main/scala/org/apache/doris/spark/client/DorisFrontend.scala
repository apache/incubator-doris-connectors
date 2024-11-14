package org.apache.doris.spark.client

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.commons.lang3.StringUtils
import org.apache.doris.spark.config.{DorisConfig, DorisOptions}
import org.apache.doris.spark.util.{HttpUtils, JdbcUtils}
import org.apache.http.HttpStatus
import org.apache.http.client.methods.{HttpGet, HttpPost}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.http.util.EntityUtils

import java.sql.Connection
import scala.util.control.Breaks
import scala.util.{Failure, Success, Try}

class DorisFrontend(config: DorisConfig) {

  private val MAPPER = JsonMapper.builder().addModule(DefaultScalaModule).build()

  private val user: String = config.getValue(DorisOptions.DORIS_USER)

  private val password: String = config.getValue(DorisOptions.DORIS_PASSWORD)

  private val frontends: List[Frontend] = initFrontends()

  private var httpsEnabled: Boolean = false

  private lazy val httpClient: CloseableHttpClient = HttpUtils.getHttpClient(config)

  private def initFrontends(): List[Frontend] = {
    val fenodes = config.getValue(DorisOptions.DORIS_FENODES)
    httpsEnabled = config.getValue(DorisOptions.DORIS_ENABLE_HTTPS)
    fenodes.split(",").map(fenode => {
      val feNodeArr = fenode.split(":")
      requestFrontends(List(Frontend(feNodeArr(0), feNodeArr(1).toInt)))((frontend, httpClient) => {
        val httpGet = new HttpGet(URLs.getFrontEndNodes(frontend.host, frontend.httpPort, httpsEnabled))
        HttpUtils.setAuth(httpGet, user, password)
        val response = httpClient.execute(httpGet)
        if (response.getStatusLine.getStatusCode != HttpStatus.SC_OK) {
          throw new Exception()
        }
        val entity = EntityUtils.toString(response.getEntity)
        val dataNode = extractEntity(entity, "data")
        val columnNames = dataNode.get("columnNames").asInstanceOf[ArrayNode]
        val rows = dataNode.get("rows").asInstanceOf[ArrayNode]
        parseFrontends(columnNames, rows)
      })
    }).head.get
  }

  def requestFrontends[T](frontEnds: List[Frontend] = frontends)(f: (Frontend, CloseableHttpClient) => T): Option[T] = {
    val breaks = new Breaks
    var requestResult: Option[T] = None
    breaks.breakable {
      frontEnds.foreach(frontEnd => {
        val result = Try(f(frontEnd, httpClient))
        result match {
          case Success(res) => requestResult = Some(res)
          case Failure(exception) =>
            // todo
            exception.printStackTrace()
        }
      })
    }
    requestResult
  }

  def queryFrontends[T](frontEnds: List[Frontend] = frontends)(f: Connection => T): Option[T] = {
    val breaks = new Breaks
    var requestResult: Option[T] = None
    breaks.breakable {
      frontEnds.foreach(frontEnd => {
        val result = Try(JdbcUtils.withConnection[T](frontEnd.host, frontEnd.queryPort, user, password)(f))
        result match {
          case Success(res) =>
            requestResult = Some(res)
            breaks.break()
          case Failure(exception) => // todo
        }
      })
    }
    requestResult
  }

  def listTables(databases: Array[String]): Array[(Array[String], String)] = {
    queryFrontends()(conn => {
      val where = if (databases.length == 1) s" WHERE TABLE_SCHEMA = '${databases.head}'" else ""
      val ps = conn.prepareStatement("SELECT TABLE_NAME FROM `information_schema`.`tables`" + where)
      val resultSet = ps.executeQuery()
      new Iterator[(Array[String], String)] {
        override def hasNext: Boolean = resultSet.next()
        override def next(): (Array[String], String) = (databases, resultSet.getString(1))
      }.toArray
    }).get
  }

  def listDatabases(): Array[String] = {
    queryFrontends()(conn => {
      val ps = conn.prepareStatement("SELECT SCHEMA_NAME FROM `information_schema`.`schemata`")
      val resultSet = ps.executeQuery()
      new Iterator[String] {
        override def hasNext: Boolean = resultSet.next()
        override def next(): String = resultSet.getString(1)
      }.toArray.filter(_ != "information_schema")
    }).get
  }

  def databaseExists(database: String): Boolean = {
    if (StringUtils.isBlank(database)) {
      return false
    }
    queryFrontends()(conn => {
      val ps = conn.prepareStatement(s"SELECT SCHEMA_NAME FROM `information_schema`.`schemata` WHERE SCHEMA_NAME = '$database'")
      val resultSet = ps.executeQuery()
      while (resultSet.next()) {
        if (resultSet.getString(1) == database) return true
      }
      false
    }).get
  }

  def getTableSchema(db: String, table: String): DorisSchema = {
    requestFrontends()((frontend, httpClient) => {
      val httpGet = new HttpGet(URLs.tableSchema(frontend.host, frontend.httpPort, db, table, httpsEnabled))
      HttpUtils.setAuth(httpGet, user, password)
      val response = httpClient.execute(httpGet)
      if (response.getStatusLine.getStatusCode != HttpStatus.SC_OK) {
        throw new Exception()
      }
      val entity = EntityUtils.toString(response.getEntity)
      val dorisSchema = MAPPER.readValue(extractEntity(entity, "data").traverse(), classOf[DorisSchema])
      val unsupportedCols = dorisSchema.properties.filter(field => field.`type`.toUpperCase == "BITMAP" || field.`type`.toUpperCase == "HLL")
      config.setProperty(DorisOptions.DORIS_UNSUPPORTED_COLUMNS, unsupportedCols.map(c => s"`$c`").mkString(","))
      dorisSchema
    }).get
  }

  private def parseFrontends(columnNames: ArrayNode, rows: ArrayNode): List[Frontend] = {
    var hostIdx = -1
    var httpPortIdx = -1
    var queryPortIdx = -1
    var flightSqlIdx = -1
    (0 until columnNames.size()).foreach(idx => {
      columnNames.get(idx).asText() match {
        case "Host" => hostIdx = idx
        case "HttpPort" => httpPortIdx = idx
        case "QueryPort" => queryPortIdx = idx
        case "ArrowFlightSqlPort" => flightSqlIdx = idx
        case _ => // do nothing
      }
    })
    if (rows.size() == 0) {
      Breaks.break()
    }
    (0 until rows.size()).map(rowIdx => {
      val row = rows.get(rowIdx).asInstanceOf[ArrayNode]
      if (flightSqlIdx == -1) {
        Frontend(row.get(hostIdx).asText(), row.get(httpPortIdx).asInt(), row.get(queryPortIdx).asInt())
      } else {
        Frontend(row.get(hostIdx).asText(), row.get(httpPortIdx).asInt(), row.get(queryPortIdx).asInt(), row.get(flightSqlIdx).asInt())
      }
    }).toList
  }

  def getQueryPlan(database: String, table: String, sql: String): QueryPlan = {
    requestFrontends()((frontend, httpClient) => {
      val httpPost = new HttpPost(URLs.queryPlan(frontend.host, frontend.httpPort, database, table, httpsEnabled))
      HttpUtils.setAuth(httpPost, user, password)
      val body = MAPPER.writeValueAsString(Map[String, String]("sql" -> sql))
      httpPost.setEntity(new StringEntity(body))
      val response = httpClient.execute(httpPost)
      if (response.getStatusLine.getStatusCode != HttpStatus.SC_OK) {
        throw new Exception()
      }
      val entity = EntityUtils.toString(response.getEntity)
      MAPPER.readValue(extractEntity(entity, "data").traverse(), classOf[QueryPlan])
    }).get
  }

  private def extractEntity(entityStr: String, fieldName: String): JsonNode = {
    MAPPER.readTree(entityStr).get(fieldName)
  }

  def getTableAllColumns(db: String, table: String): Array[String] = {
    queryFrontends()(conn => {
      val ps = conn.prepareStatement(s"DESC `$db`.`$table`")
      try {
        val rs = ps.executeQuery()
        new Iterator[String] {
          override def hasNext: Boolean = rs.next()
          override def next(): String = rs.getString(1)
        }.toArray
      } finally {
        ps.close()
      }
    }).get
  }

  def close(): Unit = {
    httpClient.close()
  }

}

object DorisFrontend {
  def apply(config: DorisConfig): DorisFrontend = new DorisFrontend(config)
}
