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

import org.apache.doris.spark.cfg.{ConfigurationOptions, SparkSettings}
import org.apache.http.client.config.RequestConfig
import org.apache.http.conn.ssl.{SSLConnectionSocketFactory, TrustAllStrategy}
import org.apache.http.impl.client.{CloseableHttpClient, DefaultRedirectStrategy, HttpClients}
import org.apache.http.ssl.SSLContexts

import java.io.{File, FileInputStream}
import java.security.KeyStore
import scala.util.{Failure, Success, Try}

object HttpUtil {

  def getHttpClient(settings: SparkSettings): CloseableHttpClient = {
    val connectTimeout = settings.getIntegerProperty(ConfigurationOptions.DORIS_REQUEST_CONNECT_TIMEOUT_MS,
      ConfigurationOptions.DORIS_REQUEST_CONNECT_TIMEOUT_MS_DEFAULT)
    val socketTimeout = settings.getIntegerProperty(ConfigurationOptions.DORIS_REQUEST_READ_TIMEOUT_MS,
      ConfigurationOptions.DORIS_REQUEST_READ_TIMEOUT_MS_DEFAULT)
    val requestConfig = RequestConfig.custom().setConnectTimeout(connectTimeout).setSocketTimeout(socketTimeout).build()
    val clientBuilder = HttpClients.custom()
      .setRedirectStrategy(new DefaultRedirectStrategy {
        override def isRedirectable(method: String): Boolean = true
      })
      .setDefaultRequestConfig(requestConfig)
    val enableHttps = settings.getBooleanProperty("doris.enable.https", false)
    if (enableHttps) {
      val props = settings.asProperties()
      require(props.containsKey(ConfigurationOptions.DORIS_HTTPS_KEY_STORE_PATH))
      val keyStorePath: String = props.getProperty(ConfigurationOptions.DORIS_HTTPS_KEY_STORE_PATH)
      val keyStoreFile = new File(keyStorePath)
      if (!keyStoreFile.exists()) throw new IllegalArgumentException()
      val keyStoreType: String = props.getProperty(ConfigurationOptions.DORIS_HTTPS_KEY_STORE_TYPE,
        ConfigurationOptions.DORIS_HTTPS_KEY_STORE_TYPE_DEFAULT)
      val keyStore = KeyStore.getInstance(keyStoreType)
      var fis: FileInputStream = null
      Try {
        fis = new FileInputStream(keyStoreFile)
        val password = props.getProperty(ConfigurationOptions.DORIS_HTTPS_KEY_STORE_PASSWORD)
        keyStore.load(fis, if (password == null) null else password.toCharArray)
      } match {
        case Success(_) => if (fis != null) fis.close()
        case Failure(e) =>
          if (fis != null) fis.close()
          throw e
      }
      val sslContext = SSLContexts.custom().loadTrustMaterial(keyStore, new TrustAllStrategy).build()
      clientBuilder.setSSLSocketFactory(new SSLConnectionSocketFactory(sslContext))
    }
    clientBuilder.build()
  }

}
