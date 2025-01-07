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
package org.apache.doris.spark.load;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.doris.spark.cfg.ConfigurationOptions;
import org.apache.doris.spark.cfg.SparkSettings;
import org.apache.doris.spark.exception.IllegalArgumentException;
import org.apache.doris.spark.exception.StreamLoadException;
import org.apache.doris.spark.rest.RestService;
import org.apache.doris.spark.rest.models.BackendV2;
import org.apache.doris.spark.rest.models.RespContent;
import org.apache.doris.spark.sql.Utils;
import org.apache.doris.spark.util.ResponseUtil;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.BufferedHttpEntity;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.zip.GZIPOutputStream;

/**
 * DorisStreamLoad
 **/
@Deprecated
public class DorisStreamLoad implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(DorisStreamLoad.class);

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final static List<String> DORIS_SUCCESS_STATUS = new ArrayList<>(Arrays.asList("Success", "Publish Timeout"));

    private static final String loadUrlPattern = "http://%s/api/%s/%s/_stream_load";

    private static final String abortUrlPattern = "http://%s/api/%s/_stream_load_2pc";

    private String loadUrlStr;
    private final String db;
    private final String tbl;
    private final String authEncoded;
    private final String columns;
    private final String maxFilterRatio;
    private final Map<String, String> streamLoadProp;
    private boolean addDoubleQuotes;
    private static final long cacheExpireTimeout = 4 * 60;
    private final LoadingCache<String, List<BackendV2.BackendRowV2>> cache;
    private final String fenodes;
    private final String benodes;
    private final DataFormat dataFormat;
    private String FIELD_DELIMITER;
    private final String LINE_DELIMITER;
    private boolean streamingPassthrough = false;
    private final boolean enable2PC;
    private final Integer txnRetries;
    private final Integer txnIntervalMs;
    private final boolean autoRedirect;
    private final String compressType;

    public DorisStreamLoad(SparkSettings settings) {
        String[] dbTable = settings.getProperty(ConfigurationOptions.DORIS_TABLE_IDENTIFIER).split("\\.");
        this.db = dbTable[0];
        this.tbl = dbTable[1];
        this.fenodes = settings.getProperty(ConfigurationOptions.DORIS_FENODES);
        this.benodes = settings.getProperty(ConfigurationOptions.DORIS_BENODES);
        String user = settings.getProperty(ConfigurationOptions.DORIS_REQUEST_AUTH_USER);
        String passwd = settings.getProperty(ConfigurationOptions.DORIS_REQUEST_AUTH_PASSWORD);
        this.authEncoded = getAuthEncoded(user, passwd);
        this.columns = settings.getProperty(ConfigurationOptions.DORIS_WRITE_FIELDS);
        this.maxFilterRatio = settings.getProperty(ConfigurationOptions.DORIS_MAX_FILTER_RATIO);
        this.streamLoadProp = getStreamLoadProp(settings);
        cache = CacheBuilder.newBuilder().expireAfterWrite(cacheExpireTimeout, TimeUnit.MINUTES).build(new BackendCacheLoader(settings));
        dataFormat = DataFormat.valueOf(streamLoadProp.getOrDefault("format", "csv").toUpperCase());
        switch (dataFormat) {
            case CSV:
                FIELD_DELIMITER = escapeString(streamLoadProp.getOrDefault("column_separator", "\t"));
                this.addDoubleQuotes = Boolean.parseBoolean(streamLoadProp.getOrDefault("add_double_quotes", "false"));
                if (addDoubleQuotes) {
                    LOG.info("set add_double_quotes for csv mode, add trim_double_quotes to true for prop.");
                    streamLoadProp.put("trim_double_quotes", "true");
                }
                break;
            case JSON:
                streamLoadProp.put("read_json_by_line", "true");
                break;
        }
        LINE_DELIMITER = escapeString(streamLoadProp.getOrDefault("line_delimiter", "\n"));
        this.streamingPassthrough = settings.getBooleanProperty(ConfigurationOptions.DORIS_SINK_STREAMING_PASSTHROUGH,
                ConfigurationOptions.DORIS_SINK_STREAMING_PASSTHROUGH_DEFAULT);
        this.enable2PC = settings.getBooleanProperty(ConfigurationOptions.DORIS_SINK_ENABLE_2PC,
                ConfigurationOptions.DORIS_SINK_ENABLE_2PC_DEFAULT);
        this.txnRetries = settings.getIntegerProperty(ConfigurationOptions.DORIS_SINK_TXN_RETRIES,
                ConfigurationOptions.DORIS_SINK_TXN_RETRIES_DEFAULT);
        this.txnIntervalMs = settings.getIntegerProperty(ConfigurationOptions.DORIS_SINK_TXN_INTERVAL_MS,
                ConfigurationOptions.DORIS_SINK_TXN_INTERVAL_MS_DEFAULT);

        this.autoRedirect = settings.getBooleanProperty(ConfigurationOptions.DORIS_SINK_AUTO_REDIRECT,
                ConfigurationOptions.DORIS_SINK_AUTO_REDIRECT_DEFAULT);
        compressType=settings.getProperty(ConfigurationOptions.DORIS_SINK_DATA_COMPRESS_TYPE);
    }

    public String getLoadUrlStr() {
        if (StringUtils.isEmpty(loadUrlStr)) {
            return "";
        }
        return loadUrlStr;
    }

    private CloseableHttpClient getHttpClient() {
        HttpClientBuilder httpClientBuilder = HttpClients
                .custom()
                .setRedirectStrategy(new DefaultRedirectStrategy() {
                    @Override
                    protected boolean isRedirectable(String method) {
                        return true;
                    }
                });
        return httpClientBuilder.build();
    }

    private HttpPut getHttpPut(String label, String loadUrlStr, Boolean enable2PC, StructType schema) {
        HttpPut httpPut = new HttpPut(loadUrlStr);
        addCommonHeader(httpPut);
        httpPut.setHeader("label", label);
        if (StringUtils.isNotBlank(columns)) {
            httpPut.setHeader("columns", columns);
        } else {
            if (schema != null && !schema.isEmpty()) {
                String dfColumns = Arrays.stream(schema.fieldNames()).map(Utils::quote).collect(Collectors.joining(","));
                httpPut.setHeader("columns", dfColumns);
            }
        }
        if (StringUtils.isNotBlank(maxFilterRatio)) {
            httpPut.setHeader("max_filter_ratio", maxFilterRatio);
        }
        if (enable2PC) {
            httpPut.setHeader("two_phase_commit", "true");
        }
        if (MapUtils.isNotEmpty(streamLoadProp)) {
            streamLoadProp.forEach((k, v) -> {
                if (!"strip_outer_array".equalsIgnoreCase(k)) {
                    httpPut.setHeader(k, v);
                }
            });
        }
        return httpPut;
    }

    public static class LoadResponse {
        public int status;
        public String respMsg;
        public String respContent;

        public LoadResponse(HttpResponse response) throws IOException {
            this.status = response.getStatusLine().getStatusCode();
            this.respMsg = response.getStatusLine().getReasonPhrase();
            this.respContent = EntityUtils.toString(new BufferedHttpEntity(response.getEntity()), StandardCharsets.UTF_8);
        }

        @Override
        public String toString() {
            return "status: " + status + ", resp msg: " + respMsg + ", resp content: " + respContent;
        }
    }

    public long load(Iterator<InternalRow> rows, StructType schema)
            throws StreamLoadException, JsonProcessingException {

        String label = generateLoadLabel();
        LoadResponse loadResponse;
        try (CloseableHttpClient httpClient = getHttpClient()) {
            String loadUrlStr = String.format(loadUrlPattern, getBackend(), db, tbl);
            this.loadUrlStr = loadUrlStr;
            HttpPut httpPut = getHttpPut(label, loadUrlStr, enable2PC, schema);

            if(StringUtils.isNotEmpty(compressType)){
                if("gz".equals(compressType.toLowerCase()) && dataFormat.equals(DataFormat.CSV) ){
                    RecordBatchString recordBatchString = new RecordBatchString(RecordBatch.newBuilder(rows)
                            .format(dataFormat)
                            .sep(FIELD_DELIMITER)
                            .delim(LINE_DELIMITER)
                            .schema(schema)
                            .addDoubleQuotes(addDoubleQuotes).build(), streamingPassthrough);
                    String content = recordBatchString.getContent();
                    byte[] compressedData = compressByGZ(content);
                    httpPut.setEntity(new ByteArrayEntity(compressedData));
                }else{
                    throw new StreamLoadException("Not support the compress type [" + compressType + "] for the dataformat [" + dataFormat + "]");
                }
            }else{
                RecordBatchInputStream recodeBatchInputStream = new RecordBatchInputStream(RecordBatch.newBuilder(rows)
                        .format(dataFormat)
                        .sep(FIELD_DELIMITER)
                        .delim(LINE_DELIMITER)
                        .schema(schema)
                        .addDoubleQuotes(addDoubleQuotes).build(), streamingPassthrough);
                httpPut.setEntity(new InputStreamEntity(recodeBatchInputStream));
            }
            HttpResponse httpResponse = httpClient.execute(httpPut);
            loadResponse = new LoadResponse(httpResponse);
        } catch (IOException e) {
            if (enable2PC) {
                int retries = txnRetries;
                while (retries > 0) {
                    try {
                        abortByLabel(label);
                        retries = 0;
                    } catch (StreamLoadException ex) {
                        LockSupport.parkNanos(Duration.ofMillis(txnIntervalMs).toNanos());
                        retries--;
                    }
                }
            }
            throw new StreamLoadException("load execute failed", e);
        }

        if (loadResponse.status != HttpStatus.SC_OK) {
            LOG.error("Stream load http status is not OK, status: {}, response: {}", loadResponse.status, loadResponse);
            throw new StreamLoadException(
                    String.format("stream load error, http status:%d, response:%s", loadResponse.status, loadResponse));
        } else {
            try {
                RespContent respContent = MAPPER.readValue(loadResponse.respContent, RespContent.class);
                if (!DORIS_SUCCESS_STATUS.contains(respContent.getStatus())) {
                    LOG.error("Stream load status is not success, status:{}, response:{}", respContent.getStatus(), loadResponse);
                    throw new StreamLoadException(
                            String.format("stream load error, load status:%s, response:%s", respContent.getStatus(), loadResponse));
                }
                LOG.info("Stream load Response:{}", loadResponse);
                return respContent.getTxnId();
            } catch (IOException e) {
                throw new StreamLoadException(e);
            }
        }

    }

    public Long loadStream(Iterator<InternalRow> rows, StructType schema)
            throws StreamLoadException, JsonProcessingException {
        if (this.streamingPassthrough) {
            handleStreamPassThrough();
        }
        return load(rows, schema);
    }

    public void commit(long txnId) throws StreamLoadException {

        try (CloseableHttpClient client = getHttpClient()) {

            String backend = getBackend();
            String abortUrl = String.format(abortUrlPattern, backend, db);
            HttpPut httpPut = new HttpPut(abortUrl);
            addCommonHeader(httpPut);
            httpPut.setHeader("txn_operation", "commit");
            httpPut.setHeader("txn_id", String.valueOf(txnId));

            CloseableHttpResponse response = client.execute(httpPut);
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode != 200 || response.getEntity() == null) {
                LOG.warn("commit transaction response: " + response.getStatusLine().toString());
                throw new StreamLoadException("Fail to commit transaction " + txnId + " with url " + abortUrl);
            }

            statusCode = response.getStatusLine().getStatusCode();
            String reasonPhrase = response.getStatusLine().getReasonPhrase();
            if (statusCode != 200) {
                LOG.warn("commit failed with {}, reason {}", backend, reasonPhrase);
                throw new StreamLoadException("stream load error: " + reasonPhrase);
            }

            if (response.getEntity() != null) {
                String loadResult = EntityUtils.toString(response.getEntity());
                Map<String, String> res = MAPPER.readValue(loadResult, new TypeReference<HashMap<String, String>>() {
                });
                if (res.get("status").equals("Success") || ResponseUtil.isCommitted(res.get("msg"))) {
                    LOG.info("commit transaction {} succeed, load result: {}.", txnId, loadResult);
                } else {
                    LOG.error("commit transaction {} failed. load result: {}", txnId, loadResult);
                    throw new StreamLoadException("Commit failed " + loadResult);
                }
            }

        } catch (IOException e) {
            throw new StreamLoadException(e);
        }

    }

    /**
     * abort transaction by id
     *
     * @param txnId transaction id
     * @throws StreamLoadException
     */
    public void abortById(long txnId) throws StreamLoadException {

        LOG.info("start abort transaction {}.", txnId);

        try {
            doAbort(httpPut -> httpPut.setHeader("txn_id", String.valueOf(txnId)));
        } catch (StreamLoadException e) {
            LOG.error("abort transaction by id: {} failed.", txnId);
            throw e;
        }

        LOG.info("abort transaction {} succeed.", txnId);

    }

    /**
     * abort transaction by label
     *
     * @param label label
     * @throws StreamLoadException
     */
    public void abortByLabel(String label) throws StreamLoadException {

        LOG.info("start abort transaction by label: {}.", label);

        try {
            doAbort(httpPut -> httpPut.setHeader("label", label));
        } catch (StreamLoadException e) {
            LOG.error("abort transaction by label: {} failed.", label);
            throw e;
        }

        LOG.info("abort transaction by label {} succeed.", label);

    }

    /**
     * execute abort
     *
     * @param putConsumer http put process function
     * @throws StreamLoadException
     */
    private void doAbort(Consumer<HttpPut> putConsumer) throws StreamLoadException {

        try (CloseableHttpClient client = getHttpClient()) {
            String abortUrl = String.format(abortUrlPattern, getBackend(), db);
            HttpPut httpPut = new HttpPut(abortUrl);
            addCommonHeader(httpPut);
            httpPut.setHeader("txn_operation", "abort");
            putConsumer.accept(httpPut);

            CloseableHttpResponse response = client.execute(httpPut);
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode != 200 || response.getEntity() == null) {
                LOG.error("abort transaction response: " + response.getStatusLine().toString());
                throw new IOException("Fail to abort transaction with url " + abortUrl);
            }

            String loadResult = EntityUtils.toString(response.getEntity());
            Map<String, String> res = MAPPER.readValue(loadResult, new TypeReference<HashMap<String, String>>() {
            });
            if (!"Success".equals(res.get("status"))) {
                if (ResponseUtil.isCommitted(res.get("msg"))) {
                    throw new IOException("try abort committed transaction");
                }
                LOG.error("Fail to abort transaction. error: {}", res.get("msg"));
                throw new IOException(String.format("Fail to abort transaction. error: %s", res.get("msg")));
            }

        } catch (IOException e) {
            throw new StreamLoadException(e);
        }

    }

    public Map<String, String> getStreamLoadProp(SparkSettings sparkSettings) {
        Map<String, String> streamLoadPropMap = new HashMap<>();
        Properties properties = sparkSettings.asProperties();
        for (String key : properties.stringPropertyNames()) {
            if (key.contains(ConfigurationOptions.STREAM_LOAD_PROP_PREFIX)) {
                String subKey = key.substring(ConfigurationOptions.STREAM_LOAD_PROP_PREFIX.length());
                streamLoadPropMap.put(subKey, properties.getProperty(key));
            }
        }
        return streamLoadPropMap;
    }

    private String getBackend() {
        try {
            if (autoRedirect) {
                return RestService.randomEndpoint(fenodes, LOG);
            }
            // get backends from cache
            List<BackendV2.BackendRowV2> backends = cache.get("backends");
            Collections.shuffle(backends);
            BackendV2.BackendRowV2 backend = backends.get(0);
            return backend.getIp() + ":" + backend.getHttpPort();
        } catch (ExecutionException e) {
            throw new RuntimeException("get backends info fail", e);
        }
        catch (IllegalArgumentException e) {
            throw new RuntimeException("get frontend info fail", e);
        }
    }

    private static String getAuthEncoded(String user, String passwd) {
        String raw = String.format("%s:%s", user, passwd);
        return Base64.getEncoder().encodeToString(raw.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * serializable be cache loader
     */
    private static class BackendCacheLoader extends CacheLoader<String, List<BackendV2.BackendRowV2>> implements Serializable {

        private final SparkSettings settings;

        public BackendCacheLoader(SparkSettings settings) {
            this.settings = settings;
        }

        @Override
        public List<BackendV2.BackendRowV2> load(String key) throws Exception {
            return RestService.getBackendRows(settings, LOG);
        }
    }

    private String generateLoadLabel() {

        Calendar calendar = Calendar.getInstance();
        return String.format("spark_streamload_%s%02d%02d_%02d%02d%02d_%s",
                calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH) + 1, calendar.get(Calendar.DAY_OF_MONTH),
                calendar.get(Calendar.HOUR_OF_DAY), calendar.get(Calendar.MINUTE), calendar.get(Calendar.SECOND),
                UUID.randomUUID().toString().replaceAll("-", ""));

    }

    private String escapeString(String hexData) {
        if (hexData.startsWith("\\x") || hexData.startsWith("\\X")) {
            try {
                hexData = hexData.substring(2);
                StringBuilder stringBuilder = new StringBuilder();
                for (int i = 0; i < hexData.length(); i += 2) {
                    String hexByte = hexData.substring(i, i + 2);
                    int decimal = Integer.parseInt(hexByte, 16);
                    char character = (char) decimal;
                    stringBuilder.append(character);
                }
                return stringBuilder.toString();
            } catch (Exception e) {
                throw new RuntimeException("escape column_separator or line_delimiter error.{}", e);
            }
        }
        return hexData;
    }

    /**
     * add common header to http request
     *
     * @param httpReq http request
     */
    private void addCommonHeader(HttpRequestBase httpReq) {
        httpReq.setHeader(HttpHeaders.AUTHORIZATION, "Basic " + authEncoded);
        httpReq.setHeader(HttpHeaders.EXPECT, "100-continue");
        httpReq.setHeader(HttpHeaders.CONTENT_TYPE, "text/plain; charset=UTF-8");
    }

    /**
     * handle stream sink data pass through
     * if load format is json, set read_json_by_line to true and remove strip_outer_array parameter
     */
    private void handleStreamPassThrough() {

        if (dataFormat.equals(DataFormat.JSON)) {
            LOG.info("handle stream pass through, force set read_json_by_line is true for json format");
            streamLoadProp.put("read_json_by_line", "true");
            streamLoadProp.remove("strip_outer_array");
        }

    }

    /**
     * compress data by gz compression algorithm
     */
    public byte[] compressByGZ(String content) throws IOException{
        byte[] compressedData;
        try(ByteArrayOutputStream baos = new ByteArrayOutputStream();
            GZIPOutputStream gzipOutputStream = new GZIPOutputStream(baos);
        ){
            gzipOutputStream.write(content.getBytes("UTF-8"));
            gzipOutputStream.finish();
            compressedData = baos.toByteArray();
        }
        return compressedData;
    }

}
