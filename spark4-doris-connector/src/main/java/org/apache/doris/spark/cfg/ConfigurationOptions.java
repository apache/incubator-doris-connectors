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

package org.apache.doris.spark.cfg;

public interface ConfigurationOptions {
    // doris fe node address
    String DORIS_FENODES = "doris.fenodes";

    String DORIS_BENODES = "doris.benodes";
    String DORIS_QUERY_PORT = "doris.query.port";

    String DORIS_DEFAULT_CLUSTER = "default_cluster";

    String TABLE_IDENTIFIER = "table.identifier";
    String DORIS_TABLE_IDENTIFIER = "doris.table.identifier";
    String DORIS_READ_FIELD = "doris.read.field";
    String DORIS_FILTER_QUERY = "doris.filter.query";
    String DORIS_FILTER_QUERY_IN_MAX_COUNT = "doris.filter.query.in.max.count";
    int DORIS_FILTER_QUERY_IN_VALUE_UPPER_LIMIT = 10000;

    String DORIS_USER = "doris.user";
    String DORIS_REQUEST_AUTH_USER = "doris.request.auth.user";
    // use password to save doris.request.auth.password
    // reuse credentials mask method in spark ExternalCatalogUtils#maskCredentials
    String DORIS_PASSWORD = "doris.password";
    String DORIS_REQUEST_AUTH_PASSWORD = "doris.request.auth.password";

    String DORIS_REQUEST_RETRIES = "doris.request.retries";
    String DORIS_REQUEST_CONNECT_TIMEOUT_MS = "doris.request.connect.timeout.ms";
    String DORIS_REQUEST_READ_TIMEOUT_MS = "doris.request.read.timeout.ms";
    String DORIS_REQUEST_QUERY_TIMEOUT_S = "doris.request.query.timeout.s";
    int DORIS_REQUEST_RETRIES_DEFAULT = 3;
    int DORIS_REQUEST_CONNECT_TIMEOUT_MS_DEFAULT = 30 * 1000;
    int DORIS_REQUEST_READ_TIMEOUT_MS_DEFAULT = 30 * 1000;
    int DORIS_REQUEST_QUERY_TIMEOUT_S_DEFAULT = 6 * 60 * 60;

    String DORIS_TABLET_SIZE = "doris.request.tablet.size";
    int DORIS_TABLET_SIZE_DEFAULT = 1;
    int DORIS_TABLET_SIZE_MIN = 1;

    String DORIS_BATCH_SIZE = "doris.batch.size";
    int DORIS_BATCH_SIZE_DEFAULT = 1024;

    String DORIS_EXEC_MEM_LIMIT = "doris.exec.mem.limit";
    long DORIS_EXEC_MEM_LIMIT_DEFAULT = 8L * 1024 * 1024 * 1024;

    String DORIS_VALUE_READER_CLASS = "doris.value.reader.class";

    String DORIS_DESERIALIZE_ARROW_ASYNC = "doris.deserialize.arrow.async";
    boolean DORIS_DESERIALIZE_ARROW_ASYNC_DEFAULT = false;

    String DORIS_DESERIALIZE_QUEUE_SIZE = "doris.deserialize.queue.size";
    int DORIS_DESERIALIZE_QUEUE_SIZE_DEFAULT = 64;

    String DORIS_WRITE_FIELDS = "doris.write.fields";

    String DORIS_SINK_BATCH_SIZE = "doris.sink.batch.size";
    int SINK_BATCH_SIZE_DEFAULT = 100000;

    String DORIS_SINK_MAX_RETRIES = "doris.sink.max-retries";
    int SINK_MAX_RETRIES_DEFAULT = 0;

    String DORIS_MAX_FILTER_RATIO = "doris.max.filter.ratio";

    String STREAM_LOAD_PROP_PREFIX = "doris.sink.properties.";

    String DORIS_SINK_TASK_PARTITION_SIZE = "doris.sink.task.partition.size";

    /**
     * Set doris sink task partition size. If you set a small coalesce size and you don't have the action operations, this may result in the same parallelism in your computation.
     * To avoid this, you can use repartition operations. This will add a shuffle step, but means the current upstream partitions will be executed in parallel.
     */
    String DORIS_SINK_TASK_USE_REPARTITION = "doris.sink.task.use.repartition";

    boolean DORIS_SINK_TASK_USE_REPARTITION_DEFAULT = false;

    String DORIS_SINK_BATCH_INTERVAL_MS = "doris.sink.batch.interval.ms";

    int DORIS_SINK_BATCH_INTERVAL_MS_DEFAULT = 50;

    /**
     * set types to ignore, split by comma
     * e.g.
     * "doris.ignore-type"="bitmap,hll"
     */
    String DORIS_IGNORE_TYPE = "doris.ignore-type";

    String DORIS_SINK_ENABLE_2PC = "doris.sink.enable-2pc";
    boolean DORIS_SINK_ENABLE_2PC_DEFAULT = false;

    /**
     * pass through json data when sink to doris in streaming mode
     */
    String DORIS_SINK_STREAMING_PASSTHROUGH = "doris.sink.streaming.passthrough";
    boolean DORIS_SINK_STREAMING_PASSTHROUGH_DEFAULT = false;

    /**
     * txnId commit or abort interval
     */
    String DORIS_SINK_TXN_INTERVAL_MS = "doris.sink.txn.interval.ms";
    int DORIS_SINK_TXN_INTERVAL_MS_DEFAULT = 50;

    /**
     * txnId commit or abort retry times
     */
    String DORIS_SINK_TXN_RETRIES = "doris.sink.txn.retries";
    int DORIS_SINK_TXN_RETRIES_DEFAULT = 3;

    /**
     * Use automatic redirection of fe without explicitly obtaining the be list
     */
    String DORIS_SINK_AUTO_REDIRECT = "doris.sink.auto-redirect";
    boolean DORIS_SINK_AUTO_REDIRECT_DEFAULT = true;

    /**
     * compress_type
     */
    String DORIS_SINK_DATA_COMPRESS_TYPE = "doris.sink.properties.compress_type";

    String DORIS_ENABLE_HTTPS = "doris.enable.https";

    boolean DORIS_ENABLE_HTTPS_DEFAULT = false;

    String DORIS_HTTPS_KEY_STORE_PATH = "doris.https.key-store-path";

    String DORIS_HTTPS_KEY_STORE_TYPE = "doris.https.key-store-type";

    String DORIS_HTTPS_KEY_STORE_TYPE_DEFAULT = "JKS";

    String DORIS_HTTPS_KEY_STORE_PASSWORD = "doris.https.key-store-password";

    String LOAD_MODE = "doris.sink.load.mode";
    String DEFAULT_LOAD_MODE = "stream_load";

}
