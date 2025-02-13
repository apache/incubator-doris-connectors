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

package org.apache.doris.spark.client.read;

import org.apache.doris.sdk.thrift.TPrimitiveType;
import org.apache.doris.sdk.thrift.TScanBatchResult;
import org.apache.doris.sdk.thrift.TScanCloseParams;
import org.apache.doris.sdk.thrift.TScanNextBatchParams;
import org.apache.doris.sdk.thrift.TScanOpenParams;
import org.apache.doris.sdk.thrift.TScanOpenResult;
import org.apache.doris.spark.client.DorisBackendThriftClient;
import org.apache.doris.spark.client.DorisFrontendClient;
import org.apache.doris.spark.client.entity.DorisReaderPartition;
import org.apache.doris.spark.config.DorisOptions;
import org.apache.doris.spark.exception.ConnectedFailedException;
import org.apache.doris.spark.exception.DorisException;
import org.apache.doris.spark.exception.OptionRequiredException;
import org.apache.doris.spark.rest.models.Field;
import org.apache.doris.spark.rest.models.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.stream.Collectors;

public abstract class AbstractThriftReader extends DorisReader {

    protected final Logger logger = LoggerFactory.getLogger(this.getClass().getName().replace("$", ""));

    protected final DorisFrontendClient frontend;
    private final DorisBackendThriftClient backend;

    private int offset = 0;

    private final AtomicBoolean endOfStream = new AtomicBoolean(false);
    private final boolean isAsync;

    private final Lock backendLock;
    private final TScanOpenParams scanOpenParams;
    protected final TScanOpenResult scanOpenResult;
    protected final String contextId;
    protected Schema dorisSchema;

    private final BlockingQueue<RowBatch> rowBatchQueue;

    private final Thread asyncThread;

    private int readCount = 0;

    private final Boolean datetimeJava8ApiEnabled;

    protected AbstractThriftReader(DorisReaderPartition partition) throws Exception {
        super(partition);
        this.frontend = new DorisFrontendClient(config);
        this.backend = new DorisBackendThriftClient(partition.getBackend(), config);
        this.isAsync = config.getValue(DorisOptions.DORIS_DESERIALIZE_ARROW_ASYNC);
        this.backendLock = isAsync ? new ReentrantLock() : new NoOpLock();
        this.scanOpenParams = buildScanOpenParams();
        this.scanOpenResult = lockClient(backend -> {
            try {
                return backend.openScanner(scanOpenParams);
            } catch (ConnectedFailedException e) {
                throw new RuntimeException("backend open scanner failed", e);
            }
        });
        this.contextId = scanOpenResult.getContextId();
        Schema schema = getDorisSchema();
        this.dorisSchema = processDorisSchema(partition, schema);
        if (logger.isDebugEnabled()) {
            logger.debug("origin thrift read Schema: " + schema + ", processed schema: " + dorisSchema);
        }
        if (isAsync) {
            int blockingQueueSize = config.getValue(DorisOptions.DORIS_DESERIALIZE_QUEUE_SIZE);
            this.rowBatchQueue = new ArrayBlockingQueue<>(blockingQueueSize);
            this.asyncThread = new Thread(() -> {
                try {
                    runAsync();
                } catch (DorisException | InterruptedException e) {
                    throw new RuntimeException("backend read async failed", e);
                }
            });
            this.asyncThread.start();
        } else {
            this.rowBatchQueue = null;
            this.asyncThread = null;
        }
        this.datetimeJava8ApiEnabled = false;
    }

    private void runAsync() throws DorisException, InterruptedException {
        TScanNextBatchParams nextBatchParams = new TScanNextBatchParams();
        nextBatchParams.setContextId(contextId);
        while (!endOfStream.get()) {
            nextBatchParams.setOffset(offset);
            TScanBatchResult nextResult = lockClient(backend -> {
                try {
                    return backend.getNext(nextBatchParams);
                } catch (DorisException e) {
                    throw new RuntimeException("backend async get next failed", e);
                }
            });
            endOfStream.set(nextResult.isEos());
            if (!endOfStream.get()) {
                rowBatch = new RowBatch(nextResult, dorisSchema, datetimeJava8ApiEnabled);
                offset += rowBatch.getReadRowCount();
                rowBatch.close();
                rowBatchQueue.put(rowBatch);
            }
        }
    }

    @Override
    public boolean hasNext() throws DorisException {
        if (partition.getLimit() > 0 && readCount >= partition.getLimit()) {
            return false;
        }
        boolean hasNext = false;
        if (isAsync && asyncThread != null && asyncThread.isAlive()) {
            if (rowBatch == null || !rowBatch.hasNext()) {
                while (!endOfStream.get() || !rowBatchQueue.isEmpty()) {
                    if (!rowBatchQueue.isEmpty()) {
                        try {
                            rowBatch = rowBatchQueue.take();
                            hasNext = true;
                            break;
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    } else {
                        try {
                            Thread.sleep(5);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                }
            } else {
                hasNext = true;
            }
        } else {
            if (!endOfStream.get() && (rowBatch == null || !rowBatch.hasNext())) {
                if (rowBatch != null) {
                    offset += rowBatch.getReadRowCount();
                    rowBatch.close();
                }
                TScanNextBatchParams nextBatchParams = new TScanNextBatchParams();
                nextBatchParams.setContextId(contextId);
                nextBatchParams.setOffset(offset);
                TScanBatchResult nextResult = lockClient(backend -> {
                    try {
                        return backend.getNext(nextBatchParams);
                    } catch (DorisException e) {
                        throw new RuntimeException("backend get next failed", e);
                    }
                });
                endOfStream.set(nextResult.isEos());
                if (!endOfStream.get()) {
                    rowBatch = new RowBatch(nextResult, dorisSchema, datetimeJava8ApiEnabled);
                }
            }
            hasNext = !endOfStream.get();
        }
        return hasNext;
    }

    @Override
    public Object next() throws DorisException {
        if (!hasNext()) {
            throw new RuntimeException("No more elements");
        }
        if (partition.getLimit() > 0) {
            readCount++;
        }
        return rowBatch.next().toArray();
    }

    @Override
    public void close() {
        TScanCloseParams closeParams = new TScanCloseParams();
        closeParams.setContextId(contextId);
        lockClient(backend -> {
            backend.closeScanner(closeParams);
            return null;
        });
    }

    private TScanOpenParams buildScanOpenParams() throws OptionRequiredException {
        TScanOpenParams params = new TScanOpenParams();
        params.setCluster(DorisOptions.DORIS_DEFAULT_CLUSTER);
        params.setDatabase(partition.getDatabase());
        params.setTable(partition.getTable());
        List<Long> tabletIds = new ArrayList<>(Arrays.asList(partition.getTablets()));
        params.setTabletIds(tabletIds);
        params.setOpaquedQueryPlan(partition.getOpaquedQueryPlan());

        int batchSize = config.getValue(DorisOptions.DORIS_BATCH_SIZE);
        if (batchSize > DorisOptions.DORIS_BATCH_SIZE_MAX) {
            batchSize = DorisOptions.DORIS_BATCH_SIZE_MAX;
        }
        int queryDorisTimeout = config.getValue(DorisOptions.DORIS_REQUEST_QUERY_TIMEOUT_S);
        long execMemLimit = config.getValue(DorisOptions.DORIS_EXEC_MEM_LIMIT);

        params.setBatchSize(batchSize);
        params.setQueryTimeout(queryDorisTimeout);
        params.setMemLimit(execMemLimit);
        params.setUser(config.getValue(DorisOptions.DORIS_USER));
        params.setPasswd(config.getValue(DorisOptions.DORIS_PASSWORD));

        logger.debug("Open scan params: cluster: {}, database: {}, table: {}, tabletId: {}, batch size: {}, query timeout: {}, execution memory limit: {}, user: {}, query plan: {}",
                params.getCluster(), params.getDatabase(), params.getTable(), params.getTabletIds(), batchSize, queryDorisTimeout, execMemLimit, params.getUser(), params.getOpaquedQueryPlan());
        return params;
    }

    private <T> T lockClient(Function<DorisBackendThriftClient, T> action) {
        backendLock.lock();
        try {
            return action.apply(backend);
        } finally {
            backendLock.unlock();
        }
    }

    protected abstract Schema getDorisSchema() throws Exception;

    protected Schema processDorisSchema(DorisReaderPartition partition, final Schema originSchema) throws Exception {
        Schema processedSchema = new Schema();
        Schema tableSchema = frontend.getTableSchema(partition.getDatabase(), partition.getTable());
        Map<String, Field> fieldTypeMap = tableSchema.getProperties().stream()
                .collect(Collectors.toMap(Field::getName, Function.identity()));
        Map<String, Field> scanTypeMap = originSchema.getProperties().stream()
                .collect(Collectors.toMap(Field::getName, Function.identity()));
        String[] readColumns = partition.getReadColumns();
        List<Field> newFieldList = new ArrayList<>();
        for (String readColumn : readColumns) {
            if (readColumn.contains(" AS ")) {
                int asIdx = readColumn.indexOf(" AS ");
                String realColumn = readColumn.substring(asIdx + 4).trim().replaceAll("`", "");
                if (fieldTypeMap.containsKey(realColumn) && scanTypeMap.containsKey(realColumn)
                        && ("BITMAP".equalsIgnoreCase(fieldTypeMap.get(realColumn).getType())
                        || "HLL".equalsIgnoreCase(fieldTypeMap.get(realColumn).getType()))) {
                    newFieldList.add(new Field(realColumn, TPrimitiveType.VARCHAR.name(), null, 0, 0, null));
                }
            } else {
                newFieldList.add(scanTypeMap.get(readColumn.trim().replaceAll("`", "")));
            }
        }
        processedSchema.setProperties(newFieldList);
        return processedSchema;
    }

    private static class NoOpLock implements Lock {
        @Override
        public void lock() {}

        @Override
        public void lockInterruptibly() {}

        @Override
        public boolean tryLock() {
            return true;
        }

        @Override
        public boolean tryLock(long time, TimeUnit unit) {
            return true;
        }

        @Override
        public void unlock() {}

        @Override
        public Condition newCondition() {
            throw new UnsupportedOperationException("NoOpLock can't provide a condition");
        }
    }
}