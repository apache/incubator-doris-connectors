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

package org.apache.doris.spark.client.entity;

import org.apache.doris.spark.config.DorisConfig;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

public class DorisReaderPartition implements Serializable {

    private final String database;
    private final String table;
    private final Backend backend;
    private final Long[] tablets;
    private final String opaquedQueryPlan;
    private final String[] readColumns;
    private final String[] filters;
    private final Integer limit;
    private final DorisConfig config;
    private final Boolean datetimeJava8ApiEnabled;

    public DorisReaderPartition(String database, String table, Backend backend, Long[] tablets, String opaquedQueryPlan,
                                String[] readColumns, String[] filters, DorisConfig config, Boolean datetimeJava8ApiEnabled) {
        this.database = database;
        this.table = table;
        this.backend = backend;
        this.tablets = tablets;
        this.opaquedQueryPlan = opaquedQueryPlan;
        this.readColumns = readColumns;
        this.filters = filters;
        this.limit = -1;
        this.config = config;
        this.datetimeJava8ApiEnabled = datetimeJava8ApiEnabled;
    }

    public DorisReaderPartition(String database, String table, Backend backend, Long[] tablets, String opaquedQueryPlan,
                                String[] readColumns, String[] filters, Integer limit, DorisConfig config, Boolean datetimeJava8ApiEnabled) {
        this.database = database;
        this.table = table;
        this.backend = backend;
        this.tablets = tablets;
        this.opaquedQueryPlan = opaquedQueryPlan;
        this.readColumns = readColumns;
        this.filters = filters;
        this.limit = limit;
        this.config = config;
        this.datetimeJava8ApiEnabled = datetimeJava8ApiEnabled;
    }

    // Getters and Setters
    public String getDatabase() {
        return database;
    }

    public String getTable() {
        return table;
    }

    public Backend getBackend() {
        return backend;
    }

    public Long[] getTablets() {
        return tablets;
    }

    public String getOpaquedQueryPlan() {
        return opaquedQueryPlan;
    }

    public DorisConfig getConfig() {
        return config;
    }

    public String[] getReadColumns() {
        return readColumns;
    }

    public String[] getFilters() {
        return filters;
    }

    public Integer getLimit() {
        return limit;
    }

    public Boolean getDateTimeJava8APIEnabled() {
        return datetimeJava8ApiEnabled;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        DorisReaderPartition that = (DorisReaderPartition) o;
        return Objects.equals(database, that.database)
                && Objects.equals(table, that.table)
                && Objects.equals(backend, that.backend)
                && Objects.deepEquals(tablets, that.tablets)
                && Objects.equals(opaquedQueryPlan, that.opaquedQueryPlan)
                && Objects.deepEquals(readColumns, that.readColumns)
                && Objects.deepEquals(filters, that.filters)
                && Objects.equals(limit, that.limit)
                && Objects.equals(config, that.config)
                && Objects.equals(datetimeJava8ApiEnabled, that.datetimeJava8ApiEnabled);
    }

    @Override
    public int hashCode() {
        return Objects.hash(database, table, backend, Arrays.hashCode(tablets), opaquedQueryPlan,
                Arrays.hashCode(readColumns), Arrays.hashCode(filters), limit, config, datetimeJava8ApiEnabled);
    }
}