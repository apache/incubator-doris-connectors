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

import com.google.common.annotations.VisibleForTesting;
import org.apache.doris.spark.client.DorisFrontendClient;
import org.apache.doris.spark.client.entity.Backend;
import org.apache.doris.spark.client.entity.DorisReaderPartition;
import org.apache.doris.spark.config.DorisConfig;
import org.apache.doris.spark.config.DorisOptions;
import org.apache.doris.spark.rest.models.Field;
import org.apache.doris.spark.rest.models.QueryPlan;
import org.apache.doris.spark.rest.models.Schema;
import org.apache.doris.spark.util.DorisDialects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ReaderPartitionGenerator {

    private static final Logger LOG = LoggerFactory.getLogger(ReaderPartitionGenerator.class);

    /*
     * for spark 2
     */
    public static DorisReaderPartition[] generatePartitions(DorisConfig config, Boolean datetimeJava8ApiEnabled) throws Exception {
        String[] originReadCols;
        if (config.contains(DorisOptions.DORIS_READ_FIELDS) && !config.getValue(DorisOptions.DORIS_READ_FIELDS).equals("*")) {
            originReadCols = Arrays.stream(config.getValue(DorisOptions.DORIS_READ_FIELDS).split(","))
                    .map(x -> x.replaceAll("`", "")).toArray(String[]::new);
        } else {
            originReadCols = new String[0];
        }
        String[] filters = config.contains(DorisOptions.DORIS_FILTER_QUERY) ?
                config.getValue(DorisOptions.DORIS_FILTER_QUERY).split("\\.") : new String[0];
        return generatePartitions(config, originReadCols, filters, -1, datetimeJava8ApiEnabled);
    }

    /*
     * for spark 3
     */
    public static DorisReaderPartition[] generatePartitions(DorisConfig config,
                                                            String[] fields, String[] filters, Integer limit,
                                                            Boolean datetimeJava8ApiEnabled) throws Exception {
        DorisFrontendClient frontend = new DorisFrontendClient(config);
        String fullTableName = config.getValue(DorisOptions.DORIS_TABLE_IDENTIFIER);
        String[] tableParts = fullTableName.split("\\.");
        String db = tableParts[0].replaceAll("`", "");
        String table = tableParts[1].replaceAll("`", "");
        String[] originReadCols = fields;
        if (fields == null || fields.length == 0) {
            originReadCols = frontend.getTableAllColumns(db, table);
        }
        String[] finalReadColumns = getFinalReadColumns(config, frontend, db, table, originReadCols);
        String finalReadColumnString = String.join(",", finalReadColumns);
        String finalWhereClauseString = filters.length == 0 ? "" : " WHERE " + String.join(" AND ", filters);
        String sql = "SELECT " + finalReadColumnString + " FROM `" + db + "`.`" + table + "`" + finalWhereClauseString;
        LOG.info("get query plan for table " + db + "." + table + ", sql: " + sql);
        QueryPlan queryPlan = frontend.getQueryPlan(db, table, sql);
        Map<String, List<Long>> beToTablets = mappingBeToTablets(queryPlan);
        int maxTabletSize = config.getValue(DorisOptions.DORIS_TABLET_SIZE);
        return distributeTabletsToPartitions(db, table, beToTablets, queryPlan.getOpaqued_query_plan(), maxTabletSize,
                finalReadColumns, filters, config, limit, datetimeJava8ApiEnabled);
    }

    @VisibleForTesting
    static Map<String, List<Long>> mappingBeToTablets(QueryPlan queryPlan) {
        Map<String, List<Long>> beToTablets = new HashMap<>();
        queryPlan.getPartitions().forEach((tabletId, tabletInfos) -> {
            String targetBe = null;
            int tabletCount = Integer.MAX_VALUE;
            for (String backend : tabletInfos.getRoutings()) {
                if (!beToTablets.containsKey(backend)) {
                    beToTablets.put(backend, new ArrayList<>());
                    targetBe = backend;
                    break;
                } else if (beToTablets.get(backend).size() < tabletCount) {
                    targetBe = backend;
                    tabletCount = beToTablets.get(backend).size();
                }
            }
            if (targetBe == null) {
                throw new RuntimeException("mapping tablet to be failed");
            }
            beToTablets.get(targetBe).add(Long.parseLong(tabletId));
        });
        return beToTablets;
    }

    private static DorisReaderPartition[] distributeTabletsToPartitions(String database, String table,
                                                                        Map<String, List<Long>> beToTablets,
                                                                        String opaquedQueryPlan, int maxTabletSize,
                                                                        String[] readColumns, String[] predicates,
                                                                        DorisConfig config, Integer limit,
                                                                        Boolean datetimeJava8ApiEnabled) {
        List<DorisReaderPartition> partitions = new ArrayList<>();
        beToTablets.forEach((backendStr, tabletIds) -> {
            List<Long> distinctTablets = new ArrayList<>(new HashSet<>(tabletIds));
            int offset = 0;
            while (offset < distinctTablets.size()) {
                Long[] tablets = distinctTablets.subList(offset, Math.min(offset + maxTabletSize, distinctTablets.size())).toArray(new Long[0]);
                offset += maxTabletSize;
                partitions.add(new DorisReaderPartition(database, table, new Backend(backendStr), tablets,
                        opaquedQueryPlan, readColumns, predicates, limit, config, datetimeJava8ApiEnabled));
            }
        });
        return partitions.toArray(new DorisReaderPartition[0]);
    }

    protected static String[] getFinalReadColumns(DorisConfig config, DorisFrontendClient frontendClient, String db, String table, String[] readFields) throws Exception {
        Schema tableSchema = frontendClient.getTableSchema(db, table);
        Map<String, String> fieldTypeMap = tableSchema.getProperties().stream().collect(Collectors.toMap(Field::getName, Field::getType));
        Boolean bitmapToString = config.getValue(DorisOptions.DORIS_READ_BITMAP_TO_STRING);
        Boolean bitmapToBase64 = config.getValue(DorisOptions.DORIS_READ_BITMAP_TO_BASE64);
        return Arrays.stream(readFields).filter(fieldTypeMap::containsKey).map(readField -> {
            switch (fieldTypeMap.get(readField).toUpperCase()) {
                case "BITMAP":
                    if (bitmapToBase64) {
                        return String.format("bitmap_to_base64(%1$s) AS %1$s", DorisDialects.quote(readField));
                    } else if (bitmapToString) {
                        return String.format("bitmap_to_string(%1$s) AS %1$s", DorisDialects.quote(readField));
                    } else {
                        return String.format("'Read unsupported' AS %s", DorisDialects.quote(readField));
                    }
                case "HLL":
                    return String.format("'Read unsupported' AS %s", DorisDialects.quote(readField));
                default:
                    return DorisDialects.quote(readField);
            }
        }).toArray(String[]::new);
    }

    @FunctionalInterface
    interface ReadColumnsFunction {
        String[] apply(DorisFrontendClient frontend, String db, String table) throws Exception;
    }

}