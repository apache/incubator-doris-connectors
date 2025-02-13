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

package org.apache.doris.spark.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ListUtils {
    private static final Logger LOG = LoggerFactory.getLogger(ListUtils.class);

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static List<String> getSerializedList(List<Map<Object, Object>> batch,
                                           String lineDelimiter) throws JsonProcessingException {
        List<String> result = new ArrayList<>();
        divideAndSerialize(batch, result, lineDelimiter);
        return result;
    }

    /***
     * recursively splits large collections to normal collection and serializes the collection
     * @param batch
     * @param result
     */
    public static void divideAndSerialize(List<Map<Object, Object>> batch, List<String> result, String lineDelimiter) {

        // if an error occurred in the batch call to getBytes ,average divide the batch
        try {
            String serializedResult = generateSerializedResult(batch, lineDelimiter);
            //the "Requested array size exceeds VM limit" exception occurs when the collection is large
            serializedResult.getBytes("UTF-8");
            result.add(serializedResult);
            return;
        } catch (Throwable error) {
            LOG.error("getBytes error:{} ,average divide the collection", ExceptionUtils.getStackTrace(error));
        }
        for (List<Map<Object, Object>> avgSubCollection : getAvgSubCollections(batch)) {
            divideAndSerialize(avgSubCollection, result, lineDelimiter);
        }
    }

    /***
     * average divide the collection
     * @param values
     * @return
     */
    public static List<List<Map<Object, Object>>> getAvgSubCollections(List<Map<Object, Object>> values) {
        return Lists.partition(values, (values.size() + 1) / 2);
    }

    private static String generateSerializedResult(List<Map<Object, Object>> batch, String lineDelimiter)
            throws JsonProcessingException {

        // when lineDelimiter is null, use strip_outer_array mode, otherwise use json_by_line mode
        if (lineDelimiter == null) {
            return MAPPER.writeValueAsString(batch);
        } else {
            StringBuilder builder = new StringBuilder();
            for (Map<Object, Object> data : batch) {
                builder.append(MAPPER.writeValueAsString(data)).append(lineDelimiter);
            }
            int lastIdx = builder.lastIndexOf(lineDelimiter);
            if (lastIdx != -1) {
                return builder.substring(0, lastIdx);
            }
            return builder.toString();
        }
    }

}
