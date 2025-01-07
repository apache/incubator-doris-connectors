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
import com.fasterxml.jackson.databind.json.JsonMapper;
import junit.framework.TestCase;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import scala.jdk.CollectionConverters;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

@RunWith(JUnit4.class)
public class DataUtilTest extends TestCase {

    private static final ObjectMapper MAPPER = JsonMapper.builder().build();

    private List<Object> values;

    private StructType schema;

    @Override
    @Before
    public void setUp() throws Exception {
        if (values == null) {
            values = new LinkedList<>();
            values.add(1);
            values.add(null);
            values.add(UTF8String.fromString("abc"));
        }
        schema = new StructType(new StructField[]{
                StructField.apply("c1", DataTypes.IntegerType, true, Metadata.empty()),
                StructField.apply("c2", DataTypes.StringType, true, Metadata.empty()),
                StructField.apply("c3", DataTypes.StringType, true, Metadata.empty())
        });
    }


    @Test
    public void rowToCsvBytes() {

        InternalRow row = InternalRow.apply(CollectionConverters.CollectionHasAsScala(values).asScala().toList());
        byte[] bytes = DataUtil.rowToCsvBytes(row, schema, ",", false);
        Assert.assertArrayEquals("1,\\N,abc".getBytes(StandardCharsets.UTF_8), bytes);
        byte[] bytes1 = DataUtil.rowToCsvBytes(row, schema, ",", true);
        Assert.assertArrayEquals("\"1\",\"\\N\",\"abc\"".getBytes(StandardCharsets.UTF_8), bytes1);
    }

    @Test
    public void rowToJsonBytes() throws JsonProcessingException {
        Map<String, Object> dataMap = new HashMap<>(values.size());
        dataMap.put("c1", 1);
        dataMap.put("c2", null);
        dataMap.put("c3", "abc");
        InternalRow row = InternalRow.apply(CollectionConverters.CollectionHasAsScala(values).asScala().toList() );
        byte[] bytes = DataUtil.rowToJsonBytes(row, schema);
        Assert.assertArrayEquals(MAPPER.writeValueAsBytes(dataMap), bytes);
    }

}