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

import java.util.Map;
import java.util.Properties;
import java.util.StringJoiner;
import java.util.List;
import java.util.Arrays;

public class CopySQLBuilder {
    private final static String COPY_SYNC = "copy.async";
    private final static String FILE_TYPE = "file.type";
    private final static String FORMAT_KEY = "format";
    private final static String FIELD_DELIMITER_KEY = "column_separator";
    private final static String LINE_DELIMITER_KEY = "line_delimiter";
    private final static String COMPRESSION = "compression";

    private final String fileName;
    private Properties copyIntoProps;
    private String tableIdentifier;
    private String data_type;

    public CopySQLBuilder(String data_type, Properties copyIntoProps, String tableIdentifier, String fileName) {
        this.data_type = data_type;
        this.fileName = fileName;
        this.tableIdentifier = tableIdentifier;
        this.copyIntoProps = copyIntoProps;
    }

    public String buildCopySQL() {
        StringBuilder sb = new StringBuilder();
        sb.append("COPY INTO ").append(tableIdentifier).append(" FROM @~('{").append(String.format(fileName,"}*')")).append(" PROPERTIES (");

        //copy into must be sync
        copyIntoProps.put(COPY_SYNC, false);
        copyIntoProps.put(FILE_TYPE, data_type);
        if (data_type.equals("JSON")) {
            copyIntoProps.put("file.strip_outer_array", "false");
        }
        StringJoiner props = new StringJoiner(",");
        for (Map.Entry<Object, Object> entry : copyIntoProps.entrySet()) {
            // remove format
            if (!String.valueOf(entry.getKey()).equals("format")){
                String key = concatPropPrefix(String.valueOf(entry.getKey()));
                String value = String.valueOf(entry.getValue());
                String prop = String.format("'%s'='%s'", key, value);
                props.add(prop);
            }
        }
        sb.append(props).append(" )");
        return sb.toString();
    }

    static final List<String> PREFIX_LIST =
            Arrays.asList(FIELD_DELIMITER_KEY, LINE_DELIMITER_KEY, COMPRESSION);

    private String concatPropPrefix(String key) {
        if (PREFIX_LIST.contains(key)) {
            return "file." + key;
        }
        if (FORMAT_KEY.equals(key)) {
            return "file.type";
        }
        return key;
    }
}