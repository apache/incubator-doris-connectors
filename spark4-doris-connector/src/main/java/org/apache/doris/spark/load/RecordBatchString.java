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
import org.apache.doris.spark.exception.DorisException;
import org.apache.doris.spark.exception.IllegalArgumentException;
import org.apache.doris.spark.util.DataUtil;
import org.apache.spark.sql.catalyst.InternalRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;

/**
 * InputStream for batch load
 */
public class RecordBatchString {

    public static final Logger LOG = LoggerFactory.getLogger(RecordBatchString.class);

    /**
     * Load record batch
     */
    private final RecordBatch recordBatch;

    private final byte[] delim;

    /**
     * record count has been read
     */
    private int readCount = 0;

    /**
     * streaming mode pass through data without process
     */
    private final boolean passThrough;

    public RecordBatchString(RecordBatch recordBatch, boolean passThrough) {
        this.recordBatch = recordBatch;
        this.passThrough = passThrough;
        this.delim = recordBatch.getDelim();
    }

    public String getContent() throws IOException {
        String delimStr = new String(this.recordBatch.getDelim());
        StringBuilder builder = new StringBuilder();
        Iterator<InternalRow> iterator = recordBatch.getIterator();
        while (iterator.hasNext()) {
            try {
                builder.append(rowToString(iterator.next()));
            } catch (DorisException e) {
                throw new IOException(e);
            }
            builder.append(delimStr);
        }
        return builder.toString().substring(0, builder.length()-delimStr.length());
    }


    /**
     * Convert Spark row data to string
     *
     * @param row row data
     * @return byte array
     * @throws DorisException
     */
    private String rowToString(InternalRow row) throws DorisException {

        String str;

        if (passThrough) {
            str = row.getString(0);
            return str;
        }

        switch (recordBatch.getFormat()) {
            case CSV:
                str = DataUtil.rowToCsvString(row, recordBatch.getSchema(), recordBatch.getSep(), recordBatch.getAddDoubleQuotes());
                break;
            case JSON:
                try {
                    str = DataUtil.rowToJsonString(row, recordBatch.getSchema());
                } catch (JsonProcessingException e) {
                    throw new DorisException("parse row to json bytes failed", e);
                }
                break;
            default:
                throw new IllegalArgumentException("Unsupported format: ", recordBatch.getFormat().toString());
        }

        return str;

    }
}
