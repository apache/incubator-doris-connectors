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

import org.apache.doris.spark.cfg.SparkSettings;
import org.apache.spark.SparkConf;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class TestDorisStreamLoad {

    @Test
    public void compressByGZ() throws IOException {
        String content = "1,aa,1\n" +
                "2,aa,2\n" +
                "3,aa,3\n" +
                "4,aa,4\n" +
                "5,aa,5\n" +
                "6,aa,6\n" +
                "7,aa,7\n" +
                "8,aa,8\n" +
                "9,aa,9\n" +
                "10,aa,10\n" +
                "11,aa,11\n" +
                "12,aa,12\n" +
                "13,aa,13\n" +
                "14,aa,14\n" +
                "15,aa,15\n" +
                "16,aa,16\n" +
                "17,aa,17\n" +
                "18,aa,18\n" +
                "19,aa,19\n" +
                "20,aa,20\n" +
                "21,aa,21\n" +
                "22,aa,22\n" +
                "23,aa,23\n" +
                "24,aa,24\n" +
                "25,aa,25\n" +
                "26,aa,26\n" +
                "27,aa,27\n" +
                "28,aa,28\n" +
                "29,aa,29\n" +
                "30,aa,30\n" +
                "31,aa,31\n" +
                "32,aa,32\n" +
                "33,aa,33\n" +
                "34,aa,34\n" +
                "35,aa,35\n" +
                "36,aa,36\n" +
                "37,aa,37\n" +
                "38,aa,38\n" +
                "39,aa,39";
        byte[] compressByte = new DorisStreamLoad(new SparkSettings(new SparkConf().set("doris.table.identifier", "aa.bb"))).compressByGZ(content);

        int contentByteLength = content.getBytes("utf-8").length;
        int compressByteLength = compressByte.length;
        System.out.println(contentByteLength);
        System.out.println(compressByteLength);
        Assert.assertTrue(contentByteLength > compressByteLength);

        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        java.io.ByteArrayInputStream in = new java.io.ByteArrayInputStream(compressByte);
        java.util.zip.GZIPInputStream ungzip = new java.util.zip.GZIPInputStream(in);
        byte[] buffer = new byte[1024];
        int n;
        while ((n = ungzip.read(buffer)) >= 0) out.write(buffer, 0, n);
        byte[] unGzipByte = out.toByteArray();

        String unGzipStr = new String(unGzipByte);
        Assert.assertArrayEquals(unGzipStr.getBytes("utf-8"), content.getBytes("utf-8"));
        Assert.assertEquals(unGzipStr, content);
    }
}
