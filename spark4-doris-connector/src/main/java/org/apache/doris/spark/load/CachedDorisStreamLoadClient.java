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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.doris.spark.cfg.SparkSettings;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * a cached streamload client for each partition
 */
public class CachedDorisStreamLoadClient {
    private static final long cacheExpireTimeout = 30 * 60;
    private static LoadingCache<SparkSettings, DorisStreamLoad> dorisStreamLoadLoadingCache;

    static {
        dorisStreamLoadLoadingCache = CacheBuilder.newBuilder()
                .expireAfterWrite(cacheExpireTimeout, TimeUnit.SECONDS)
                .removalListener(removalNotification -> {
                    //do nothing
                })
                .build(
                        new CacheLoader<SparkSettings, DorisStreamLoad>() {
                            @Override
                            public DorisStreamLoad load(SparkSettings sparkSettings) {
                                return new DorisStreamLoad(sparkSettings);
                            }
                        }
                );
    }

    public static DorisStreamLoad getOrCreate(SparkSettings settings) throws ExecutionException {
        return dorisStreamLoadLoadingCache.get(settings);
    }
}
