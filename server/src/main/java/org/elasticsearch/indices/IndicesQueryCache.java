/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.indices;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.QueryCache;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;

import io.crate.lucene.CustomLRUQueryCache;

public final class IndicesQueryCache {

    private static final Logger LOGGER = LogManager.getLogger(IndicesQueryCache.class);

    public static final Setting<ByteSizeValue> INDICES_CACHE_QUERY_SIZE_SETTING =
            Setting.memorySizeSetting("indices.queries.cache.size", "10%", Property.NodeScope);
    // mostly a way to prevent queries from being the main source of memory usage
    // of the cache
    public static final Setting<Integer> INDICES_CACHE_QUERY_COUNT_SETTING =
            Setting.intSetting("indices.queries.cache.count", 10_000, 1, Property.NodeScope);
    // enables caching on all segments instead of only the larger ones, for testing only
    public static final Setting<Boolean> INDICES_QUERIES_CACHE_ALL_SEGMENTS_SETTING =
            Setting.boolSetting("indices.queries.cache.all_segments", false, Property.NodeScope);

    private IndicesQueryCache() {
    }

    public static QueryCache createCache(Settings settings) {
        final ByteSizeValue size = INDICES_CACHE_QUERY_SIZE_SETTING.get(settings);
        final int count = INDICES_CACHE_QUERY_COUNT_SETTING.get(settings);
        LOGGER.debug("using [node] query cache with size [{}] max filter count [{}]",
                size, count);
        if (INDICES_QUERIES_CACHE_ALL_SEGMENTS_SETTING.get(settings)) {
            return new CustomLRUQueryCache(count, size.getBytes(), context -> true, 1f);
        } else {
            return new CustomLRUQueryCache(count, size.getBytes());
        }
    }
}
