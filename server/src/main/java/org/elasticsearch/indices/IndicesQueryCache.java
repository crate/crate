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

import java.io.Closeable;
import java.io.IOException;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.LRUQueryCache;
import org.apache.lucene.search.QueryCache;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.elasticsearch.common.lucene.ShardCoreKeyMap;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;

public class IndicesQueryCache implements QueryCache, Closeable {

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

    private final LRUQueryCache cache;
    private final ShardCoreKeyMap shardKeyMap = new ShardCoreKeyMap();

    public IndicesQueryCache(Settings settings) {
        final ByteSizeValue size = INDICES_CACHE_QUERY_SIZE_SETTING.get(settings);
        final int count = INDICES_CACHE_QUERY_COUNT_SETTING.get(settings);
        LOGGER.debug("using [node] query cache with size [{}] max filter count [{}]",
                size, count);
        if (INDICES_QUERIES_CACHE_ALL_SEGMENTS_SETTING.get(settings)) {
            cache = new LRUQueryCache(count, size.getBytes(), context -> true, 1f);
        } else {
            cache = new LRUQueryCache(count, size.getBytes());
        }
    }

    @Override
    public Weight doCache(Weight weight, QueryCachingPolicy policy) {
        while (weight instanceof CachingWeightWrapper) {
            weight = ((CachingWeightWrapper) weight).in;
        }
        final Weight in = cache.doCache(weight, policy);
        // We wrap the weight to track the readers it sees and map them with
        // the shards they belong to
        return new CachingWeightWrapper(in);
    }

    private class CachingWeightWrapper extends Weight {

        private final Weight in;

        protected CachingWeightWrapper(Weight in) {
            super(in.getQuery());
            this.in = in;
        }

        @Override
        public Explanation explain(LeafReaderContext context, int doc) throws IOException {
            shardKeyMap.add(context.reader());
            return in.explain(context, doc);
        }

        @Override
        public Scorer scorer(LeafReaderContext context) throws IOException {
            shardKeyMap.add(context.reader());
            return in.scorer(context);
        }

        @Override
        public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
            shardKeyMap.add(context.reader());
            return in.scorerSupplier(context);
        }

        @Override
        public BulkScorer bulkScorer(LeafReaderContext context) throws IOException {
            shardKeyMap.add(context.reader());
            return in.bulkScorer(context);
        }

        @Override
        public boolean isCacheable(LeafReaderContext ctx) {
            return in.isCacheable(ctx);
        }
    }

    /** Clear all entries that belong to the given index. */
    public void clearIndex(String index) {
        final Set<Object> coreCacheKeys = shardKeyMap.getCoreKeysForIndex(index);
        for (Object coreKey : coreCacheKeys) {
            cache.clearCoreCacheKey(coreKey);
        }

        // This cache stores two things: filters, and doc id sets. Calling
        // clear only removes the doc id sets, but if we reach the situation
        // that the cache does not contain any DocIdSet anymore, then it
        // probably means that the user wanted to remove everything.
        if (cache.getCacheSize() == 0) {
            cache.clear();
        }
    }

    @Override
    public void close() {
        assert shardKeyMap.size() == 0 : shardKeyMap.size();
        cache.clear();
    }
}
