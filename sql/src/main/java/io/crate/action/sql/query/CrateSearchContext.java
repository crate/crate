/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.action.sql.query;

import com.carrotsearch.hppc.ObjectObjectAssociativeContainer;
import com.google.common.base.Optional;
import io.crate.Constants;
import org.apache.lucene.util.Counter;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.cache.recycler.CacheRecycler;
import org.elasticsearch.cache.recycler.PageCacheRecycler;
import org.elasticsearch.common.HasContext;
import org.elasticsearch.common.HasContextAndHeaders;
import org.elasticsearch.common.HasHeaders;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.internal.DefaultSearchContext;
import org.elasticsearch.search.internal.ShardSearchRequest;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

public class CrateSearchContext extends DefaultSearchContext {

    public CrateSearchContext(long id,
                              final long nowInMillis,
                              SearchShardTarget shardTarget,
                              Engine.Searcher engineSearcher,
                              IndexService indexService,
                              final IndexShard indexShard,
                              ScriptService scriptService,
                              CacheRecycler cacheRecycler,
                              PageCacheRecycler pageCacheRecycler,
                              BigArrays bigArrays,
                              Counter timeEstimateCounter,
                              Optional<Scroll> scroll,
                              long keepAlive) {
        super(id, new CrateSearchShardRequest(nowInMillis, scroll, indexShard),
                shardTarget, engineSearcher, indexService,
                indexShard, scriptService, cacheRecycler, pageCacheRecycler,
                bigArrays, timeEstimateCounter);
        if (scroll.isPresent()) {
            scroll(scroll.get());
        }
        keepAlive(keepAlive);
    }

    private static class CrateSearchShardRequest implements ShardSearchRequest {

        private final String[] types = new String[]{Constants.DEFAULT_MAPPING_TYPE};
        private final long nowInMillis;
        private final Scroll scroll;
        private final String index;
        private final int shardId;

        private CrateSearchShardRequest(long nowInMillis, Optional<Scroll> scroll,
                                        IndexShard indexShard) {
            this.nowInMillis = nowInMillis;
            this.scroll = scroll.orNull();
            this.index = indexShard.indexService().index().name();
            this.shardId = indexShard.shardId().id();
        }


        @Override
        public String index() {
            return index;
        }

        @Override
        public int shardId() {
            return shardId;
        }

        @Override
        public String[] types() {
            return types;
        }

        @Override
        public BytesReference source() {
            return null;
        }

        @Override
        public void source(BytesReference source) {
        }

        @Override
        public BytesReference extraSource() {
            return null;
        }

        @Override
        public int numberOfShards() {
            return 0;
        }

        @Override
        public SearchType searchType() {
            return null;
        }

        @Override
        public String[] filteringAliases() {
            return Strings.EMPTY_ARRAY;
        }

        @Override
        public long nowInMillis() {
            return nowInMillis;
        }

        @Override
        public String templateName() {
            return null;
        }

        @Override
        public ScriptService.ScriptType templateType() {
            return null;
        }

        @Override
        public Map<String, Object> templateParams() {
            return null;
        }

        @Override
        public BytesReference templateSource() {
            return null;
        }

        @Override
        public Boolean queryCache() {
            return null;
        }

        @Override
        public Scroll scroll() {
            return scroll;
        }

        @Override
        public boolean useSlowScroll() {
            return false;
        }

        @Override
        public BytesReference cacheKey() throws IOException {
            return null;
        }

        @Override
        public void copyContextAndHeadersFrom(HasContextAndHeaders other) {
        }

        @Override
        public <V> V putInContext(Object key, Object value) {
            return null;
        }

        @Override
        public void putAllInContext(ObjectObjectAssociativeContainer<Object, Object> map) {

        }

        @Override
        public <V> V getFromContext(Object key) {
            return null;
        }

        @Override
        public <V> V getFromContext(Object key, V defaultValue) {
            return null;
        }

        @Override
        public boolean hasInContext(Object key) {
            return false;
        }

        @Override
        public int contextSize() {
            return 0;
        }

        @Override
        public boolean isContextEmpty() {
            return false;
        }

        @Override
        public ImmutableOpenMap<Object, Object> getContext() {
            return null;
        }

        @Override
        public void copyContextFrom(HasContext other) {
        }

        @Override
        public HasHeaders putHeader(String key, Object value) {
            return null;
        }

        @Override
        public <V> V getHeader(String key) {
            return null;
        }

        @Override
        public boolean hasHeader(String key) {
            return false;
        }

        @Override
        public Set<String> getHeaders() {
            return null;
        }

        @Override
        public void copyHeadersFrom(HasHeaders from) {
        }
    }
}
