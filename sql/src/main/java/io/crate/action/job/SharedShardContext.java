/*
 * Licensed to Crate.IO GmbH ("Crate") under one or more contributor
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

package io.crate.action.job;

import io.crate.operation.collect.EngineSearcher;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.indices.IndicesService;

import java.util.concurrent.atomic.AtomicInteger;

public class SharedShardContext {

    private final static ESLogger LOGGER = Loggers.getLogger(SharedShardContext.class);

    private final Object mutex = new Object();
    private final IndicesService indicesService;
    private final ShardId shardId;
    private final int readerId;

    private volatile RefCountSearcher searcher;
    private IndexService indexService;
    private IndexShard indexShard;

    public SharedShardContext(IndicesService indicesService, ShardId shardId, int readerId) {
        this.indicesService = indicesService;
        this.shardId = shardId;
        this.readerId = readerId;
    }

    public Engine.Searcher searcher() throws IndexMissingException {
        if (searcher != null) {
            searcher.inc();
            return searcher;
        }
        synchronized (mutex) {
            if (searcher == null) {
                searcher = new RefCountSearcher(
                        EngineSearcher.getSearcherWithRetry(indexShard(), "foo", null));
            }
        }
        searcher.inc();
        return searcher;
    }

    public synchronized IndexShard indexShard() {
        if (indexShard == null) {
            indexShard = indexService().shardSafe(shardId.id());
        }
        return indexShard;
    }

    public synchronized IndexService indexService() throws IndexMissingException {
        if (indexService == null) {
            indexService = indicesService.indexServiceSafe(shardId.getIndex());
        }
        return indexService;
    }

    public int readerId() {
        return readerId;
    }

    private static class RefCountSearcher extends Engine.Searcher {

        private final AtomicInteger refs = new AtomicInteger();
        private final Engine.Searcher searcher;

        public RefCountSearcher(Engine.Searcher searcher) {
            super(searcher.source(), searcher.searcher());
            this.searcher = searcher;
        }

        @Override
        public String source() {
            return searcher.source();
        }

        @Override
        public IndexReader reader() {
            return searcher.reader();
        }

        @Override
        public IndexSearcher searcher() {
            return searcher.searcher();
        }

        @Override
        public void close() throws ElasticsearchException {
            int remainingRefs = refs.decrementAndGet();
            LOGGER.trace("Close called on RefCountSearcher; Remaining refs: {}", remainingRefs);
            if (remainingRefs == 0) {
                searcher.close();
            }
        }

        void inc() {
            int newRefs = refs.incrementAndGet();
            LOGGER.trace("Searcher refs increased: {}", newRefs);
        }
    }
}
