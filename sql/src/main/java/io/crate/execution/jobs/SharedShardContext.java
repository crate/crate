/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.execution.jobs;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.search.profile.query.QueryProfiler;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import java.util.concurrent.atomic.AtomicInteger;

@NotThreadSafe
public class SharedShardContext {

    private static final Logger LOGGER = Loggers.getLogger(SharedShardContext.class);

    private final IndicesService indicesService;
    private final ShardId shardId;
    private final int readerId;
    @Nullable
    private final QueryProfiler profiler;

    private RefCountSearcher searcher;
    private IndexService indexService;
    private IndexShard indexShard;

    SharedShardContext(IndicesService indicesService, ShardId shardId, int readerId, @Nullable QueryProfiler profiler) {
        this.indicesService = indicesService;
        this.shardId = shardId;
        this.readerId = readerId;
        this.profiler = profiler;
    }

    public Engine.Searcher acquireSearcher() throws IndexNotFoundException {
        if (searcher == null) {
            searcher = new RefCountSearcher(indexShard().acquireSearcher("shared-shard-context"), profiler);
        }
        searcher.inc();
        return searcher;
    }

    public IndexShard indexShard() {
        if (indexShard == null) {
            indexShard = indexService().getShard(shardId.id());
        }
        return indexShard;
    }

    public IndexService indexService() throws IndexNotFoundException {
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
        private InstrumentedIndexSearcher indexSearcher;

        RefCountSearcher(Engine.Searcher searcher, @Nullable QueryProfiler profiler) {
            super(searcher.source(), searcher.searcher());
            this.searcher = searcher;
            indexSearcher = new InstrumentedIndexSearcher(searcher, profiler);
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
            return indexSearcher;
        }

        @Override
        public void close() throws ElasticsearchException {
            int remainingRefs = refs.decrementAndGet();
            traceLog(remainingRefs, "Close called on RefCountSearcher; Remaining refs: {}");
            if (remainingRefs == 0) {
                System.out.println();
                searcher.close();
            }
        }

        void inc() {
            int newRefs = refs.incrementAndGet();
            traceLog(newRefs, "Searcher refs increased: {}");
        }

        private void traceLog(int remainingRefs, String msg) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace(msg, remainingRefs);
            }
        }
    }
}
