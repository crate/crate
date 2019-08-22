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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.ShardId;

import java.util.concurrent.atomic.AtomicInteger;

class RefCountSearcher extends Engine.Searcher {

    private static final Logger LOGGER = LogManager.getLogger(SharedShardContext.class);

    private final AtomicInteger refs = new AtomicInteger();
    private final ShardId shardId;
    private final Engine.Searcher searcher;
    private final boolean traceEnabled;

    RefCountSearcher(ShardId shardId,
                     Engine.Searcher searcher,
                     IndexSearcher indexSearcher) {
        super(searcher.source(), indexSearcher, () -> {});
        this.shardId = shardId;
        this.searcher = searcher;
        this.traceEnabled = LOGGER.isTraceEnabled();
    }

    @Override
    public IndexReader reader() {
        return searcher.reader();
    }

    @Override
    public void close() throws ElasticsearchException {
        int remainingRefs = refs.decrementAndGet();
        if (traceEnabled) {
            LOGGER.trace("method=close shardId={} remainingRefs={}", shardId, remainingRefs);
        }
        if (remainingRefs == 0) {
            searcher.close();
        }
    }

    void inc() {
        int newRefs = refs.incrementAndGet();
        if (traceEnabled) {
            LOGGER.trace("method=inc shardId={} newRefs={}", shardId, newRefs);
        }
    }
}
