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

import org.apache.lucene.search.IndexSearcher;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.function.UnaryOperator;

@NotThreadSafe
public class SharedShardContext {

    private final IndicesService indicesService;
    private final ShardId shardId;
    private final int readerId;
    private final UnaryOperator<IndexSearcher> wrapSearcher;

    private RefCountSearcher searcher;
    private IndexService indexService;
    private IndexShard indexShard;

    SharedShardContext(IndicesService indicesService,
                       ShardId shardId,
                       int readerId,
                       UnaryOperator<IndexSearcher> wrapSearcher) {
        this.indicesService = indicesService;
        this.shardId = shardId;
        this.readerId = readerId;
        this.wrapSearcher = wrapSearcher;
    }

    public Engine.Searcher acquireSearcher(String source) throws IndexNotFoundException {
        if (searcher == null) {
            Engine.Searcher searcher = indexShard().acquireSearcher(source);
            this.searcher = new RefCountSearcher(shardId, searcher, wrapSearcher.apply(searcher.searcher()));
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
}
