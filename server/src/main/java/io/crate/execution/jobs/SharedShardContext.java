/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.execution.jobs;

import java.util.function.UnaryOperator;

import org.apache.lucene.search.IndexSearcher;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;

import io.crate.common.annotations.NotThreadSafe;
import io.crate.common.collections.RefCountedItem;


@NotThreadSafe
public class SharedShardContext {

    private final IndexService indexService;
    private final int readerId;
    private final RefCountedItem<Engine.Searcher> searcher;

    SharedShardContext(IndexService indexService,
                       ShardId shardId,
                       int readerId,
                       UnaryOperator<Engine.Searcher> wrapSearcher) {
        this.indexService = indexService;
        IndexShard indexShard = indexService.getShard(shardId.id());
        this.readerId = readerId;
        this.searcher = new RefCountedItem<Engine.Searcher>(
            source -> wrapSearcher.apply(indexShard.acquireSearcher(source)),
            Engine.Searcher::close
        );
    }

    public RefCountedItem<? extends IndexSearcher> acquireSearcher(String source) throws IndexNotFoundException {
        searcher.markAcquired(source);
        return searcher;
    }

    public IndexService indexService() {
        return indexService;
    }

    public int readerId() {
        return readerId;
    }
}
