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

import java.util.HashMap;
import java.util.Map;
import java.util.function.UnaryOperator;

import javax.annotation.concurrent.NotThreadSafe;

import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;

@NotThreadSafe
public class SharedShardContexts {

    private final IndicesService indicesService;
    private final UnaryOperator<Engine.Searcher> wrapSearcher;
    private final Map<ShardId, SharedShardContext> allocatedShards = new HashMap<>();
    private int readerId = 0;

    public SharedShardContexts(IndicesService indicesService, UnaryOperator<Engine.Searcher> wrapSearcher) {
        this.indicesService = indicesService;
        this.wrapSearcher = wrapSearcher;
    }

    public SharedShardContext createContext(ShardId shardId, int readerId) throws IndexNotFoundException {
        assert !allocatedShards.containsKey(shardId) : "shardId shouldn't have been allocated yet";

        IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
        SharedShardContext sharedShardContext = new SharedShardContext(indexService, shardId, readerId, wrapSearcher);
        allocatedShards.put(shardId, sharedShardContext);
        return sharedShardContext;
    }

    public SharedShardContext getOrCreateContext(ShardId shardId) throws IndexNotFoundException {
        SharedShardContext sharedShardContext = allocatedShards.get(shardId);
        if (sharedShardContext == null) {
            IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
            sharedShardContext = new SharedShardContext(indexService, shardId, readerId, wrapSearcher);
            allocatedShards.put(shardId, sharedShardContext);
            readerId++;
        }
        return sharedShardContext;
    }

    @Override
    public String toString() {
        return "SharedShardContexts{" +
               "allocatedShards=" + allocatedShards.keySet() +
               '}';
    }
}
