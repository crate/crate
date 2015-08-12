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

package io.crate.operation.fetch;

import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import io.crate.action.job.SharedShardContext;
import io.crate.action.job.SharedShardContexts;
import io.crate.exceptions.TableUnknownException;
import io.crate.jobs.AbstractExecutionSubContext;
import io.crate.metadata.PartitionName;
import io.crate.metadata.Routing;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndexMissingException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class FetchContext extends AbstractExecutionSubContext {

    private final IntObjectOpenHashMap<Engine.Searcher> searchers = new IntObjectOpenHashMap<>();
    private final IntObjectOpenHashMap<SharedShardContext> shardContexts = new IntObjectOpenHashMap<>();
    private final String localNodeId;
    private final SharedShardContexts sharedShardContexts;
    private final Iterable<? extends Routing> routingIterable;

    public FetchContext(int id,
                        String localNodeId,
                        SharedShardContexts sharedShardContexts,
                        Iterable<? extends Routing> routingIterable) {
        super(id);
        this.localNodeId = localNodeId;
        this.sharedShardContexts = sharedShardContexts;
        this.routingIterable = routingIterable;
    }

    @Override
    public void innerPrepare() {
        for (Routing routing : routingIterable) {
            Map<String, Map<String, List<Integer>>> locations = routing.locations();
            if (locations == null) {
                continue;
            }
            int readerId = routing.jobSearchContextIdBase();
            for (Map.Entry<String, Map<String, List<Integer>>> entry : locations.entrySet()) {
                String nodeId = entry.getKey();
                if (nodeId.equals(localNodeId)) {
                    Map<String, List<Integer>> indexShards = entry.getValue();

                    for (Map.Entry<String, List<Integer>> indexShardsEntry : indexShards.entrySet()) {
                        String index = indexShardsEntry.getKey();
                        for (Integer shard : indexShardsEntry.getValue()) {
                            ShardId shardId = new ShardId(index, shard);
                            SharedShardContext shardContext = sharedShardContexts.createContext(shardId, readerId);
                            shardContexts.put(readerId, shardContext);
                            try {
                                searchers.put(readerId, shardContext.searcher());
                            } catch (IndexMissingException e) {
                                if (!PartitionName.isPartition(index)) {
                                    throw new TableUnknownException(index, e);
                                }
                            }
                            readerId++;
                        }
                    }
                } else if (readerId > -1) {
                    for (List<Integer> shards : entry.getValue().values()) {
                        readerId += shards.size();
                    }
                }
            }
        }
    }


    @Override
    protected void innerClose(@Nullable Throwable t) {
        for (IntObjectCursor<Engine.Searcher> cursor : searchers) {
            cursor.value.close();
        }
    }

    @Override
    protected void innerKill(@Nullable Throwable t) {
        innerClose(t);
    }

    @Override
    public String name() {
        return "fetchContext";
    }

    @Nonnull
    public Engine.Searcher searcher(int readerId) {
        final Engine.Searcher searcher = searchers.get(readerId);
        if (searcher == null) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH, "searcher for reader with id %d not found", readerId));
        }
        return searcher;
    }

    @Nonnull
    public IndexService indexService(int readerId) {
        SharedShardContext sharedShardContext = shardContexts.get(readerId);
        if (sharedShardContext == null) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH, "Reader with id %d not found", readerId));
        }
        return sharedShardContext.indexService();
    }

}
