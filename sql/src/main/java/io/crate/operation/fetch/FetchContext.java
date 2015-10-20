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
import io.crate.analyze.symbol.Reference;
import io.crate.exceptions.TableUnknownException;
import io.crate.jobs.AbstractExecutionSubContext;
import io.crate.metadata.PartitionName;
import io.crate.metadata.Routing;
import io.crate.metadata.TableIdent;
import io.crate.planner.node.fetch.FetchPhase;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndexMissingException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;

public class FetchContext extends AbstractExecutionSubContext {

    private final IntObjectOpenHashMap<Engine.Searcher> searchers = new IntObjectOpenHashMap<>();
    private final IntObjectOpenHashMap<SharedShardContext> shardContexts = new IntObjectOpenHashMap<>();
    private final FetchPhase phase;
    private final String localNodeId;
    private final SharedShardContexts sharedShardContexts;
    private final TreeMap<Integer, TableIdent> tableIdents;
    private final Iterable<? extends Routing> routingIterable;

    public FetchContext(FetchPhase phase,
                        String localNodeId,
                        SharedShardContexts sharedShardContexts,
                        Iterable<? extends Routing> routingIterable) {
        super(phase.executionPhaseId());
        this.phase = phase;
        this.localNodeId = localNodeId;
        this.sharedShardContexts = sharedShardContexts;
        this.tableIdents = new TreeMap<>();
        this.routingIterable = routingIterable;

    }

    public Collection<Collection<Reference>> fetchRefs() {
        return phase.fetchRefs();
    }

    @Override
    public void innerPrepare() {
        HashMap<String, TableIdent> index2TableIdent = new HashMap<>();
        for (Map.Entry<TableIdent, Collection<String>> entry : phase.tableIndices().asMap().entrySet()) {
            for (String indexName : entry.getValue()) {
                index2TableIdent.put(indexName, entry.getKey());
            }
        }

        for (Routing routing : routingIterable) {
            Map<String, Map<String, List<Integer>>> locations = routing.locations();
            if (locations == null) {
                continue;
            }
            for (Map.Entry<String, Map<String, List<Integer>>> entry : locations.entrySet()) {
                String nodeId = entry.getKey();
                if (nodeId.equals(localNodeId)) {
                    Map<String, List<Integer>> indexShards = entry.getValue();
                    for (Map.Entry<String, List<Integer>> indexShardsEntry : indexShards.entrySet()) {
                        String index = indexShardsEntry.getKey();
                        Integer base = phase.bases().get(index);
                        TableIdent ident = index2TableIdent.get(index);
                        assert ident != null;
                        tableIdents.put(base, ident);
                        assert base != null;
                        for (Integer shard : indexShardsEntry.getValue()) {
                            ShardId shardId = new ShardId(index, shard);
                            int readerId = base + shardId.id();
                            SharedShardContext shardContext = sharedShardContexts.createContext(shardId, readerId);
                            shardContexts.put(readerId, shardContext);
                            try {
                                searchers.put(readerId, shardContext.searcher());
                            } catch (IndexMissingException e) {
                                if (!PartitionName.isPartition(index)) {
                                    throw new TableUnknownException(index, e);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    @Override
    protected void innerStart() {
        int c = 0;
        for (Collection<Reference> fetchRef : phase.fetchRefs()) {
            c += fetchRef.size();
        }
        if (c == 0) {
            // no fetch references means there will be no fetch requests
            // this context is only here to allow the collectors to generate docids with the right bases
            // the bases are fetched in the prepare phase therefore this context can be closed
            close();
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
    public TableIdent tableIdent(int readerId) {
        return tableIdents.floorEntry(readerId).getValue();
    }

    @Nonnull
    public Engine.Searcher searcher(int readerId) {
        final Engine.Searcher searcher = searchers.get(readerId);
        if (searcher == null) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH, "Searcher for reader with id %d not found", readerId));
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
