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

import com.carrotsearch.hppc.IntObjectHashMap;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import io.crate.action.job.SharedShardContext;
import io.crate.action.job.SharedShardContexts;
import io.crate.jobs.AbstractExecutionSubContext;
import io.crate.metadata.IndexParts;
import io.crate.metadata.Reference;
import io.crate.metadata.Routing;
import io.crate.metadata.TableIdent;
import io.crate.planner.node.fetch.FetchPhase;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.ShardId;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class FetchContext extends AbstractExecutionSubContext {

    private static final Logger LOGGER = Loggers.getLogger(FetchContext.class);
    private final IntObjectHashMap<Engine.Searcher> searchers = new IntObjectHashMap<>();
    private final IntObjectHashMap<SharedShardContext> shardContexts = new IntObjectHashMap<>();
    private final FetchPhase phase;
    private final String localNodeId;
    private final SharedShardContexts sharedShardContexts;
    private final TreeMap<Integer, TableIdent> tableIdents = new TreeMap<>();
    private final MetaData metaData;
    private final Iterable<? extends Routing> routingIterable;
    private final Map<TableIdent, Collection<Reference>> toFetch;
    private final AtomicBoolean isKilled = new AtomicBoolean(false);

    public FetchContext(FetchPhase phase,
                        String localNodeId,
                        SharedShardContexts sharedShardContexts,
                        MetaData metaData,
                        Iterable<? extends Routing> routingIterable) {
        super(phase.phaseId(), LOGGER);
        this.phase = phase;
        this.localNodeId = localNodeId;
        this.sharedShardContexts = sharedShardContexts;
        this.metaData = metaData;
        this.routingIterable = routingIterable;
        this.toFetch = new HashMap<>(phase.tableIndices().size());
    }

    public Map<TableIdent, Collection<Reference>> toFetch() {
        return toFetch;
    }

    AtomicBoolean isKilled() {
        return isKilled;
    }

    @Override
    public void innerPrepare() {
        HashMap<String, TableIdent> index2TableIdent = new HashMap<>();
        for (Map.Entry<TableIdent, Collection<String>> entry : phase.tableIndices().asMap().entrySet()) {
            for (String indexName : entry.getValue()) {
                index2TableIdent.put(indexName, entry.getKey());
            }
        }
        Set<TableIdent> tablesWithFetchRefs = new HashSet<>();
        for (Reference reference : phase.fetchRefs()) {
            tablesWithFetchRefs.add(reference.ident().tableIdent());
        }


        for (Routing routing : routingIterable) {
            Map<String, Map<String, List<Integer>>> locations = routing.locations();
            Map<String, List<Integer>> indexShards = locations.get(localNodeId);
            for (Map.Entry<String, List<Integer>> indexShardsEntry : indexShards.entrySet()) {
                String indexName = indexShardsEntry.getKey();
                Integer base = phase.bases().get(indexName);
                if (base == null) {
                    continue;
                }
                IndexMetaData indexMetaData = metaData.index(indexName);
                if (indexMetaData == null) {
                    if (IndexParts.isPartitioned(indexName)) {
                        continue;
                    }
                    throw new IndexNotFoundException(indexName);
                }
                Index index = indexMetaData.getIndex();
                TableIdent ident = index2TableIdent.get(indexName);
                assert ident != null : "no tableIdent found for index " + indexName;
                tableIdents.put(base, ident);
                toFetch.put(ident, new ArrayList<>());
                for (Integer shard : indexShardsEntry.getValue()) {
                    ShardId shardId = new ShardId(index, shard);
                    int readerId = base + shardId.id();
                    SharedShardContext shardContext = shardContexts.get(readerId);
                    if (shardContext == null) {
                        shardContext = sharedShardContexts.createContext(shardId, readerId);
                        shardContexts.put(readerId, shardContext);
                        if (tablesWithFetchRefs.contains(ident)) {
                            try {
                                searchers.put(readerId, shardContext.acquireSearcher());
                            } catch (IndexNotFoundException e) {
                                if (!IndexParts.isPartitioned(indexName)) {
                                    throw e;
                                }
                            }
                        }
                    }
                }
            }
        }
        for (Reference reference : phase.fetchRefs()) {
            Collection<Reference> references = toFetch.get(reference.ident().tableIdent());
            if (references != null) {
                references.add(reference);
            }
        }
    }

    @Override
    protected void innerStart() {
        if (searchers.isEmpty() || phase.fetchRefs().isEmpty()) {
            // no fetch references means there will be no fetch requests
            // this context is only here to allow the collectors to generate docids with the right bases
            // the bases are fetched in the prepare phase therefore this context can be closed
            close();
        }
    }

    @Override
    protected void innerKill(@Nonnull Throwable t) {
        isKilled.set(true);
    }

    @Override
    public void cleanup() {
        for (IntObjectCursor<Engine.Searcher> cursor : searchers) {
            cursor.value.close();
        }
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

    @Override
    public String toString() {
        return "FetchContext{" +
               "phase=" + phase.phaseId() +
               ", searchers=" + Arrays.toString(searchers.keys) +
               '}';
    }
}
