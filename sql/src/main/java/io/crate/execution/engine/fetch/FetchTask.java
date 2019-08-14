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

package io.crate.execution.engine.fetch;

import com.carrotsearch.hppc.IntIndexedContainer;
import com.carrotsearch.hppc.IntObjectHashMap;
import com.carrotsearch.hppc.cursors.IntCursor;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import io.crate.execution.dsl.phases.FetchPhase;
import io.crate.execution.jobs.AbstractTask;
import io.crate.execution.jobs.SharedShardContext;
import io.crate.execution.jobs.SharedShardContexts;
import io.crate.metadata.IndexParts;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.metadata.Routing;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.ShardId;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

public class FetchTask extends AbstractTask {

    private final IntObjectHashMap<Engine.Searcher> searchers = new IntObjectHashMap<>();
    private final IntObjectHashMap<SharedShardContext> shardContexts = new IntObjectHashMap<>();
    private final FetchPhase phase;
    private final String localNodeId;
    private final SharedShardContexts sharedShardContexts;
    private final TreeMap<Integer, RelationName> tableIdents = new TreeMap<>();
    private final MetaData metaData;
    private final Iterable<? extends Routing> routingIterable;
    private final Map<RelationName, Collection<Reference>> toFetch;
    private final AtomicBoolean isKilled = new AtomicBoolean(false);
    private final UUID jobId;

    public FetchTask(UUID jobId,
                     FetchPhase phase,
                     String localNodeId,
                     SharedShardContexts sharedShardContexts,
                     MetaData metaData,
                     Iterable<? extends Routing> routingIterable) {
        super(phase.phaseId());
        this.jobId = jobId;
        this.phase = phase;
        this.localNodeId = localNodeId;
        this.sharedShardContexts = sharedShardContexts;
        this.metaData = metaData;
        this.routingIterable = routingIterable;
        this.toFetch = new HashMap<>(phase.tableIndices().size());
    }

    public Map<RelationName, Collection<Reference>> toFetch() {
        return toFetch;
    }

    AtomicBoolean isKilled() {
        return isKilled;
    }

    @Override
    public void innerPrepare() {
        HashMap<String, RelationName> index2TableIdent = new HashMap<>();
        for (Map.Entry<RelationName, Collection<String>> entry : phase.tableIndices().asMap().entrySet()) {
            for (String indexName : entry.getValue()) {
                index2TableIdent.put(indexName, entry.getKey());
            }
        }
        Set<RelationName> tablesWithFetchRefs = new HashSet<>();
        for (Reference reference : phase.fetchRefs()) {
            tablesWithFetchRefs.add(reference.ident().tableIdent());
        }

        String source = jobId.toString() + '-' + phase.phaseId() + '-' + phase.name();
        for (Routing routing : routingIterable) {
            Map<String, Map<String, IntIndexedContainer>> locations = routing.locations();
            Map<String, IntIndexedContainer> indexShards = locations.get(localNodeId);
            for (Map.Entry<String, IntIndexedContainer> indexShardsEntry : indexShards.entrySet()) {
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
                RelationName ident = index2TableIdent.get(indexName);
                assert ident != null : "no relationName found for index " + indexName;
                tableIdents.put(base, ident);
                toFetch.put(ident, new ArrayList<>());
                for (IntCursor shard : indexShardsEntry.getValue()) {
                    ShardId shardId = new ShardId(index, shard.value);
                    int readerId = base + shardId.id();
                    SharedShardContext shardContext = shardContexts.get(readerId);
                    if (shardContext == null) {
                        shardContext = sharedShardContexts.createContext(shardId, readerId);
                        shardContexts.put(readerId, shardContext);
                        if (tablesWithFetchRefs.contains(ident)) {
                            try {
                                searchers.put(readerId, shardContext.acquireSearcher(source));
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
        innerClose();
    }

    protected void innerClose() {
        for (IntObjectCursor<Engine.Searcher> cursor : searchers) {
            cursor.value.close();
        }
    }

    @Override
    public void close() {
        super.close();
    }

    @Override
    public String name() {
        return phase.name();
    }

    @Override
    public long bytesUsed() {
        return -1;
    }

    @Nonnull
    public RelationName tableIdent(int readerId) {
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
        return "FetchTask{" +
               "phase=" + phase.phaseId() +
               ", searchers=" + searchers.keys() +
               '}';
    }
}
