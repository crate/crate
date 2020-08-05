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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

import com.carrotsearch.hppc.IntIndexedContainer;
import com.carrotsearch.hppc.IntObjectHashMap;
import com.carrotsearch.hppc.cursors.IntCursor;
import com.carrotsearch.hppc.cursors.IntObjectCursor;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.ShardId;

import io.crate.common.collections.BorrowedItem;
import io.crate.execution.dsl.phases.FetchPhase;
import io.crate.execution.jobs.SharedShardContext;
import io.crate.execution.jobs.SharedShardContexts;
import io.crate.execution.jobs.Task;
import io.crate.metadata.IndexParts;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.metadata.Routing;
import io.crate.metadata.doc.DocTableInfo;

public class FetchTask implements Task {

    private final IntObjectHashMap<Engine.Searcher> searchers = new IntObjectHashMap<>();
    private final IntObjectHashMap<SharedShardContext> shardContexts = new IntObjectHashMap<>();
    private final FetchPhase phase;
    private final String localNodeId;
    private final SharedShardContexts sharedShardContexts;
    private final TreeMap<Integer, RelationName> tableIdents = new TreeMap<>();
    private final Metadata metadata;
    private final Iterable<? extends Routing> routingIterable;
    private final Map<RelationName, Collection<Reference>> toFetch;
    private final UUID jobId;
    private final Function<RelationName, DocTableInfo> getTableInfo;
    private final CompletableFuture<Void> result = new CompletableFuture<>();


    @GuardedBy("jobId")
    private int borrowed = 0;

    @GuardedBy("jobId")
    private Throwable killed;

    public FetchTask(UUID jobId,
                     FetchPhase phase,
                     String localNodeId,
                     SharedShardContexts sharedShardContexts,
                     Metadata metadata,
                     Function<RelationName, DocTableInfo> getTableInfo,
                     Iterable<? extends Routing> routingIterable) {
        this.jobId = jobId;
        this.phase = phase;
        this.localNodeId = localNodeId;
        this.sharedShardContexts = sharedShardContexts;
        this.metadata = metadata;
        this.routingIterable = routingIterable;
        this.toFetch = new HashMap<>(phase.tableIndices().size());
        this.getTableInfo = getTableInfo;
    }

    private void closeSearchers() {
        for (IntObjectCursor<Engine.Searcher> cursor : searchers) {
            cursor.value.close();
        }
        searchers.clear();
    }

    public Map<RelationName, Collection<Reference>> toFetch() {
        return toFetch;
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
        var entry = tableIdents.floorEntry(readerId);
        if (entry == null) {
            throw new IllegalArgumentException("No table has been registered for readerId=" + readerId);
        }
        return entry.getValue();
    }

    @Nonnull
    public DocTableInfo table(int readerId) {
        var relationName = tableIdent(readerId);
        var table = getTableInfo.apply(relationName);
        if (table == null) {
            throw new IllegalStateException("TableInfo missing for relation " + relationName);
        }
        return table;
    }

    @Nonnull
    public BorrowedItem<Engine.Searcher> searcher(int readerId) {
        final Engine.Searcher searcher = searchers.get(readerId);
        if (searcher == null) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH, "Searcher for reader with id %d not found", readerId));
        }
        synchronized (jobId) {
            borrowed++;
            return new BorrowedItem<>(searcher, () -> {
                synchronized (jobId) {
                    borrowed--;
                    if (borrowed == 0 && killed != null) {
                        closeSearchers();
                        result.completeExceptionally(killed);
                    }
                }
            });
        }
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
               ", borrowed=" + borrowed +
               ", done=" + result.isDone() +
               ", searchers=" + searchers.keys() +
               '}';
    }

    @Override
    public CompletableFuture<Void> completionFuture() {
        return result;
    }

    @Override
    public void kill(Throwable throwable) {
        synchronized (jobId) {
            killed = throwable;
            if (borrowed == 0) {
                closeSearchers();
                result.completeExceptionally(throwable);
            }
        }
    }

    public void close() {
        synchronized (jobId) {
            closeSearchers();
            if (killed == null) {
                result.complete(null);
            } else {
                result.completeExceptionally(killed);
            }
        }
    }


    @Override
    public void prepare() throws Exception {
        synchronized (jobId) {
            if (killed != null) {
                result.completeExceptionally(killed);
                return;
            }

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
                    IndexMetadata indexMetadata = metadata.index(indexName);
                    if (indexMetadata == null) {
                        if (IndexParts.isPartitioned(indexName)) {
                            continue;
                        }
                        throw new IndexNotFoundException(indexName);
                    }
                    Index index = indexMetadata.getIndex();
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
    }

    @Override
    public void start() {
        if (searchers.isEmpty() || phase.fetchRefs().isEmpty()) {
            // no fetch references means there will be no fetch requests
            // this context is only here to allow the collectors to generate docids with the right bases
            // the bases are fetched in the prepare phase therefore this context can be closed
            close();
        }
    }

    @Override
    public int id() {
        return phase.phaseId();
    }
}
