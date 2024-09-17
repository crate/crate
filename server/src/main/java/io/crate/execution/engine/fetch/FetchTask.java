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

package io.crate.execution.engine.fetch;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import org.apache.lucene.search.IndexSearcher;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IllegalIndexShardStateException;
import org.elasticsearch.index.shard.ShardId;
import org.jetbrains.annotations.NotNull;

import com.carrotsearch.hppc.IntIndexedContainer;
import com.carrotsearch.hppc.IntObjectHashMap;
import com.carrotsearch.hppc.cursors.IntCursor;

import io.crate.common.annotations.GuardedBy;
import io.crate.common.collections.BorrowedItem;
import io.crate.common.collections.RefCountedItem;
import io.crate.common.exceptions.Exceptions;
import io.crate.execution.dsl.phases.FetchPhase;
import io.crate.execution.jobs.SharedShardContext;
import io.crate.execution.jobs.SharedShardContexts;
import io.crate.execution.jobs.Task;
import io.crate.metadata.IndexName;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.metadata.Routing;
import io.crate.metadata.doc.DocTableInfo;

public class FetchTask implements Task {

    private final IntObjectHashMap<RefCountedItem<? extends IndexSearcher>> searchers = new IntObjectHashMap<>();
    private final IntObjectHashMap<SharedShardContext> shardContexts = new IntObjectHashMap<>();
    private final FetchPhase phase;
    private final int memoryLimitInBytes;
    private final String localNodeId;
    private final SharedShardContexts sharedShardContexts;
    private final TreeMap<Integer, RelationName> tableIdents = new TreeMap<>();
    private final Metadata metadata;
    private final List<? extends Routing> routings;
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
                     int memoryLimitInBytes,
                     String localNodeId,
                     SharedShardContexts sharedShardContexts,
                     Metadata metadata,
                     Function<RelationName, DocTableInfo> getTableInfo,
                     List<? extends Routing> routings) {
        this.jobId = jobId;
        this.phase = phase;
        this.memoryLimitInBytes = memoryLimitInBytes;
        this.localNodeId = localNodeId;
        this.sharedShardContexts = sharedShardContexts;
        this.metadata = metadata;
        this.routings = routings;
        this.toFetch = new HashMap<>(phase.tableIndices().size());
        this.getTableInfo = getTableInfo;
    }

    private void closeSearchers() {
        for (var cursor : searchers) {
            cursor.value.close();
        }
        searchers.clear();
    }

    /**
     * From the memory.operation_limit session setting.
     */
    public int memoryLimitInBytes() {
        return memoryLimitInBytes;
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

    @NotNull
    public RelationName tableIdent(int readerId) {
        var entry = tableIdents.floorEntry(readerId);
        if (entry == null) {
            throw new IllegalArgumentException("No table has been registered for readerId=" + readerId);
        }
        return entry.getValue();
    }

    @NotNull
    public DocTableInfo table(int readerId) {
        var relationName = tableIdent(readerId);
        var table = getTableInfo.apply(relationName);
        if (table == null) {
            throw new IllegalStateException("TableInfo missing for relation " + relationName);
        }
        return table;
    }

    @NotNull
    public BorrowedItem<IndexSearcher> searcher(int readerId) {
        synchronized (jobId) {
            if (killed != null) {
                throw Exceptions.toRuntimeException(killed);
            }
            final var searcher = searchers.get(readerId);
            if (searcher == null) {
                throw new IllegalArgumentException(String.format(Locale.ENGLISH, "Searcher for reader with id %d not found", readerId));
            }
            borrowed++;
            return new BorrowedItem<>(searcher.item(), () -> {
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

    @NotNull
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
               ", killed=" + killed +
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
            assert borrowed == 0 : "Close shouldn't be called while searchers are in use";
            closeSearchers();
            if (killed == null) {
                result.complete(null);
            } else {
                result.completeExceptionally(killed);
            }
        }
    }



    @Override
    public CompletableFuture<Void> start() {
        synchronized (jobId) {
            if (killed != null) {
                result.completeExceptionally(killed);
                return null;
            }

            ArrayList<CompletableFuture<Void>> refreshActions = new ArrayList<>(routings.size());
            for (Routing routing : routings) {
                Map<String, Map<String, IntIndexedContainer>> locations = routing.locations();
                Map<String, IntIndexedContainer> indexShards = locations.get(localNodeId);
                try {
                    refreshActions.add(sharedShardContexts.maybeRefreshReaders(metadata, indexShards, phase.bases()));
                } catch (Throwable t) {
                    result.completeExceptionally(t);
                    throw t;
                }
            }
            return CompletableFuture.allOf(refreshActions.toArray(CompletableFuture[]::new))
                .thenApply(ignored -> {
                    synchronized (jobId) {
                        if (killed == null) {
                            setupSearchers();
                        }
                    }
                    return null;
                });
        }
    }

    private void setupSearchers() {
        HashMap<String, RelationName> index2TableIdent = new HashMap<>();
        for (Map.Entry<RelationName, Collection<String>> entry : phase.tableIndices().entrySet()) {
            for (String indexName : entry.getValue()) {
                index2TableIdent.put(indexName, entry.getKey());
            }
        }
        Set<RelationName> tablesWithFetchRefs = new HashSet<>();
        for (Reference reference : phase.fetchRefs()) {
            tablesWithFetchRefs.add(reference.ident().tableIdent());
        }
        String source = "fetch-task: " + jobId.toString() + '-' + phase.phaseId() + '-' + phase.name();
        for (Routing routing : routings) {
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
                    if (IndexName.isPartitioned(indexName)) {
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
                        try {
                            shardContext = sharedShardContexts.createContext(shardId, readerId);
                            shardContexts.put(readerId, shardContext);
                            if (tablesWithFetchRefs.contains(ident)) {
                                searchers.put(readerId, shardContext.acquireSearcher(source));
                            }
                        } catch (IllegalIndexShardStateException | IndexNotFoundException e) {
                            if (!IndexName.isPartitioned(indexName)) {
                                throw e;
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

    public UUID jobId() {
        return jobId;
    }
}
