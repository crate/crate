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
import com.google.common.util.concurrent.SettableFuture;
import io.crate.action.job.SharedShardContext;
import io.crate.action.job.SharedShardContexts;
import io.crate.exceptions.TableUnknownException;
import io.crate.jobs.ContextCallback;
import io.crate.jobs.ExecutionState;
import io.crate.jobs.ExecutionSubContext;
import io.crate.metadata.PartitionName;
import io.crate.metadata.Routing;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndexMissingException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

public class FetchContext implements ExecutionSubContext, ExecutionState {

    private static final ESLogger LOGGER = Loggers.getLogger(FetchContext.class);

    @SuppressWarnings("ThrowableInstanceNeverThrown")
    private static final CancellationException CANCELLATION_EXCEPTION = new CancellationException();

    private final List<ContextCallback> callbacks = new ArrayList<>();
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final SettableFuture<Boolean> closeFuture = SettableFuture.create();
    private final IntObjectOpenHashMap<Engine.Searcher> searchers = new IntObjectOpenHashMap<>();
    private final IntObjectOpenHashMap<SharedShardContext> shardContexts = new IntObjectOpenHashMap<>();
    private final String localNodeId;
    private final SharedShardContexts sharedShardContexts;
    private final Iterable<? extends Routing> routingIterable;
    private volatile boolean killed;

    public FetchContext(String localNodeId,
                        SharedShardContexts sharedShardContexts,
                        Iterable<? extends Routing> routingIterable) {
        this.localNodeId = localNodeId;
        this.sharedShardContexts = sharedShardContexts;
        this.routingIterable = routingIterable;
    }

    @Override
    public void addCallback(ContextCallback contextCallback) {
        callbacks.add(contextCallback);
    }

    @Override
    public void start() {
        try {
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
        } catch (Throwable t) {
            // close all already allocated searchers
            close(t);
            throw t;
        }
    }

    @Override
    public void close() {
        close(null);
    }

    private void close(@Nullable Throwable t) {
        if (closed.compareAndSet(false, true)) {
            LOGGER.trace("Closing FetchContext");
            try {
                for (IntObjectCursor<Engine.Searcher> cursor : searchers) {
                    cursor.value.close();
                }
                for (ContextCallback callback : callbacks) {
                    callback.onClose(t, 0);
                }
            } finally {
                closeFuture.set(true);
            }
        } else {
            try {
                closeFuture.get();
            } catch (ExecutionException | InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public void kill() {
        killed = true;
        close(CANCELLATION_EXCEPTION);
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

    @Override
    public boolean isKilled() {
        return killed;
    }
}
