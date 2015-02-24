/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.executor.transport;

import com.carrotsearch.hppc.IntObjectMap;
import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.cache.*;
import com.google.common.util.concurrent.UncheckedExecutionException;
import org.apache.lucene.index.IndexReader;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.internal.SearchContext;

import javax.annotation.Nonnull;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class CollectContextService implements Releasable {

    // TODO: maybe make configurable
    public static final TimeValue EXPIRATION_DEFAULT = new TimeValue(5, TimeUnit.MINUTES);

    private final ReentrantLock lock;
    // TODO: wrap search context map into object and add shardId -> Reader map
    // so we can get a reader for a shard if one is open already
    private final LoadingCache<UUID, IntObjectMap<SearchContext>> contexts;

    public CollectContextService() {
        this.contexts = CacheBuilder.newBuilder().expireAfterAccess(
                EXPIRATION_DEFAULT.millis(),
                TimeUnit.MILLISECONDS
        ).removalListener(new RemovalListener<UUID, IntObjectMap<SearchContext>>() {
            @Override
            public void onRemoval(@Nonnull RemovalNotification<UUID, IntObjectMap<SearchContext>> notification) {
                IntObjectMap<SearchContext> removedMap = notification.getValue();
                if (removedMap != null) {
                    for (ObjectCursor<SearchContext> cursor : removedMap.values()){
                        Releasables.close(cursor.value);
                    }
                }
            }
        }).build(new CacheLoader<UUID, IntObjectMap<SearchContext>>() {
            @Override
            public IntObjectMap<SearchContext> load(@Nonnull UUID key) throws Exception {
                return new IntObjectOpenHashMap<>();
            }
        });
        this.lock = new ReentrantLock();
    }


    @Nullable
    public SearchContext getContext(UUID jobId, int searchContextId) {
        final IntObjectMap<SearchContext> searchContexts;
        try {
            searchContexts = contexts.get(jobId);
        } catch (ExecutionException|UncheckedExecutionException e) {
            throw Throwables.propagate(e);
        }
        return searchContexts.get(searchContextId);
    }

    public SearchContext getOrCreateContext(UUID jobId,
                                            int searchContextId,
                                            Function<IndexReader, SearchContext> createSearchContextFunction) {
        try {
            final IntObjectMap<SearchContext> searchContexts = contexts.get(jobId);
            lock.lock();
            try {
                SearchContext searchContext = searchContexts.get(searchContextId);
                if (searchContext == null) {
                    // TODO: insert shard IndexReader here
                    searchContext = createSearchContextFunction.apply(null);
                    searchContexts.put(searchContextId, searchContext);
                }
                return searchContext;
            } finally {
                lock.unlock();
            }
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void close() throws ElasticsearchException {
        this.contexts.invalidateAll();
    }
}
