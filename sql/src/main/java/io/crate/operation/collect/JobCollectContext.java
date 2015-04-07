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

package io.crate.operation.collect;

import com.google.common.base.Function;
import io.crate.action.sql.query.CrateSearchContext;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.search.internal.SearchContext;

import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class JobCollectContext implements Releasable {

    private final UUID id;
    private final Map<Integer, LuceneDocCollector> activeCollectors = new HashMap<>();
    private final ConcurrentMap<ShardId, List<Integer>> shardsMap = new ConcurrentHashMap<>();
    private final ConcurrentMap<Integer, ShardId> jobContextIdMap = new ConcurrentHashMap<>();
    private final ConcurrentMap<ShardId, Integer> engineSearchersRefCount = new ConcurrentHashMap<>();
    private final Object lock = new Object();
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private static final ESLogger LOGGER = Loggers.getLogger(JobCollectContext.class);

    private volatile long lastAccessTime = -1;
    private volatile long keepAlive;

    public JobCollectContext(UUID id) {
        this.id = id;
    }

    public UUID id() {
        return id;
    }

    public void registerJobContextId(ShardId shardId, int jobContextId) {
        if (jobContextIdMap.putIfAbsent(jobContextId, shardId) == null) {
            List<Integer> oldShardContextIds;
            List<Integer> shardContextIds = new ArrayList<>();
            shardContextIds.add(jobContextId);
            for (;;) {
                oldShardContextIds = shardsMap.putIfAbsent(shardId, shardContextIds);
                if (oldShardContextIds == null) {
                    return;
                }
                shardContextIds = new ArrayList<>();
                shardContextIds.addAll(oldShardContextIds);
                shardContextIds.add(jobContextId);
                if (shardsMap.replace(shardId, oldShardContextIds, shardContextIds)) {
                    return;
                }
            }
        }
    }

    public LuceneDocCollector createCollectorAndContext(IndexShard indexShard,
                                            int jobSearchContextId,
                                            Function<Engine.Searcher, LuceneDocCollector> createCollectorFunction) throws Exception {
        assert shardsMap.containsKey(indexShard.shardId()) : "all jobSearchContextId's must be registered first using registerJobContextId(..)";
        LuceneDocCollector docCollector;
        synchronized (lock) {
            docCollector = activeCollectors.get(jobSearchContextId);
            if (docCollector == null) {
                boolean sharedEngineSearcher = true;
                Engine.Searcher engineSearcher = acquireSearcher(indexShard);
                if (engineSearcher == null) {
                    sharedEngineSearcher = false;
                    engineSearcher = acquireNewSearcher(indexShard);
                    engineSearchersRefCount.put(indexShard.shardId(), 1);
                }
                docCollector = createCollectorFunction.apply(engineSearcher);
                assert docCollector != null; // should be never null, but interface marks it as nullable
                docCollector.searchContext().sharedEngineSearcher(sharedEngineSearcher);
                activeCollectors.put(jobSearchContextId, docCollector);
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("Created doc collector with context {} on shard {} for job {}",
                            jobSearchContextId, indexShard.shardId(), id);
                }
            }
        }
        return docCollector;
    }

    @Nullable
    public LuceneDocCollector findCollector(int jobSearchContextId) {
        return activeCollectors.get(jobSearchContextId);
    }
    public void closeContext(int jobSearchContextId) {
        closeContext(jobSearchContextId, true);
    }

    public void closeContext(int jobSearchContextId, boolean removeFromActive) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Closing context {} on shard {} for job {}",
                    jobSearchContextId, jobContextIdMap.get(jobSearchContextId), id);
        }
        synchronized (lock) {
            LuceneDocCollector docCollector = activeCollectors.get(jobSearchContextId);
            if (docCollector != null) {
                if (docCollector.searchContext().isEngineSearcherShared()) {
                    ShardId shardId = jobContextIdMap.get(jobSearchContextId);
                    Integer refCount = engineSearchersRefCount.get(shardId);
                    assert refCount != null : "refCount should be initialized while creating context";
                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace("[closeContext] Current engine searcher refCount {} for context {} of shard {}",
                                refCount, jobSearchContextId, shardId);
                    }
                    while (!engineSearchersRefCount.replace(shardId, refCount, refCount - 1)) {
                        refCount = engineSearchersRefCount.get(shardId);
                    }
                    if (engineSearchersRefCount.get(shardId) == 0) {
                        docCollector.searchContext().sharedEngineSearcher(false);
                    }
                }
                if (removeFromActive) {
                    activeCollectors.remove(jobSearchContextId);
                }
                docCollector.searchContext().close();
            }
        }
    }

    /**
     * Try to find a {@link CrateSearchContext} for the same shard.
     * If one is found return its {@link Engine.Searcher}, otherwise return null.
     */
    protected Engine.Searcher acquireSearcher(IndexShard indexShard) {
        List<Integer> jobSearchContextIds = shardsMap.get(indexShard.shardId());
        if (jobSearchContextIds != null && jobSearchContextIds.size() > 0) {
            CrateSearchContext searchContext = null;
            Integer jobSearchContextId = null;
            synchronized (lock) {
                Iterator<Integer> it = jobSearchContextIds.iterator();
                while (searchContext == null && it.hasNext()) {
                    jobSearchContextId = it.next();
                    LuceneDocCollector docCollector = activeCollectors.get(jobSearchContextId);
                    if (docCollector != null) {
                        searchContext = docCollector.searchContext();
                    }
                }
            }
            if (searchContext != null) {
                LOGGER.trace("Reusing engine searcher of shard {}", indexShard.shardId());
                Integer refCount = engineSearchersRefCount.get(indexShard.shardId());
                assert refCount != null : "refCount should be initialized while creating context";
                while (!engineSearchersRefCount.replace(indexShard.shardId(), refCount, refCount+1)) {
                    refCount = engineSearchersRefCount.get(indexShard.shardId());
                }
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("[acquireSearcher] Current engine searcher refCount {}:{} for context {} of shard {}",
                            refCount, engineSearchersRefCount.get(indexShard.shardId()), jobSearchContextId, indexShard.shardId());
                }
                searchContext.sharedEngineSearcher(true);
                return searchContext.engineSearcher();
            }
        }
        return null;
    }

    public void acquireContext(SearchContext context) {
        SearchContext.setCurrent(context);
    }

    public void releaseContext(SearchContext context) {
        assert context == SearchContext.current();
        context.clearReleasables(SearchContext.Lifetime.PHASE);
        SearchContext.removeCurrent();
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) { // prevent double release
            synchronized (lock) {
                Iterator<Integer> it = activeCollectors.keySet().iterator();
                while (it.hasNext()) {
                    Integer jobSearchContextId = it.next();
                    closeContext(jobSearchContextId, false);
                    it.remove();
                }
            }
        }
    }

    public void accessed(long accessTime) {
        this.lastAccessTime = accessTime;
    }

    public long lastAccessTime() {
        return this.lastAccessTime;
    }

    public long keepAlive() {
        return this.keepAlive;
    }

    public void keepAlive(long keepAlive) {
        this.keepAlive = keepAlive;
    }

    /**
     * Acquire a new searcher, wrapper method needed for simplified testing
     */
    protected Engine.Searcher acquireNewSearcher(IndexShard indexShard) {
        return EngineSearcher.getSearcherWithRetry(indexShard, null);
    }

}
