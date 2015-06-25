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
import io.crate.jobs.ContextCallback;
import io.crate.jobs.ExecutionState;
import io.crate.jobs.ExecutionSubContext;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.search.internal.SearchContext;

import java.util.ArrayList;
import java.util.List;

public class JobQueryShardContext implements ExecutionSubContext {

    private final IndexShard indexShard;
    private final int jobSearchContextId;
    private final boolean keepContextForFetcher;
    private final Function<JobQueryShardContext, LuceneDocCollector> createCollectorFunction;
    private final List<ContextCallback> contextCallbacks = new ArrayList<>(1);
    private LuceneDocCollector docCollector;
    private EngineSearcherDelegate engineSearcherDelegate;
    private Engine.Searcher engineSearcher;
    private CrateSearchContext searchContext;

    public JobQueryShardContext(IndexShard indexShard,
                                int jobSearchContextId,
                                boolean keepContextForFetcher,
                                Function<JobQueryShardContext, LuceneDocCollector> createCollectorFunction) {
        this.indexShard = indexShard;
        this.jobSearchContextId = jobSearchContextId;
        this.keepContextForFetcher = keepContextForFetcher;
        this.createCollectorFunction = createCollectorFunction;
    }

    public void searcher(EngineSearcherDelegate engineSearcherDelegate) {
        this.engineSearcherDelegate = engineSearcherDelegate;
        engineSearcher = engineSearcherDelegate.acquireSearcher();
        docCollector = createCollectorFunction.apply(this);
    }

    public void searchContext(CrateSearchContext searchContext) {
        this.searchContext = searchContext;
    }

    public IndexShard indexShard() {
        return indexShard;
    }

    @Override
    public void addCallback(ContextCallback contextCallback) {
        contextCallbacks.add(contextCallback);
    }

    @Override
    public void start() {
        // shard operation will be started through node collect operation
    }

    @Override
    public void close() {
        if (!keepContextForFetcher) {
            Releasables.close(searchContext);
        }

        // will decrement engine searcher ref count and close if possible, assuming that a searcher
        // was acquired for the fetcher also
        Releasables.close(engineSearcherDelegate);

        for (ContextCallback contextCallback : contextCallbacks) {
            contextCallback.onClose(null, 0);
        }
    }

    @Override
    public void kill() {
        close();
    }

    @Override
    public String name() {
        return "query";
    }

    public CrateSearchContext searchContext() {
        return searchContext;
    }

    public LuceneDocCollector collector() {
        return docCollector;
    }

    public Engine.Searcher engineSearcher() {
        return engineSearcher;
    }

    public int jobSearchContextId() {
        return jobSearchContextId;
    }

    public void acquireContext() {
        SearchContext.setCurrent(searchContext);
    }

    public void releaseContext() {
        assert searchContext == SearchContext.current();
        searchContext.clearReleasables(SearchContext.Lifetime.PHASE);
        SearchContext.removeCurrent();
    }

}
