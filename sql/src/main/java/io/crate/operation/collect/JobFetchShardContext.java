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

import io.crate.action.sql.query.CrateSearchContext;
import io.crate.jobs.ContextCallback;
import io.crate.jobs.ExecutionState;
import io.crate.jobs.ExecutionSubContext;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.search.internal.SearchContext;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;

public class JobFetchShardContext implements ExecutionSubContext, ExecutionState {

    private final EngineSearcherDelegate engineSearcherDelegate;
    private final CrateSearchContext searchContext;
    private final List<ContextCallback> contextCallbacks = new ArrayList<>(1);

    private volatile boolean isKilled = false;

    public JobFetchShardContext(EngineSearcherDelegate engineSearcherDelegate,
                                CrateSearchContext searchContext) {
        this.engineSearcherDelegate = engineSearcherDelegate;
        this.searchContext = searchContext;
        // increase ref count of engine searcher
        engineSearcherDelegate.acquireSearcher();
    }

    @Override
    public void addCallback(ContextCallback contextCallback) {
        contextCallbacks.add(contextCallback);
    }

    @Override
    public void start() {

    }

    @Override
    public void close() {
        Releasables.close(searchContext, engineSearcherDelegate);
        for (ContextCallback contextCallback : contextCallbacks) {
            contextCallback.onClose(null, 0);
        }
    }

    @Override
    public void kill() {
        isKilled = true;
        close();
    }

    @Override
    public boolean isKilled() {
        return isKilled;
    }

    public void interruptIfKilled() {
        if (isKilled) {
            throw new CancellationException();
        }
    }

    @Override
    public String name() {
        return "fetch";
    }

    public CrateSearchContext searchContext() {
        return searchContext;
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
