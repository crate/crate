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

package io.crate.jobs;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.crate.analyze.WhereClause;
import io.crate.core.collections.Row1;
import io.crate.operation.RowDownstream;
import io.crate.operation.RowDownstreamHandle;
import io.crate.operation.RowUpstream;
import io.crate.operation.count.CountOperation;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class CountContext implements RowUpstream, ExecutionSubContext {

    private final CountOperation countOperation;
    private final Map<String, List<Integer>> indexShardMap;
    private final WhereClause whereClause;
    private final RowDownstreamHandle rowDownstreamHandle;
    private final ArrayList<ContextCallback> callbacks = new ArrayList<>(1);
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private ListenableFuture<Long> countFuture;

    public CountContext(CountOperation countOperation,
                        RowDownstream rowDownstream,
                        Map<String, List<Integer>> indexShardMap,
                        WhereClause whereClause) {
        this.countOperation = countOperation;
        rowDownstreamHandle = rowDownstream.registerUpstream(this);
        this.indexShardMap = indexShardMap;
        this.whereClause = whereClause;
    }

    public void start() {
        try {
            countFuture = countOperation.count(indexShardMap, whereClause);
            Futures.addCallback(countFuture, new FutureCallback<Long>() {
                @Override
                public void onSuccess(@Nullable Long result) {
                    rowDownstreamHandle.setNextRow(new Row1(result));
                    rowDownstreamHandle.finish();
                    close();
                }

                @Override
                public void onFailure(@Nonnull Throwable t) {
                    rowDownstreamHandle.fail(t);
                    close();
                }
            });
        } catch (InterruptedException | IOException e) {
            rowDownstreamHandle.fail(e);
            close();
        }
    }

    @Override
    public void addCallback(ContextCallback contextCallback) {
        callbacks.add(contextCallback);
    }

    @Override
    public void close() {
        if (!closed.getAndSet(true)) {
            for (ContextCallback callback : callbacks) {
                callback.onClose();
            }
        }
    }

    public void kill() {
        if (countFuture != null) {
            countFuture.cancel(true);
        }
        close();
    }
}
