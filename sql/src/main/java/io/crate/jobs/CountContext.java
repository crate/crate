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
import com.google.common.util.concurrent.SettableFuture;
import io.crate.analyze.WhereClause;
import io.crate.core.collections.Row1;
import io.crate.operation.RowUpstream;
import io.crate.operation.count.CountOperation;
import io.crate.operation.projectors.RowReceiver;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;

public class CountContext implements RowUpstream, ExecutionSubContext {

    private final CountOperation countOperation;
    private final RowReceiver rowReceiver;
    private final Map<String, List<Integer>> indexShardMap;
    private final WhereClause whereClause;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final SettableFuture<Void> closeFuture = SettableFuture.create();

    private ListenableFuture<Long> countFuture;
    private ContextCallback callback = ContextCallback.NO_OP;
    private volatile boolean killed = false;

    private final static ESLogger LOGGER = Loggers.getLogger(CountContext.class);

    public CountContext(CountOperation countOperation,
                        RowReceiver rowReceiver,
                        Map<String, List<Integer>> indexShardMap,
                        WhereClause whereClause) {
        this.countOperation = countOperation;
        this.rowReceiver = rowReceiver;
        rowReceiver.setUpstream(this);
        this.indexShardMap = indexShardMap;
        this.whereClause = whereClause;
    }

    public void start() {
        try {
            countFuture = countOperation.count(indexShardMap, whereClause);
            Futures.addCallback(countFuture, new FutureCallback<Long>() {
                @Override
                public void onSuccess(@Nullable Long result) {
                    rowReceiver.setNextRow(new Row1(result));
                    rowReceiver.finish();
                    close();
                }

                @Override
                public void onFailure(@Nonnull Throwable t) {
                    rowReceiver.fail(t);
                    close();
                }
            });
        } catch (InterruptedException | IOException e) {
            rowReceiver.fail(e);
            close();
        }
    }

    @Override
    public void addCallback(ContextCallback contextCallback) {
        callback = MultiContextCallback.merge(callback, contextCallback);
    }

    @Override
    public void close() {
        doClose(null);
    }

    @Override
    public void kill(@Nullable Throwable throwable) {
        killed = true;
        if (countFuture != null) {
            countFuture.cancel(true);
        }
        if (throwable == null) {
            throwable = new CancellationException();
        }
        doClose(throwable);
    }

    @Override
    public String name() {
        return "count(*)";
    }

    private void doClose(@Nullable Throwable throwable) {
        if (!closed.getAndSet(true)) {
            if (killed) {
                callback.onKill();
            } else {
                callback.onClose(throwable, -1L);
            }
            closeFuture.set(null);
        } else {
            try {
                closeFuture.get();
            } catch (Throwable e) {
                LOGGER.warn("Error while waiting for already running close {}", e);
            }
        }
    }

    @Override
    public void pause() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void resume(boolean async) {
        throw new UnsupportedOperationException();
    }
}
