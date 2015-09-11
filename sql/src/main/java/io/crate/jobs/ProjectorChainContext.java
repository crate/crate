/*
 * Licensed to CRATE.IO GmbH ("Crate") under one or more contributor
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
import com.google.common.util.concurrent.SettableFuture;
import io.crate.breaker.RamAccountingContext;
import io.crate.operation.projectors.*;
import io.crate.planner.projection.Projection;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;

public class ProjectorChainContext implements ExecutionSubContext, ExecutionState {

    private final static ESLogger LOGGER = Loggers.getLogger(ProjectorChainContext.class);

    private final String name;
    private final AtomicBoolean finished = new AtomicBoolean(false);
    private final SettableFuture<Void> finishedFuture = SettableFuture.create();
    private final RowReceiver rowReceiver;
    private final FlatProjectorChain projectorChain;

    private volatile boolean killed = false;
    private List<ContextCallback> callbacks = new ArrayList<>();

    public ProjectorChainContext(String name,
                                 UUID jobId,
                                 ProjectorFactory projectorFactory,
                                 List<Projection> projections,
                                 RowReceiver rowReceiver,
                                 RamAccountingContext ramAccountingContext) {
        this.name = name;
        ListenableRowReceiver listenableRowReceiver = RowReceivers.listenableRowReceiver(rowReceiver);
        Futures.addCallback(listenableRowReceiver.finishFuture(), new FutureCallback<Void>() {
            @Override
            public void onSuccess(@Nullable Void result) {
                ProjectorChainContext.this.finish(null);
            }

            @Override
            public void onFailure(@Nonnull Throwable t) {
                ProjectorChainContext.this.finish(t);
            }
        });
        projectorChain = FlatProjectorChain.withAttachedDownstream(
                projectorFactory,
                ramAccountingContext,
                projections,
                listenableRowReceiver,
                jobId
        );
        this.rowReceiver = projectorChain.firstProjector();
    }

    @Override
    public void addCallback(ContextCallback contextCallback) {
        callbacks.add(contextCallback);
    }

    @Override
    public void prepare() {
    }

    @Override
    public void start() {
        projectorChain.startProjections(this);
    }

    @Override
    public void close() {
        finish(null);
    }

    @Override
    public void kill(@Nullable Throwable throwable) {
        if (throwable == null) {
            throwable = new CancellationException();
        }
        finish(throwable);
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public boolean isKilled() {
        return killed;
    }

    public RowReceiver rowReceiver() {
        return rowReceiver;
    }

    private void finish(@Nullable Throwable throwable) {
        if (finished.compareAndSet(false, true)) {
            if (killed) {
                for (ContextCallback callback : callbacks) {
                    callback.onKill();
                }
            } else {
                for (ContextCallback callback : callbacks) {
                    callback.onClose(throwable, -1);
                }
            }
            finishedFuture.set(null);
        } else {
            try {
                finishedFuture.get();
            } catch (Throwable e) {
                LOGGER.warn("Error while waiting for already running {} {}", killed ? "kill" : "close", e);
            }
        }
    }
}
