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
import com.google.common.util.concurrent.SettableFuture;
import io.crate.breaker.RamAccountingContext;
import io.crate.operation.join.NestedLoopOperation;
import io.crate.operation.projectors.FlatProjectorChain;
import io.crate.planner.node.dql.join.NestedLoopPhase;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class NestedLoopContext implements DownstreamExecutionSubContext, ExecutionState {

    private static final ESLogger LOGGER = Loggers.getLogger(NestedLoopContext.class);

    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicBoolean killed = new AtomicBoolean(false);
    private final AtomicInteger activeSubContexts = new AtomicInteger(0);
    private ContextCallback callback = ContextCallback.NO_OP;

    private final NestedLoopPhase nestedLoopPhase;
    private final FlatProjectorChain flatProjectorChain;
    private final RamAccountingContext ramAccountingContext;
    @Nullable
    private final PageDownstreamContext leftPageDownstreamContext;
    @Nullable
    private final PageDownstreamContext rightPageDownstreamContext;
    private final SettableFuture<Void> closeFuture = SettableFuture.create();


    public NestedLoopContext(NestedLoopPhase phase,
                             FlatProjectorChain flatProjectorChain,
                             NestedLoopOperation nestedLoopOperation,
                             final RamAccountingContext ramAccountingContext,
                             @Nullable PageDownstreamContext leftPageDownstreamContext,
                             @Nullable PageDownstreamContext rightPageDownstreamContext) {

        nestedLoopPhase = phase;
        this.flatProjectorChain = flatProjectorChain;
        this.ramAccountingContext = ramAccountingContext;
        this.leftPageDownstreamContext = leftPageDownstreamContext;
        this.rightPageDownstreamContext = rightPageDownstreamContext;


        activeSubContexts.incrementAndGet();
        if (leftPageDownstreamContext == null) {
            Futures.addCallback(nestedLoopOperation.leftRowReceiver().finishFuture(), new RemoveContextCallback(0));
        } else {
            leftPageDownstreamContext.addCallback(new RemoveContextCallback(0));
        }

        activeSubContexts.incrementAndGet();
        if (rightPageDownstreamContext == null) {
            Futures.addCallback(nestedLoopOperation.rightRowReceiver().finishFuture(), new RemoveContextCallback(1));
        } else {
            rightPageDownstreamContext.addCallback(new RemoveContextCallback(1));
        }
    }

    @Override
    public void addCallback(ContextCallback contextCallback) {
        callback = MultiContextCallback.merge(callback, contextCallback);
    }

    @Override
    public void prepare() {
        flatProjectorChain.startProjections(this);
    }

    @Override
    public void start() {
        if (leftPageDownstreamContext != null) {
            leftPageDownstreamContext.start();
        }
        if (rightPageDownstreamContext != null) {
            rightPageDownstreamContext.start();
        }
    }

    @Override
    public void close() {
        doClose(null);
    }

    @Override
    public void kill(@Nullable Throwable throwable) {
        killed.set(true);
        if (throwable == null) {
            throwable = new CancellationException();
        }
        doClose(throwable);
    }

    @Override
    public String name() {
        return nestedLoopPhase.name();
    }

    @Override
    public boolean isKilled() {
        return killed.get();
    }

    @Override
    public PageDownstreamContext pageDownstreamContext(byte inputId) {
        assert inputId < 2 : "Only 0 and 1 inputId's supported";
        if (inputId == 0) {
            return leftPageDownstreamContext;
        }
        return rightPageDownstreamContext;
    }

    private void doClose(@Nullable Throwable throwable) {
        if (!closed.getAndSet(true)) {
            if (activeSubContexts.get() == 0) {
                callContextCallback(0);
            } else {
                if (leftPageDownstreamContext != null) {
                    if (killed.get()) {
                        leftPageDownstreamContext.kill(throwable);
                    } else {
                        leftPageDownstreamContext.close();
                    }
                }
                if (rightPageDownstreamContext != null) {
                    if (killed.get()) {
                        rightPageDownstreamContext.kill(throwable);
                    } else {
                        rightPageDownstreamContext.close();
                    }
                }
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

    private boolean callContextCallback(int activeSubContexts) {
        if (activeSubContexts == 0) {
            if (killed.get()) {
                callback.onKill();
            } else {
                callback.onClose(null, ramAccountingContext.totalBytes());
            }
            ramAccountingContext.close();
            return true;
        }
        return false;
    }

    private class RemoveContextCallback implements ContextCallback, FutureCallback<Void> {

        private final int inputId;

        public RemoveContextCallback(int inputId) {
            this.inputId = inputId;
        }

        private void doClose(@Nullable Throwable t) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("calling removal listener of subContext {}", inputId);
            }
            int remainingSubContexts = activeSubContexts.decrementAndGet();
            if (!callContextCallback(remainingSubContexts) && t != null) {
                // kill other sub context
                if (inputId == 0 && rightPageDownstreamContext != null) {
                    rightPageDownstreamContext.kill(t);
                } else if (leftPageDownstreamContext != null) {
                    leftPageDownstreamContext.kill(t);
                }
            }
        }

        @Override
        public void onClose(@Nullable Throwable error, long bytesUsed) {
            doClose(error);
        }

        @Override
        public void onSuccess(@Nullable Void result) {
            doClose(null);
        }

        @Override
        public void onFailure(@Nonnull Throwable t) {
            if (t instanceof CancellationException) {
                onKill();
            } else {
                doClose(t);
            }
        }

        @Override
        public void onKill() {
            if (activeSubContexts.decrementAndGet() == 0) {
                callback.onKill();
                ramAccountingContext.close();
            }
        }

        @Override
        public void keepAlive() {
            callback.keepAlive();
        }
    }

}
