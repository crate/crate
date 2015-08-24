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

import com.google.common.base.Optional;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.Streamer;
import io.crate.breaker.RamAccountingContext;
import io.crate.operation.PageDownstream;
import io.crate.operation.PageDownstreamFactory;
import io.crate.operation.RowDownstream;
import io.crate.operation.join.NestedLoopOperation;
import io.crate.operation.projectors.FlatProjectorChain;
import io.crate.planner.node.StreamerVisitor;
import io.crate.planner.node.dql.MergePhase;
import io.crate.planner.node.dql.join.NestedLoopPhase;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.threadpool.ThreadPool;

import javax.annotation.Nullable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class NestedLoopContext implements DownstreamExecutionSubContext, ExecutionState {

    private static final ESLogger LOGGER = Loggers.getLogger(NestedLoopContext.class);

    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicBoolean killed = new AtomicBoolean(false);
    private final AtomicInteger activeSubContexts = new AtomicInteger(0);
    private ContextCallback callback = ContextCallback.NO_OP;
    private final PageDownstreamContext leftDownstreamContext;
    private final PageDownstreamContext rightDownstreamContext;

    private final NestedLoopPhase nestedLoopPhase;
    private final RamAccountingContext ramAccountingContext;
    private final PageDownstreamFactory pageDownstreamFactory;
    private final ThreadPool threadPool;
    private final FlatProjectorChain flatProjectorChain;
    private final SettableFuture<Void> closeFuture = SettableFuture.create();

    private final NestedLoopOperation nestedLoopOperation;

    public NestedLoopContext(NestedLoopPhase nestedLoopPhase,
                             RowDownstream downstream,
                             RamAccountingContext ramAccountingContext,
                             PageDownstreamFactory pageDownstreamFactory,
                             ThreadPool threadPool,
                             FlatProjectorChain flatProjectorChain) {
        this.nestedLoopPhase = nestedLoopPhase;
        this.ramAccountingContext = ramAccountingContext;
        this.pageDownstreamFactory = pageDownstreamFactory;
        this.threadPool = threadPool;
        this.flatProjectorChain = flatProjectorChain;

        nestedLoopOperation = new NestedLoopOperation();
        nestedLoopOperation.downstream(downstream);

        // left context
        if (nestedLoopPhase.leftMergePhase() != null) {
            leftDownstreamContext = createPageDownstreamContext(nestedLoopPhase.leftMergePhase());
            leftDownstreamContext.addCallback(new RemoveContextCallback(0));
            activeSubContexts.incrementAndGet();
        } else {
            leftDownstreamContext = null;
        }
        // right context
        if (nestedLoopPhase.rightMergePhase() != null) {
            rightDownstreamContext = createPageDownstreamContext(nestedLoopPhase.rightMergePhase());
            rightDownstreamContext.addCallback(new RemoveContextCallback(1));
            activeSubContexts.incrementAndGet();
        } else {
            rightDownstreamContext = null;
        }

    }


    @Override
    public void addCallback(ContextCallback contextCallback) {
        callback = MultiContextCallback.merge(callback, contextCallback);
    }

    @Override
    public void start() {
        if (flatProjectorChain != null) {
            flatProjectorChain.startProjections(this);
        }
        if (leftDownstreamContext != null) {
            leftDownstreamContext.start();
        }
        if (rightDownstreamContext != null) {
            rightDownstreamContext.start();
        }
    }

    @Override
    public void close() {
        doClose(false);
    }

    @Override
    public void kill() {
        killed.set(true);
        doClose(true);
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
            return leftDownstreamContext;
        }
        return rightDownstreamContext;
    }

    private void doClose(boolean kill) {
        if (!closed.getAndSet(true)) {
            if (activeSubContexts.get() == 0) {
                callContextCallback();
            } else {
                if (leftDownstreamContext != null) {
                    if (kill) {
                        leftDownstreamContext.kill();
                    } else {
                        leftDownstreamContext.close();
                    }
                }
                if (rightDownstreamContext != null) {
                    if (kill) {
                        rightDownstreamContext.kill();
                    } else {
                        rightDownstreamContext.close();
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

    private void callContextCallback() {
        if (activeSubContexts.get() == 0) {
            callback.onClose(null, ramAccountingContext.totalBytes());
            ramAccountingContext.close();
        }
    }

    private PageDownstreamContext createPageDownstreamContext(MergePhase phase) {
        Tuple<PageDownstream, FlatProjectorChain> pageDownstreamProjectorChain =
                pageDownstreamFactory.createMergeNodePageDownstream(
                        phase,
                        nestedLoopOperation,
                        true,
                        ramAccountingContext,
                        Optional.of(threadPool.executor(ThreadPool.Names.SEARCH)));
        Streamer<?>[] streamers = StreamerVisitor.streamerFromOutputs(phase);
        PageDownstreamContext pageDownstreamContext = new PageDownstreamContext(
                phase.name(),
                pageDownstreamProjectorChain.v1(),
                streamers,
                ramAccountingContext,
                phase.numUpstreams(),
                pageDownstreamProjectorChain.v2());

        FlatProjectorChain flatProjectorChain = pageDownstreamProjectorChain.v2();
        if (flatProjectorChain != null) {
            flatProjectorChain.startProjections(pageDownstreamContext);
        }

        return pageDownstreamContext;
    }

    private class RemoveContextCallback implements ContextCallback {

        private final int inputId;

        public RemoveContextCallback(int inputId) {
            this.inputId = inputId;
        }

        @Override
        public void onClose(@Nullable Throwable error, long bytesUsed) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("calling removal listener of subContext {}", inputId);
            }
            activeSubContexts.decrementAndGet();
            callContextCallback();
        }

        @Override
        public void keepAlive() {
            callback.keepAlive();
        }
    }

}
