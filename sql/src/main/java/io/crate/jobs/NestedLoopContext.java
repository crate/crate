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
import io.crate.breaker.RamAccountingContext;
import io.crate.operation.PageDownstream;
import io.crate.operation.PageDownstreamFactory;
import io.crate.operation.RowDownstream;
import io.crate.operation.join.NestedLoopOperation;
import io.crate.operation.projectors.FlatProjectorChain;
import io.crate.planner.node.StreamerVisitor;
import io.crate.planner.node.dql.MergeNode;
import io.crate.planner.node.dql.join.NestedLoopNode;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.threadpool.ThreadPool;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class NestedLoopContext implements DownstreamExecutionSubContext, ExecutionState {

    private static final ESLogger LOGGER = Loggers.getLogger(NestedLoopContext.class);

    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicBoolean killed = new AtomicBoolean(false);
    private final AtomicInteger activeSubContexts = new AtomicInteger(0);
    private final ArrayList<ContextCallback> contextCallbacks = new ArrayList<>(1);
    private final PageDownstreamContext leftDownstreamContext;
    private final PageDownstreamContext rightDownstreamContext;

    private final NestedLoopNode nestedLoopNode;
    private final RamAccountingContext ramAccountingContext;
    private final PageDownstreamFactory pageDownstreamFactory;
    private final ThreadPool threadPool;
    private final StreamerVisitor streamerVisitor;

    private final NestedLoopOperation nestedLoopOperation;

    public NestedLoopContext(NestedLoopNode nestedLoopNode,
                             RowDownstream downstream,
                             RamAccountingContext ramAccountingContext,
                             PageDownstreamFactory pageDownstreamFactory,
                             ThreadPool threadPool,
                             StreamerVisitor streamerVisitor) {
        this.nestedLoopNode = nestedLoopNode;
        this.ramAccountingContext = ramAccountingContext;
        this.pageDownstreamFactory = pageDownstreamFactory;
        this.threadPool = threadPool;
        this.streamerVisitor = streamerVisitor;

        nestedLoopOperation = new NestedLoopOperation();
        nestedLoopOperation.downstream(downstream);

        // left context
        if (nestedLoopNode.leftMergeNode() != null) {
            leftDownstreamContext = createPageDownstreamContext(nestedLoopNode.leftMergeNode());
            leftDownstreamContext.addCallback(new RemoveContextCallback(0));
            activeSubContexts.incrementAndGet();
        } else {
            leftDownstreamContext = null;
        }
        // right context
        if (nestedLoopNode.rightMergeNode() != null) {
            rightDownstreamContext = createPageDownstreamContext(nestedLoopNode.rightMergeNode());
            rightDownstreamContext.addCallback(new RemoveContextCallback(1));
            activeSubContexts.incrementAndGet();
        } else {
            rightDownstreamContext = null;
        }

    }


    @Override
    public void addCallback(ContextCallback contextCallback) {
        contextCallbacks.add(contextCallback);
    }

    @Override
    public void start() {
        if (leftDownstreamContext != null) {
            leftDownstreamContext.start();
        }
        if (rightDownstreamContext != null) {
            rightDownstreamContext.start();
        }
    }

    @Override
    public void close() {
        if (!closed.getAndSet(true)) {
            if (activeSubContexts.get() == 0) {
                callContextCallback();
            } else {
                if (leftDownstreamContext != null) {
                    leftDownstreamContext.close();
                }
                if (rightDownstreamContext != null) {
                    rightDownstreamContext.close();
                }
            }
        }
    }

    @Override
    public void kill() {
        if (!killed.getAndSet(true)) {
            if (activeSubContexts.get() == 0) {
                callContextCallback();
            } else {
                if (leftDownstreamContext != null) {
                    leftDownstreamContext.kill();
                }
                if (rightDownstreamContext != null) {
                    rightDownstreamContext.kill();
                }
            }
        }
    }

    @Override
    public String name() {
        return nestedLoopNode.name();
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

    private void callContextCallback() {
        if (contextCallbacks.isEmpty()) {
            return;
        }
        if (activeSubContexts.get() == 0) {
            for (ContextCallback contextCallback : contextCallbacks) {
                contextCallback.onClose(null, ramAccountingContext.totalBytes());
            }
            ramAccountingContext.close();
        }
    }

    public void interruptIfKilled() {
        if (killed.get()) {
            throw new CancellationException();
        }
    }

    private PageDownstreamContext createPageDownstreamContext(MergeNode node) {
        Tuple<PageDownstream, FlatProjectorChain> pageDownstreamProjectorChain =
                pageDownstreamFactory.createMergeNodePageDownstream(
                        node,
                        nestedLoopOperation,
                        ramAccountingContext,
                        Optional.of(threadPool.executor(ThreadPool.Names.SEARCH)));
        StreamerVisitor.Context streamerContext = streamerVisitor.processPlanNode(node);
        PageDownstreamContext pageDownstreamContext = new PageDownstreamContext(
                node.name(),
                pageDownstreamProjectorChain.v1(),
                streamerContext.inputStreamers(),
                ramAccountingContext,
                node.numUpstreams());

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
    }

}
