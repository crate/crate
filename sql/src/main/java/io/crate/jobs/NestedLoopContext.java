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
import io.crate.operation.join.NestedLoopOperation;
import io.crate.operation.projectors.FlatProjectorChain;
import io.crate.operation.projectors.ListenableRowReceiver;
import io.crate.planner.node.dql.join.NestedLoopPhase;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class NestedLoopContext extends AbstractExecutionSubContext implements DownstreamExecutionSubContext, ExecutionState {

    private final AtomicInteger activeSubContexts = new AtomicInteger(0);
    private final NestedLoopPhase nestedLoopPhase;
    private final FlatProjectorChain flatProjectorChain;

    @Nullable
    private final PageDownstreamContext leftPageDownstreamContext;
    @Nullable
    private final PageDownstreamContext rightPageDownstreamContext;
    private final ListenableRowReceiver leftRowReceiver;
    private final ListenableRowReceiver rightRowReceiver;

    private volatile boolean isKilled;
    private final AtomicReference<Throwable> failure = new AtomicReference<>(null);

    public NestedLoopContext(NestedLoopPhase phase,
                             FlatProjectorChain flatProjectorChain,
                             NestedLoopOperation nestedLoopOperation,
                             @Nullable PageDownstreamContext leftPageDownstreamContext,
                             @Nullable PageDownstreamContext rightPageDownstreamContext) {
        super(phase.executionPhaseId());

        nestedLoopPhase = phase;
        this.flatProjectorChain = flatProjectorChain;
        this.leftPageDownstreamContext = leftPageDownstreamContext;
        this.rightPageDownstreamContext = rightPageDownstreamContext;

        leftRowReceiver = nestedLoopOperation.leftRowReceiver();
        rightRowReceiver = nestedLoopOperation.rightRowReceiver();

        if (leftPageDownstreamContext == null) {
            Futures.addCallback(leftRowReceiver.finishFuture(), new RemoveContextCallback());
        } else {
            leftPageDownstreamContext.future.addCallback(new RemoveContextCallback());
        }

        if (rightPageDownstreamContext == null) {
            Futures.addCallback(rightRowReceiver.finishFuture(), new RemoveContextCallback());
        } else {
            rightPageDownstreamContext.future.addCallback(new RemoveContextCallback());
        }
    }

    @Override
    public String name() {
        return nestedLoopPhase.name();
    }

    @Override
    public int id() {
        return nestedLoopPhase.executionPhaseId();
    }

    @Override
    public void keepAliveListener(KeepAliveListener listener) {
    }

    @Override
    protected void innerPrepare() {
        flatProjectorChain.prepare(this);
    }

    @Override
    protected void innerStart() {
        if (leftPageDownstreamContext != null) {
            leftPageDownstreamContext.start();
        }
        if (rightPageDownstreamContext != null) {
            rightPageDownstreamContext.start();
        }
    }

    @Override
    protected void innerClose(@Nullable Throwable t) {
        closeSubContext(t, leftPageDownstreamContext, leftRowReceiver);
        closeSubContext(t, rightPageDownstreamContext, rightRowReceiver);
    }

    private static void closeSubContext(@Nullable Throwable t, @Nullable PageDownstreamContext subContext, ListenableRowReceiver rowReceiver) {
        if (subContext == null) {
            finishOrFail(t, rowReceiver);
        } else {
            subContext.close(t);
        }
    }

    private static void finishOrFail(@Nullable Throwable t, ListenableRowReceiver rowReceiver) {
        if (t == null) {
            rowReceiver.finish();
        } else {
            rowReceiver.fail(t);
        }
    }

    @Override
    protected void innerKill(@Nullable Throwable t) {
        isKilled = true;
        killSubContext(t, leftPageDownstreamContext, leftRowReceiver);
        killSubContext(t, rightPageDownstreamContext, rightRowReceiver);
    }

    private static void killSubContext(Throwable t, @Nullable PageDownstreamContext subContext, ListenableRowReceiver rowReceiver) {
        if (subContext == null) {
            rowReceiver.fail(t);
        } else {
            subContext.kill(t);
        }
    }

    @Override
    public boolean isKilled() {
        return isKilled;
    }

    @Override
    public PageDownstreamContext pageDownstreamContext(byte inputId) {
        assert inputId < 2 : "Only 0 and 1 inputId's supported";
        if (inputId == 0) {
            return leftPageDownstreamContext;
        }
        return rightPageDownstreamContext;
    }

    private class RemoveContextCallback implements FutureCallback<Object> {

        public RemoveContextCallback() {
            activeSubContexts.incrementAndGet();
        }

        @Override
        public void onSuccess(@Nullable Object result) {
            countdown();
        }

        private void countdown() {
            if (activeSubContexts.decrementAndGet() == 0) {
                Throwable t = failure.get();
                if (future.firstClose()) {
                    future.close(t);
                }
            }
        }

        @Override
        public void onFailure(@Nonnull Throwable t) {
            failure.set(t);
            countdown();
        }
    }
}
