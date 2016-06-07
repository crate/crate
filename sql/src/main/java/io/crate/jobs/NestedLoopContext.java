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

import io.crate.concurrent.CompletionListener;
import io.crate.concurrent.CompletionState;
import io.crate.operation.join.NestedLoopOperation;
import io.crate.operation.projectors.FlatProjectorChain;
import io.crate.operation.projectors.ListenableRowReceiver;
import io.crate.planner.node.dql.join.NestedLoopPhase;
import org.elasticsearch.common.logging.ESLogger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class NestedLoopContext extends AbstractExecutionSubContext implements DownstreamExecutionSubContext {

    private final NestedLoopPhase nestedLoopPhase;
    private final FlatProjectorChain flatProjectorChain;

    @Nullable
    private final PageDownstreamContext leftPageDownstreamContext;
    @Nullable
    private final PageDownstreamContext rightPageDownstreamContext;
    private final ListenableRowReceiver leftRowReceiver;
    private final ListenableRowReceiver rightRowReceiver;

    public NestedLoopContext(ESLogger logger,
                             NestedLoopPhase phase,
                             FlatProjectorChain flatProjectorChain,
                             NestedLoopOperation nestedLoopOperation,
                             @Nullable PageDownstreamContext leftPageDownstreamContext,
                             @Nullable PageDownstreamContext rightPageDownstreamContext) {
        super(phase.executionPhaseId(), logger);

        nestedLoopPhase = phase;
        this.flatProjectorChain = flatProjectorChain;
        this.leftPageDownstreamContext = leftPageDownstreamContext;
        this.rightPageDownstreamContext = rightPageDownstreamContext;

        leftRowReceiver = nestedLoopOperation.leftRowReceiver();
        rightRowReceiver = nestedLoopOperation.rightRowReceiver();

        nestedLoopOperation.addListener(new CompletionListener() {
            @Override
            public void onSuccess(@Nullable CompletionState result) {
                future.close(null);
            }

            @Override
            public void onFailure(@Nonnull Throwable t) {
                future.close(t);
            }
        });
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
    protected void innerPrepare() {
        flatProjectorChain.prepare();
    }

    @Override
    protected void innerClose(@Nullable Throwable t) {
        closeReceiver(t, leftPageDownstreamContext, leftRowReceiver);
        closeReceiver(t, rightPageDownstreamContext, rightRowReceiver);
    }

    private static void closeReceiver(@Nullable Throwable t, @Nullable PageDownstreamContext subContext, ListenableRowReceiver rowReceiver) {
        if (subContext == null && t != null) {
            rowReceiver.fail(t);
        }
    }

    @Override
    protected void innerKill(@Nullable Throwable t) {
        killReceiver(t, leftPageDownstreamContext, leftRowReceiver);
        killReceiver(t, rightPageDownstreamContext, rightRowReceiver);
    }

    private static void killReceiver(Throwable t, @Nullable PageDownstreamContext subContext, ListenableRowReceiver rowReceiver) {
        if (subContext == null) {
            rowReceiver.kill(t);
        }
    }

    @Override
    public PageDownstreamContext pageDownstreamContext(byte inputId) {
        assert inputId < 2 : "Only 0 and 1 inputId's supported";
        if (inputId == 0) {
            return leftPageDownstreamContext;
        }
        return rightPageDownstreamContext;
    }

    @Override
    public String toString() {
        return "NestedLoopContext{" +
               "id=" + id() +
               ", leftCtx=" + leftPageDownstreamContext +
               ", rightCtx=" + rightPageDownstreamContext +
               ", closed=" + future.closed() +
               '}';
    }

}
