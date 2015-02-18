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

package io.crate.operation.join.nestedloop;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import io.crate.executor.PageInfo;
import io.crate.executor.TaskResult;
import io.crate.operation.projectors.Projector;

import javax.annotation.Nonnull;
import java.util.concurrent.Executor;

public class UnsortedNestedLoopStrategy extends OneShotNestedLoopStrategy {

    public UnsortedNestedLoopStrategy(NestedLoopOperation nestedLoopOperation,
                                      NestedLoopExecutorService nestedLoopExecutorService) {
        super(nestedLoopOperation, nestedLoopExecutorService);
    }

    @Override
    public String name() {
        return "unsorted nl";
    }

    @Override
    public NestedLoopExecutor executor(JoinContext ctx, Optional<PageInfo> pageInfo, Projector downstream, FutureCallback<Void> callback) {
        return new UnsortedNestedLoopExecutor(ctx, pageInfo, downstream, nestedLoopExecutorService.executor(), callback);
    }

    private static class UnsortedNestedLoopExecutor implements NestedLoopExecutor {

        private final JoinContext ctx;
        private final Optional<PageInfo> globalPageInfo;
        private final Projector downstream;
        private final Executor nestedLoopExecutor;
        private final FutureCallback<Void> finalCallback;
        private final FutureCallback<TaskResult> onInnerPage;
        private final FutureCallback<TaskResult> onOuterPage;

        public UnsortedNestedLoopExecutor(JoinContext ctx,
                                          Optional<PageInfo> globalPageInfo,
                                          Projector downstream,
                                          Executor nestedLoopExecutor,
                                          final FutureCallback<Void> finalCallback) {
            this.ctx = ctx;
            this.globalPageInfo = globalPageInfo;
            this.downstream = downstream;
            this.nestedLoopExecutor = nestedLoopExecutor;
            this.finalCallback = finalCallback;

            onInnerPage = new FutureCallback<TaskResult>() {
                @Override
                public void onSuccess(TaskResult result) {
                    onNewInnerPage(result);
                }

                @Override
                public void onFailure(@Nonnull Throwable t) {
                    finalCallback.onFailure(t);
                }
            };

            onOuterPage = new FutureCallback<TaskResult>() {
                @Override
                public void onSuccess(TaskResult result) {
                    onNewOuterPage(result);
                }

                @Override
                public void onFailure(@Nonnull Throwable t) {
                    finalCallback.onFailure(t);
                }
            };
        }

        private void joinOuterPage() {
            if (ctx.innerIsFinished()) {
                Futures.addCallback(ctx.innerTaskResult.fetch(ctx.advanceInnerPageInfo()), onInnerPage, nestedLoopExecutor);
            }
            for (Object[] outerRow : ctx.outerTaskResult.page()) {
                for (Object[] innerRow : ctx.innerTaskResult.page()) {
                    boolean canContinue = downstream.setNextRow(ctx.combine(outerRow, innerRow));
                    if (!canContinue) {
                        finalCallback.onSuccess(null);
                        return;
                    }
                }
            }
            Futures.addCallback(ctx.outerTaskResult.fetch(ctx.advanceOuterPageInfo()), onOuterPage, nestedLoopExecutor);
        }

        @Override
        public void onNewOuterPage(TaskResult taskResult) {
            ctx.newOuterPage(taskResult);
            if (ctx.outerIsFinished()) {
                finalCallback.onSuccess(null);
            } else {
                joinOuterPage();
            }
        }

        public void onNewInnerPage(TaskResult taskResult) {
            ctx.newInnerPage(taskResult);
            if (ctx.outerIsFinished()) {
                Futures.addCallback(ctx.outerTaskResult.fetch(ctx.advanceOuterPageInfo()), onOuterPage, nestedLoopExecutor);
            } else {
                joinOuterPage();
            }
        }

        @Override
        public void joinInnerPage() {
            joinOuterPage();
        }
    }
}
