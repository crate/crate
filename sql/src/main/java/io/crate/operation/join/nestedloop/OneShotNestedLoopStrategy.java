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
import io.crate.executor.QueryResult;
import io.crate.executor.TaskResult;
import io.crate.operation.projectors.Projector;
import io.crate.operation.projectors.TopN;

import java.io.IOException;
import java.util.concurrent.Executor;

/**
 * nestedloop execution that produces all rows at once and returns them as
 * one single instance of {@linkplain io.crate.executor.QueryResult}.
 *
 * This is used, when NestedLoop is not paged.
 */
class OneShotNestedLoopStrategy implements NestedLoopStrategy {

    static class OneShotExecutor implements NestedLoopExecutor {
        private final JoinContext ctx;
        private final FutureCallback<Void> callback;
        private final Executor nestedLoopExecutor;

        private final Projector downstream;

        private final FutureCallback<TaskResult> onOuterPage;
        private final FutureCallback<TaskResult> onInnerPage;

        public OneShotExecutor(JoinContext ctx,
                                 Optional<PageInfo> globalPageInfo,
                                 Projector downstream,
                                 Executor nestedLoopExecutor,
                                 FutureCallback<Void> finalCallback) {
            this.ctx = ctx;
            this.callback = finalCallback;
            this.nestedLoopExecutor = nestedLoopExecutor;
            this.downstream = downstream;
            onOuterPage = new FutureCallback<TaskResult>() {
                @Override
                public void onSuccess(TaskResult result) {
                    if (result == null) {
                        callback.onFailure(new NullPointerException("outer relation result result is null"));
                        return;
                    }
                    onNewOuterPage(result);
                }

                @Override
                public void onFailure(Throwable t) {
                    callback.onFailure(t);
                }
            };
            onInnerPage = new FutureCallback<TaskResult>() {
                @Override
                public void onSuccess(TaskResult result) {
                    if (result == null) {
                        callback.onFailure(new NullPointerException("inner relation result result is null"));
                        return;
                    }
                    onNewInnerPage(result);
                }

                @Override
                public void onFailure(Throwable t) {
                    callback.onFailure(t);
                }
            };
        }

        @Override
        public void joinOuterPage() {
            // use current inner task result,
            // fetch another page/taskresult if necessary
            if (ctx.innerIsFinished()) {
                Futures.addCallback(
                        ctx.innerTaskResult.fetch(ctx.resetInnerPageInfo()),
                        onInnerPage,
                        nestedLoopExecutor
                );
            } else {
                onNewInnerPage(ctx.innerTaskResult);
            }
        }

        @Override
        public void onNewOuterPage(TaskResult taskResult) {
            ctx.newOuterPage(taskResult);
            if (ctx.outerIsFinished()) {
                callback.onSuccess(null);
            } else {
                ctx.advanceOuterRow();
                joinOuterPage();
            }
        }

        @Override
        public void onNewInnerPage(TaskResult taskResult) {
            ctx.newInnerPage(taskResult);

            if (taskResult.page().size() == 0) {
                // reached last page of inner relation
                if (ctx.advanceOuterRow()) {
                    // advance to next outer row
                    // and reiterate the inner relation
                    Futures.addCallback(
                            ctx.innerTaskResult.fetch(ctx.resetInnerPageInfo()),
                            onInnerPage,
                            nestedLoopExecutor
                    );
                    return;
                } else {
                    // fetch new outer page
                    // and reiterate the inner relation
                    Futures.addCallback(
                            ctx.outerTaskResult.fetch(ctx.advanceOuterPageInfo()),
                            onOuterPage,
                            nestedLoopExecutor
                    );
                    return;
                }
            }
            joinInnerPage();
        }

        @Override
        public void joinInnerPage() {
            boolean wantMore = true;
            Object[] innerRow;
            while (ctx.innerPageIterator.hasNext()) {
                innerRow = ctx.innerPageIterator.next();
                wantMore = this.downstream.setNextRow(
                        ctx.combine(ctx.outerRow(), innerRow)
                );
                if (!wantMore) {
                    break;
                }
            }
            if (!wantMore) {
                callback.onSuccess(null);
            } else {
                // get the next page from the inner relation
                Futures.addCallback(
                        ctx.innerTaskResult.fetch(ctx.advanceInnerPageInfo()),
                        onInnerPage,
                        nestedLoopExecutor
                );
            }
        }
    }

    protected NestedLoopOperation nestedLoopOperation;
    protected NestedLoopExecutorService nestedLoopExecutorService;

    public OneShotNestedLoopStrategy(NestedLoopOperation nestedLoopOperation,
                                     NestedLoopExecutorService nestedLoopExecutorService) {
        this.nestedLoopOperation = nestedLoopOperation;
        this.nestedLoopExecutorService = nestedLoopExecutorService;
    }

    @Override
    public NestedLoopExecutor executor(JoinContext ctx, Optional<PageInfo> pageInfo, Projector downstream, FutureCallback<Void> callback) {
        return new OneShotExecutor(ctx, pageInfo, downstream, nestedLoopExecutorService.executor(), callback);
    }

    @Override
    public int rowsToProduce(Optional<PageInfo> pageInfo) {
        if (nestedLoopOperation.limit() == TopN.NO_LIMIT) {
            return TopN.NO_LIMIT;
        }
        return nestedLoopOperation.limit() + nestedLoopOperation.offset();
    }

    @Override
    public void onFirstJoin(JoinContext joinContext) {
        // we can close the context as we produced ALL results in one batch
        try {
            joinContext.close();
        } catch (IOException e) {
            nestedLoopOperation.logger.error("error closing joinContext after {} NestedLoop execution", e, name());
        }
    }

    @Override
    public TaskResult produceFirstResult(Object[][] rows, Optional<PageInfo> pageInfo, JoinContext joinContext) {
        return new QueryResult(rows);
    }

    @Override
    public String name() {
        return "one shot";
    }
}
