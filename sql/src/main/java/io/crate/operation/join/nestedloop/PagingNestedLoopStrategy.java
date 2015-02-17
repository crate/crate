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

import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * true paging for nested loop, only loading a single page at a time
 * into ram for the outer relation.
 *
 * Only possible, when:
 *
 *  * NestedLoopNode has no projections
 *
 * This brave strategy keeps track of the offset and pagelimit itself
 */
class PagingNestedLoopStrategy implements NestedLoopStrategy {

    private final NestedLoopOperation nestedLoopOperation;
    private final NestedLoopExecutorService nestedLoopExecutorService;

    public PagingNestedLoopStrategy(NestedLoopOperation nestedLoopOperation,
                                    NestedLoopExecutorService nestedLoopExecutorService) {
        this.nestedLoopOperation = nestedLoopOperation;
        this.nestedLoopExecutorService = nestedLoopExecutorService;
    }

    @Override
    public int rowsToProduce(Optional<PageInfo> pageInfo) {
        assert pageInfo.isPresent() : "pageInfo is not present for " + name();
        return pageInfo.get().position() + pageInfo.get().size();
    }

    @Override
    public void onFirstJoin(JoinContext joinContext) {
        // do nothing
    }

    @Override
    public String name() {
        return "optimized paging";
    }

    @Override
    public NestedLoopExecutor executor(JoinContext ctx, Optional<PageInfo> pageInfo, Projector downstream, FutureCallback<Void> callback) {
        assert pageInfo.isPresent() : "pageInfo is not present for " + name();
        int rowsToProduce = pageInfo.get().size();
        int rowsToSkip = 0;
        if (ctx.inFirstIteration()) {
            rowsToProduce += pageInfo.get().position(); // only produce pageInfo offset on first page
            rowsToSkip += nestedLoopOperation.offset();
        }
        return new PagingExecutor(ctx, rowsToProduce, rowsToSkip, nestedLoopExecutorService.executor(), downstream, callback);
    }

    static class PagingExecutor implements NestedLoopExecutor {

        private final JoinContext ctx;
        private final FutureCallback<Void> callback;
        private final Executor nestedLoopExecutor;

        /**
         * handle the pagelimit and offset ourselves
         */
        private final AtomicInteger rowsToProduce;
        private final AtomicInteger rowsToSkip;

        private final Projector downstream;

        private final FutureCallback<TaskResult> onOuterPage;
        private final FutureCallback<TaskResult> onInnerPage;

        public PagingExecutor(JoinContext ctx,
                               int rowsToProduce,
                               int rowsToSkip,
                               Executor nestedLoopExecutor,
                               Projector downstream,
                               FutureCallback<Void> finalCallback) {
            this.ctx = ctx;
            this.rowsToProduce = new AtomicInteger(rowsToProduce);
            this.rowsToSkip = new AtomicInteger(rowsToSkip);
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
                if (rowsToSkip.get() > 0) {
                    rowsToSkip.decrementAndGet();
                    continue;
                }
                wantMore = this.downstream.setNextRow(
                        ctx.combine(ctx.outerRow(), innerRow)
                );
                wantMore = wantMore && rowsToProduce.decrementAndGet() > 0;
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
}
