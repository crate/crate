/*
* Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
* license agreements. See the NOTICE file distributed with this work for
* additional information regarding copyright ownership. Crate licenses
* this file to you under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License. You may
* obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
* WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
* License for the specific language governing permissions and limitations
* under the License.
*
* However, if you have executed another commercial license agreement
* with Crate these terms will supersede the license and you may use the
* software solely pursuant to the terms of the relevant commercial agreement.
*/
package io.crate.operation.join;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.Constants;
import io.crate.breaker.RamAccountingContext;
import io.crate.core.bigarray.IterableBigArray;
import io.crate.core.bigarray.MultiNativeArrayBigArray;
import io.crate.core.bigarray.MultiObjectArrayBigArray;
import io.crate.executor.*;
import io.crate.operation.ProjectorUpstream;
import io.crate.operation.projectors.FlatProjectorChain;
import io.crate.operation.projectors.ProjectionToProjectorVisitor;
import io.crate.operation.projectors.Projector;
import io.crate.operation.projectors.TopN;
import io.crate.planner.node.dql.join.NestedLoopNode;
import io.crate.planner.projection.Projection;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;


public class NestedLoopOperation implements ProjectorUpstream {

    public static final int DEFAULT_PAGE_SIZE = 1024;

    public static interface RowCombinator {

        public Object[] combine(Object[] outer, Object[] inner);
    }

    /**
     * [o, u, t, e, r] + [i, n, n, e, r] = [o, u, t, e, r, i, n, n, e, r]
     */
    static class BothRowCombinator implements RowCombinator {

        protected final int outerNumColumns;
        protected final int innerNumColumns;

        public BothRowCombinator(int outerNumColumns, int innerNumColumns) {
            this.outerNumColumns = outerNumColumns;
            this.innerNumColumns = innerNumColumns;
        }

        public static Object[] combine(Object[] outer, int outerNumColumns, Object[] inner, int innerNumColumns) {
            // TODO: avoid creating new array for each row
            Object[] newRow = new Object[outerNumColumns + innerNumColumns];
            System.arraycopy(outer, 0, newRow, 0, outerNumColumns);
            System.arraycopy(inner, 0, newRow, outerNumColumns, innerNumColumns);
            return newRow;
        }

        @Override
        public Object[] combine(Object[] outer, Object[] inner) {
            return combine(outer, outerNumColumns, inner, innerNumColumns);
        }
    }

    /**
     * [o, u, t, e, r] + [i, n, n, e, r] = [i, n, n, e, r, o, u, t, e, r]
     */
    static class BothRowSwitchingCombinator extends BothRowCombinator {

        public BothRowSwitchingCombinator(int outerNumColumns, int innerNumColumns) {
            super(outerNumColumns, innerNumColumns);
        }

        @Override
        public Object[] combine(Object[] outer, Object[] inner) {
            return combine(inner, innerNumColumns, outer, outerNumColumns);
        }
    }


    /**
     * [o, u, t, e, r] + [] = [o, u, t, e, r]
     */
    private static final RowCombinator OUTER_COMBINATOR = new RowCombinator() {

        @Override
        public Object[] combine(Object[] outer, Object[] inner) {
            return outer;
        }
    };

    /**
     * [] + [i, n, n, e, r] = [i, n, n, e, r]
     */
    private static final RowCombinator INNER_COMBINATOR = new RowCombinator() {

        @Override
        public Object[] combine(Object[] outer, Object[] inner) {
            return inner;
        }
    };

    private final ESLogger logger = Loggers.getLogger(getClass());

    private final int limit;
    private final int offset;
    private final List<Task> outerRelationTasks;
    private final List<Task> innerRelationTasks;

    private final TaskExecutor taskExecutor;
    private final ProjectionToProjectorVisitor projectionToProjectorVisitor;
    private final RamAccountingContext ramAccountingContext;
    private final List<Projection> projections;
    private Projector downstream;

    private RowCombinator rowCombinator;

    /**
     * OPTIMIZATION: always let inner relation be the one with the smaller limit
     * and fewer records
     *
     * @param nestedLoopNode               must have outputTypes set
     * @param executor                     the executor to build and execute child-tasks
     * @param projectionToProjectorVisitor used for building the ProjectorChain
     */
    public NestedLoopOperation(NestedLoopNode nestedLoopNode,
                               List<Task> outerTasks,
                               List<Task> innerTasks,
                               TaskExecutor executor,
                               ProjectionToProjectorVisitor projectionToProjectorVisitor,
                               RamAccountingContext ramAccountingContext) {
        this.limit = nestedLoopNode.limit();
        this.offset = nestedLoopNode.offset();

        this.ramAccountingContext = ramAccountingContext;
        this.projectionToProjectorVisitor = projectionToProjectorVisitor;
        this.projections = nestedLoopNode.projections();
        this.taskExecutor = executor;

        int outerNumColumns = nestedLoopNode.outer().outputTypes().size();
        int innerNumColumns = nestedLoopNode.inner().outputTypes().size();
        if (outerNumColumns > 0 && innerNumColumns > 0) {
            if (nestedLoopNode.leftOuterLoop()) {
                rowCombinator = new BothRowCombinator(outerNumColumns, innerNumColumns);
            } else {
                rowCombinator = new BothRowSwitchingCombinator(outerNumColumns, innerNumColumns);
            }
        } else if (outerNumColumns == 0) {
            rowCombinator = INNER_COMBINATOR;
        } else if (innerNumColumns == 0) {
            rowCombinator = OUTER_COMBINATOR;
        }

        this.outerRelationTasks = outerTasks;
        this.innerRelationTasks = innerTasks;
    }

    private int limit() {
        return this.limit == TopN.NO_LIMIT ? Constants.DEFAULT_SELECT_LIMIT : this.limit;
    }

    private boolean hasLimit() {
        return this.limit != TopN.NO_LIMIT;
    }

    private boolean hasOffset() {
        return this.offset > 0;
    }

    private boolean needsToFetchAllForPaging() {
        return !this.projections.isEmpty();
    }

    private FlatProjectorChain initializeProjectors() {
        FlatProjectorChain projectorChain = new FlatProjectorChain(projections, projectionToProjectorVisitor, ramAccountingContext);
        downstream(projectorChain.firstProjector());
        projectorChain.startProjections();
        return projectorChain;
    }

    private List<ListenableFuture<TaskResult>> executeChildTasks(List<Task> tasks, PageInfo pageInfo) {
        assert !tasks.isEmpty() : "nested loop child tasks are empty";
        if (tasks.size() == 1 &&
                tasks.get(0) instanceof PageableTask) {
            // one pageable task, page it
            PageableTask task = (PageableTask) tasks.get(0);
            if (logger.isTraceEnabled()) {
                logger.trace("fetching page {} from source relation", pageInfo);
            }
            task.start(pageInfo);
            return task.result();
        } else {
            logger.trace("fetching from source relation");
            return this.taskExecutor.execute(tasks, null);
        }
    }

    public ListenableFuture<TaskResult> execute(final Optional<PageInfo> pageInfo) {
        // only optimize if no offset for this
        final boolean canOptimizePaging = pageInfo.isPresent() && !hasOffset() && !needsToFetchAllForPaging();
        if (logger.isTraceEnabled()) {
            String paging = pageInfo.isPresent() ? pageInfo.get().toString() : "none";
            logger.trace("executing NestedLoop. paging: {}, optimized paging: {}", paging, canOptimizePaging);
        }

        if (limit() == 0) {
            // shortcut
            return Futures.immediateFuture(
                    pageInfo.isPresent()
                            ? PageableTaskResult.EMPTY_PAGABLE_RESULT
                            : TaskResult.EMPTY_RESULT);
        }

        FlatProjectorChain projectorChain = initializeProjectors();

        int rowsToProduce = canOptimizePaging ? pageInfo.get().position() + pageInfo.get().size(): limit() + offset;

        // this size is set arbitrarily to a value between 1 and DEFAULT_PAGE_SIZE
        // we arbitrarily assume that the inner relation produces around 10 results
        // and we fetch all that is needed to fetch all others
        int outerPageSize = Math.max(1, Math.min(rowsToProduce / 10, DEFAULT_PAGE_SIZE));
        final PageInfo outerPageInfo = new PageInfo(0, outerPageSize);
        List<ListenableFuture<TaskResult>> outerResults = executeChildTasks(outerRelationTasks, outerPageInfo);

        int innerPageSize = Math.max(1, rowsToProduce/outerPageSize);

        final PageInfo innerPageInfo = new PageInfo(0, innerPageSize);
        List<ListenableFuture<TaskResult>> innerResults = executeChildTasks(innerRelationTasks, innerPageInfo);

        final SettableFuture<JoinContext> joinContextFuture = SettableFuture.create();
        Futures.addCallback(
                Futures.allAsList(
                    ImmutableList.of(
                            outerResults.get(outerResults.size()-1),
                            innerResults.get(innerResults.size()-1)
                    )
                ),
                new FutureCallback<List<TaskResult>>() {
                    @Override
                    public void onSuccess(List<TaskResult> results) {
                        assert results.size() == 2;
                        try {
                            final RelationIterable outerIterable = RelationIterable.forTaskResult(results.get(0), outerPageInfo, false);
                            final RelationIterable innerIterable = RelationIterable.forTaskResult(results.get(1), innerPageInfo, true);

                            final JoinContext joinContext = new JoinContext(
                                    outerIterable,
                                    innerIterable);
                            joinContext.refreshOuterIteratorIfNeeded(); // initialize outer iterator
                            joinContextFuture.set(joinContext);

                            FutureCallback<Void> callback = new FutureCallback<Void>() {
                                private void close() {
                                    try {
                                        joinContext.close();
                                    } catch (IOException e) {
                                        logger.error("error closing CROSS JOIN source relation resources", e);
                                    }

                                }

                                @Override
                                public void onSuccess(@Nullable Void result) {
                                    downstream.upstreamFinished();
                                    if (!canOptimizePaging) {
                                        close();
                                    }
                                }

                                @Override
                                public void onFailure(Throwable t) {
                                    downstream.upstreamFailed(t);
                                    close();
                                }
                            };
                            if (canOptimizePaging) {
                                executeAsync(joinContext, pageInfo, callback);
                            } else {
                                executeAsync(joinContext, Optional.<PageInfo>absent(), callback);
                            }
                        } catch (Throwable t) {
                            logger.error("Error during execution of CROSS JOIN", t);
                            downstream.upstreamFailed(t);
                        }
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        logger.error("Error during resolving the CROSS JOIN source relations", t);
                        downstream.upstreamFailed(t);
                    }
        });
        final SettableFuture<TaskResult> future = SettableFuture.create();
        Futures.addCallback(projectorChain.result(), new FutureCallback<Object[][]>() {
            @Override
            public void onSuccess(final @Nullable Object[][] rows) {
                if (rows == null) {
                    future.setException(new NullPointerException("rows is null"));
                } else {
                    if (canOptimizePaging) {
                        // optimized paging
                        Futures.addCallback(joinContextFuture, new FutureCallback<JoinContext>() {
                            @Override
                            public void onSuccess(@Nullable JoinContext result) {
                                IterableBigArray<Object[]> wrappedRows = new MultiNativeArrayBigArray<Object[]>(0, rows.length, rows);
                                future.set(
                                        new NestedLoopPageableTaskResult(wrappedRows, 0L, pageInfo.get(), result)
                                );
                            }

                            @Override
                            public void onFailure(Throwable t) {
                                logger.error("error waiting for the join for the initial page to finish", t);
                                future.setException(t);
                            }
                        });
                    } else if (pageInfo.isPresent()) {
                        // fake paging after projections
                        IterableBigArray<Object[]> wrappedRows = new MultiNativeArrayBigArray<Object[]>(0, rows.length, rows);
                        future.set(new FetchedRowsPageableTaskResult(wrappedRows, 0L, pageInfo.get()));

                    } else {
                        // no paging
                        future.set(new QueryResult(rows));
                    }
                }
            }

            @Override
            public void onFailure(Throwable t) {
                future.setException(t);
            }
        });
        return future;
    }

    /**
     * executing a join for a single page only.
     * this adds some overhead, as we have handle the offset manually,
     * track the produced rows and compare them against the given limit.
     *
     * @return true if we still need some rows, false if we got all we needed.
     */
    private boolean executeNestedLoopForOptimizedPaging(final JoinContext ctx, int limit) {
        boolean wantMore = true;
        int rowsLeft = limit;
        Object[] outerRow, innerRow;

        Outer:
        while (ctx.outerIterator.hasNext()) {
            outerRow = ctx.outerIterator.next();

            ctx.refreshInnerIteratorIfNeeded();
            while (ctx.innerIterator.hasNext()) {
                innerRow = ctx.innerIterator.next();
                wantMore = downstream.setNextRow(
                        rowCombinator.combine(outerRow, innerRow)
                );
                rowsLeft--;
                wantMore = wantMore && rowsLeft > 0;
                if (!wantMore) {
                    break Outer;
                }
            }
        }
        return wantMore;
    }

    /**
     * executing a join until the upstream projectors have enough or one
     * of the iterables is exhausted.
     *
     * @return true if we still need some rows, false if we got all we needed.
     */
    private boolean executeNestedLoop(final JoinContext ctx) {
        boolean wantMore = true;
        Object[] outerRow, innerRow;

        Outer:
        while (ctx.outerIterator.hasNext()) {
            outerRow = ctx.outerIterator.next();

            ctx.refreshInnerIteratorIfNeeded();
            while (ctx.innerIterator.hasNext()) {
                innerRow = ctx.innerIterator.next();
                wantMore = downstream.setNextRow(
                        rowCombinator.combine(outerRow, innerRow)
                );
                if (!wantMore) {
                    break Outer;
                }
            }
        }
        return wantMore;
    }

    /**
     *
     * @param ctx bearing state needed for joining things
     * @param pageInfo if a pageInfo is present, use the position and size of it
     *                 in combination with query offset and limit to determine
     *                 the number of rows to produce.
     * @param callback called when the execution is done
     */
    private void executeAsync(final JoinContext ctx, final Optional<PageInfo> pageInfo, final FutureCallback<Void> callback) {
        boolean wantMore;
        if (pageInfo.isPresent()) {
            wantMore = executeNestedLoopForOptimizedPaging(ctx, pageInfo.get().position() + pageInfo.get().size());
        } else {
            wantMore = executeNestedLoop(ctx);
        }

        if (!wantMore) {
            // downstream has enough
            callback.onSuccess(null);
            return;
        }

        // get next pages
        if (ctx.innerNeedsToFetchMore()) {
            Futures.addCallback(
                    ctx.innerFetchNextPage(),
                    new FutureCallback<Void>() {
                        @Override
                        public void onSuccess(@Nullable Void result) {
                            ctx.refreshInnerIteratorIfNeeded();
                            executeAsync(
                                    ctx,
                                    pageInfo,
                                    callback
                            );
                        }

                        @Override
                        public void onFailure(Throwable t) {
                            callback.onFailure(t);
                        }
                    }
            );


        } else if (ctx.outerNeedsToFetchMore()) {
            Futures.addCallback(
                    ctx.outerFetchNextPage(),
                    new FutureCallback<Void>() {
                        @Override
                        public void onSuccess(@Nullable Void result) {
                            ctx.refreshOuterIteratorIfNeeded(); // refresh iterator
                            executeAsync(
                                    ctx,
                                    pageInfo,
                                    callback
                            );
                        }

                        @Override
                        public void onFailure(Throwable t) {
                            callback.onFailure(t);
                        }
                    }
            );
        } else {
            // both exhausted and complete
            callback.onSuccess(null);
        }
    }

    @Override
    public void downstream(Projector downstream) {
        this.downstream = downstream;
        this.downstream.registerUpstream(this);
    }

    private class NestedLoopPageableTaskResult extends AbstractBigArrayPageableTaskResult {

        private final JoinContext joinContext;
        private final PageInfo currentPageInfo;

        public NestedLoopPageableTaskResult(IterableBigArray<Object[]> backingArray,
                                            long backingArrayStartIndex,
                                            PageInfo pageInfo,
                                            JoinContext joinContext) {
            super(backingArray, backingArrayStartIndex, pageInfo);
            this.joinContext = joinContext;
            this.currentPageInfo = pageInfo;
        }

        private ListenableFuture<PageableTaskResult> fetchFromSource(final PageInfo pageInfo, final long restSize) {
            final SettableFuture<PageableTaskResult> future = SettableFuture.create();
            final FlatProjectorChain projectorChain = initializeProjectors();

            joinContext.refreshOuterIteratorIfNeeded();
            executeAsync(joinContext, Optional.of(pageInfo), new FutureCallback<Void>() {

                @Override
                public void onSuccess(@Nullable Void result) {
                    downstream.upstreamFinished();
                    Futures.addCallback(projectorChain.result(), new FutureCallback<Object[][]>() {
                        @Override
                        public void onSuccess(@Nullable Object[][] result) {
                            if (result == null) {
                                future.setException(new NullPointerException("NestedLoopTask result page is null"));
                            } else {
                                if (logger.isTraceEnabled()) {
                                    logger.trace("fetched {} new rows from NestedLoop, " +
                                            "use {} already produced rows for page {}.",
                                            result.length, restSize, pageInfo);
                                }
                                IterableBigArray<Object[]> resultArray;
                                long startIdx = 0L;
                                if (restSize > 0) {
                                    if (result.length == 0) {
                                        resultArray = backingArray;
                                        startIdx = backingArrayStartIdx;
                                    } else {
                                        IterableBigArray<Object[]> wrapped = new MultiNativeArrayBigArray<Object[]>(0, result.length, result);
                                        resultArray = new MultiObjectArrayBigArray<>(
                                                backingArrayStartIdx,
                                                restSize + result.length,
                                                backingArray,
                                                wrapped);
                                    }
                                } else {
                                    resultArray = new MultiNativeArrayBigArray<Object[]>(0, result.length, result);
                                }
                                future.set(new NestedLoopPageableTaskResult(resultArray, startIdx, pageInfo, joinContext));
                            }
                        }

                        @Override
                        public void onFailure(Throwable t) {
                            future.setException(t);
                        }
                    });
                }

                @Override
                public void onFailure(Throwable t) {
                    future.setException(t);
                    downstream.upstreamFinished();
                    close();
                }
            });

            return future;
        }

        @Override
        public void close() {
            try {
                joinContext.close();
                super.close();
            } catch (Throwable e) {
                logger.error("error closing NestedLoopPageableTaskResult", e);
            }
        }

        @Override
        public ListenableFuture<PageableTaskResult> fetch(PageInfo pageInfo) {
            // TODO: maybe we don't have to be that strict, but its easier that way
            Preconditions.checkArgument(
                    pageInfo.position() == (this.currentPageInfo.size() + this.currentPageInfo.position()),
                    "NestedLoopTask can only page forward without gaps");

            long restSize = backingArray.size() - backingArrayStartIdx - (this.currentPageInfo.position() + this.currentPageInfo.size());
            if (restSize >= pageInfo.size()) {
                // no need to fetch from source
                logger.trace("already fetched next {} rows for page {}", pageInfo.size(), pageInfo);
                return Futures.<PageableTaskResult>immediateFuture(new NestedLoopPageableTaskResult(
                        backingArray,
                        backingArrayStartIdx + this.currentPageInfo.size(),
                        pageInfo,
                        joinContext));
            } else if (restSize < 0) {
                // exhausted
                if (logger.isTraceEnabled()) {
                    logger.trace("nestedloop is exhausted. Page: {} has 0 rows", pageInfo);
                }
                close();
                return Futures.immediateFuture(PageableTaskResult.EMPTY_PAGABLE_RESULT);
            } else {
                // restSize >= 0
                return fetchFromSource(pageInfo, restSize);
            }
        }
    }
}