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
package io.crate.operation.join.nestedloop;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.Constants;
import io.crate.breaker.RamAccountingContext;
import io.crate.core.bigarray.MultiNativeArrayBigArray;
import io.crate.executor.*;
import io.crate.executor.transport.AbstractNonCachingPageableTaskResult;
import io.crate.operation.ProjectorUpstream;
import io.crate.operation.projectors.FlatProjectorChain;
import io.crate.operation.projectors.ProjectionToProjectorVisitor;
import io.crate.operation.projectors.Projector;
import io.crate.operation.projectors.TopN;
import io.crate.planner.PlanPrinter;
import io.crate.planner.node.dql.join.NestedLoopNode;
import io.crate.planner.projection.Projection;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.util.ObjectArray;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;


public class NestedLoopOperation implements ProjectorUpstream {

    public static final int MAX_PAGE_SIZE = 10000;

    public static final int MIN_PAGE_SIZE = 1000;

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

    final ESLogger logger = Loggers.getLogger(getClass());

    private final NestedLoopNode nestedLoopNode;
    private final NestedLoopExecutorService nestedLoopExecutorService;

    private final int limit;
    private final int offset;
    private final List<Task> outerRelationTasks;
    private final List<Task> innerRelationTasks;

    private final TaskExecutor taskExecutor;
    private final ProjectionToProjectorVisitor projectionToProjectorVisitor;
    private final RamAccountingContext ramAccountingContext;
    private final List<Projection> projections;
    private final RowCombinator rowCombinator;

    private Projector downstream;
    private NestedLoopStrategy strategy;

    /**
     * OPTIMIZATION: always let inner relation be the one with the smaller limit
     * and fewer records
     *
     * @param nestedLoopNode               must have outputTypes set
     * @param executor                     the executor to build and execute child-tasks
     * @param projectionToProjectorVisitor used for building the ProjectorChain
     */
    public NestedLoopOperation(NestedLoopNode nestedLoopNode,
                               NestedLoopExecutorService nestedLoopExecutorService,
                               List<Task> outerTasks,
                               List<Task> innerTasks,
                               TaskExecutor executor,
                               ProjectionToProjectorVisitor projectionToProjectorVisitor,
                               RamAccountingContext ramAccountingContext) {
        this.nestedLoopNode = nestedLoopNode;
        this.nestedLoopExecutorService = nestedLoopExecutorService;
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
        } else {
            rowCombinator = OUTER_COMBINATOR;
        }

        this.outerRelationTasks = outerTasks;
        this.innerRelationTasks = innerTasks;
    }

    protected int limit() {
        return this.limit;
    }

    protected int offset() {
        return this.offset;
    }

    private boolean needsToFetchAllForPaging() {
        return !this.projections.isEmpty();
    }

    protected FlatProjectorChain initializeProjectors() {
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
            task.start(pageInfo);
            return task.result();
        } else {
            logger.trace("fetching whole source relation - ignoring page", pageInfo);
            return this.taskExecutor.execute(tasks);
        }
    }

    private void loadStrategy(Optional<PageInfo> pageInfo) {
        if (pageInfo.isPresent()) {
            if (!needsToFetchAllForPaging()){
                strategy = new PagingNestedLoopStrategy(this, nestedLoopExecutorService);
            } else {
                strategy = new FetchedPagingNestedLoopStrategy(this, nestedLoopExecutorService);
            }
        } else {
            strategy = new OneShotNestedLoopStrategy(this, nestedLoopExecutorService);
        }
    }

    public ListenableFuture<TaskResult> execute(final Optional<PageInfo> pageInfo) {
        // only optimize if no offset for this
        loadStrategy(pageInfo);
        if (logger.isTraceEnabled()) {
            logger.trace("executing NestedLoop: {}{}",
                    strategy.name(),
                    pageInfo.isPresent() ? ", page: " + pageInfo.get().toString() : ""
            );
        }

        if (limit() == 0) {
            // shortcut
            return Futures.immediateFuture(strategy.emptyResult());
        }

        FlatProjectorChain projectorChain = initializeProjectors();

        int rowsToProduce = strategy.rowsToProduce(pageInfo);
        if (rowsToProduce == TopN.NO_LIMIT) {
            // if we have no limit, we assume the default limit for the first page
            rowsToProduce = offset() + Constants.DEFAULT_SELECT_LIMIT;
        }

        // this size is set arbitrarily to a value between MIN_ and MAX_PAGE_SIZE
        // we arbitrarily assume that the inner relation produces around 10 results
        // and we fetch all that is needed to fetch all others
        final int outerPageSize = Math.min(rowsToProduce,
                Math.max(MIN_PAGE_SIZE,
                        Math.min(rowsToProduce / 10, MAX_PAGE_SIZE)
                )
        );
        final PageInfo outerPageInfo = PageInfo.firstPage(outerPageSize);
        if (logger.isTraceEnabled()) {
            logger.trace("fetching page {} from outer relation {}", outerPageInfo, new PlanPrinter().print(nestedLoopNode.outer()));
        }
        List<ListenableFuture<TaskResult>> outerResults = executeChildTasks(outerRelationTasks, outerPageInfo);

        int innerPageSize = Math.max(MIN_PAGE_SIZE, Math.min(rowsToProduce / outerPageSize, MAX_PAGE_SIZE));

        final PageInfo innerPageInfo = PageInfo.firstPage(innerPageSize);
        if (logger.isTraceEnabled()) {
            logger.trace("fetching page {} from inner relation {}", innerPageInfo, new PlanPrinter().print(nestedLoopNode.inner()));
        }
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
                            // TODO: no sorting + real paging Optimization: use non-caching TaskResults
                            final JoinContext joinContext = new JoinContext(
                                    results.get(0),
                                    outerPageInfo,
                                    results.get(1),
                                    innerPageInfo,
                                    rowCombinator
                            );
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
                                    strategy.onFirstJoin(joinContext);
                                }

                                @Override
                                public void onFailure(Throwable t) {
                                    downstream.upstreamFailed(t);
                                    close();
                                }
                            };

                            if (joinContext.hasZeroRowRelation()) {
                                // shortcut
                                callback.onSuccess(null);
                            } else {
                                executeAsync(joinContext, pageInfo, callback);
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
                    Futures.addCallback(joinContextFuture, new FutureCallback<JoinContext>() {
                        @Override
                        public void onSuccess(@Nullable JoinContext joinContext) {
                            if (joinContext == null) {
                                future.setException(new NullPointerException("joinContext is null"));
                                return;
                            }
                            joinContext.finishedFirstIteration();
                            future.set(
                                    strategy.produceFirstResult(rows, pageInfo, joinContext)
                            );
                        }

                        @Override
                        public void onFailure(Throwable t) {
                            logger.error("error waiting for the join for the initial page to finish", t);
                            future.setException(t);
                        }
                    });
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
     * entry point for starting the nestedloop execution when both the
     * outerTaskResult and the innerTaskResult have already been fetched e.g. at
     * first start of the nestedloop task.
     *
     * @param ctx bearing state needed for joining things
     * @param pageInfo if a pageInfo is present, use the position and size of it
     *                 in combination with query offset and limit to determine
     *                 the number of rows to produce.
     * @param callback called when the execution is done
     */
    private void executeAsync(final JoinContext ctx, final Optional<PageInfo> pageInfo, final FutureCallback<Void> callback) {
        NestedLoopStrategy.NestedLoopExecutor executor = strategy.executor(ctx, pageInfo, downstream, callback);
        executor.onNewOuterPage(ctx.outerTaskResult);
    }


    @Override
    public void downstream(Projector downstream) {
        this.downstream = downstream;
        this.downstream.registerUpstream(this);
    }

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

    static class NestedLoopPageableTaskResult extends AbstractNonCachingPageableTaskResult<NestedLoopPageableTaskResult> {

        private final JoinContext joinContext;
        private final NestedLoopOperation operation;
        private final ESLogger logger = Loggers.getLogger(getClass());

        public NestedLoopPageableTaskResult(NestedLoopOperation operation,
                                            ObjectArray<Object[]> backingArray,
                                            long backingArrayStartIndex,
                                            PageInfo pageInfo,
                                            JoinContext joinContext) {
            super(backingArray, backingArrayStartIndex, pageInfo);
            this.operation = operation;
            this.joinContext = joinContext;
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
        protected void fetchFromSource(final int from, final int size, final FutureCallback<ObjectArray<Object[]>> callback) {
            final FlatProjectorChain projectorChain = operation.initializeProjectors();
            final Projector downstream = projectorChain.firstProjector();
            PageInfo pageInfo = new PageInfo(from, size);
            NestedLoopStrategy.NestedLoopExecutor executor = operation.strategy.executor(joinContext, Optional.of(pageInfo), downstream, new FutureCallback<Void>() {

                @Override
                public void onSuccess(@Nullable Void result) {
                    downstream.upstreamFinished();
                    Futures.addCallback(projectorChain.result(), new FutureCallback<Object[][]>() {
                        @Override
                        public void onSuccess(@Nullable Object[][] result) {
                            if (result == null) {
                                callback.onFailure(new NullPointerException("NestedLoop result page is null"));
                            } else {
                                if (logger.isTraceEnabled()) {
                                    logger.trace("fetched {} new rows from NestedLoop for page from {} size {}.",
                                            result.length, from, size);
                                }
                                ObjectArray<Object[]> resultArray = new MultiNativeArrayBigArray<Object[]>(0, result.length, result);
                                callback.onSuccess(resultArray);
                            }
                        }

                        @Override
                        public void onFailure(Throwable t) {
                            callback.onFailure(t);
                        }
                    });
                }

                @Override
                public void onFailure(Throwable t) {
                    downstream.upstreamFailed(t);
                    close();
                    callback.onFailure(t);
                }
            });
            // directly carry on where we left
            executor.joinInnerPage();
        }

        @Override
        protected ListenableFuture<PageableTaskResult> fetchWithNewQuery(PageInfo pageInfo) {
            if (operation.limit() != TopN.NO_LIMIT && operation.limit() - pageInfo.position() <= 0) {
                closeSafe();
                return Futures.immediateFuture(PageableTaskResult.EMPTY_PAGEABLE_RESULT);
            }
            return Futures.transform(operation.execute(Optional.of(pageInfo)), new Function<TaskResult, PageableTaskResult>() {
                @Nullable
                @Override
                public PageableTaskResult apply(@Nullable TaskResult input) {
                    assert input != null && input instanceof PageableTaskResult;
                    return (PageableTaskResult)input;
                }
            });
        }

        @Override
        protected int maxGapSize() {
            // it is always cheaper to iterate through the sources than to
            // start a new query, as this query would have to iterate through the sources as well
            return Integer.MAX_VALUE;
        }

        @Override
        protected void closeSafe() {
            close();
        }

        @Override
        protected NestedLoopPageableTaskResult newTaskResult(PageInfo pageInfo, ObjectArray<Object[]> pageSource, long startIndexAtPageSource) {
            return new NestedLoopPageableTaskResult(operation, pageSource, startIndexAtPageSource, pageInfo, joinContext);
        }
    }
}