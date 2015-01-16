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

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.crate.Constants;
import io.crate.breaker.RamAccountingContext;
import io.crate.core.bigarray.IterableBigArray;
import io.crate.core.bigarray.MultiNativeArrayBigArray;
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
import java.io.Closeable;
import java.io.IOException;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.List;


public class NestedLoopOperation implements ProjectorUpstream {

    public static final int DEFAULT_PAGE_SIZE = 1024;

    public static interface RowCombinator {

        public Object[] combine(Object[] left, Object[] right);
    }

    /**
     * [l, e, f, t] + [r, i, g, h, t] = [l, e, f, t, r, i, g, h, t]
     */
    static class BothRowCombinator implements RowCombinator {

        protected final int leftNumColumns;
        protected final int rightNumColumns;

        public BothRowCombinator(int leftNumColumns, int rightNumColumns) {
            this.leftNumColumns = leftNumColumns;
            this.rightNumColumns = rightNumColumns;
        }

        public static Object[] combine(Object[] left, int leftNumCols, Object[] right, int rightNumCols) {
            // TODO: avoid creating new array for each row
            Object[] newRow = new Object[leftNumCols + rightNumCols];
            System.arraycopy(left, 0, newRow, 0, leftNumCols);
            System.arraycopy(right, 0, newRow, leftNumCols, rightNumCols);
            return newRow;
        }

        @Override
        public Object[] combine(Object[] left, Object[] right) {
            return combine(left, leftNumColumns, right, rightNumColumns);
        }
    }

    /**
     * [l, e, f, t] + [r, i, g, h, t] = [r, i, g, h, t, l, e, f, t]
     */
    static class BothRowSwitchingCombinator extends BothRowCombinator {

        public BothRowSwitchingCombinator(int leftNumColumns, int rightNumColumns) {
            // switch numcolumns here
            super(rightNumColumns, leftNumColumns);
        }

        @Override
        public Object[] combine(Object[] left, Object[] right) {
            return combine(right, rightNumColumns, left, leftNumColumns);
        }
    }


    /**
     * [l, e, f, t] + [] = [l, e, f, t]
     */
    private static final RowCombinator LEFT_COMBINATOR = new RowCombinator() {

        @Override
        public Object[] combine(Object[] left, Object[] right) {
            return left;
        }
    };

    /**
     * [] + [r, i, g, h, t] = [r, i, g, h, t]
     */
    private static final RowCombinator RIGHT_COMBINATOR = new RowCombinator() {

        @Override
        public Object[] combine(Object[] left, Object[] right) {
            return right;
        }
    };

    /**
     * contains state needed during join execution
     * and must be portable between multiple execution steps
     */
    private static class JoinContext implements Closeable {

        final RelationIterable outerIterable;
        final RelationIterable innerIterable;
        final boolean singlePage;

        Iterator<Object[]> outerIterator;
        Iterator<Object[]> innerIterator;

        private JoinContext(RelationIterable outerIterable,
                            RelationIterable innerIterable,
                            boolean singlePage) {
            this.outerIterable = outerIterable;
            this.innerIterable = innerIterable;
            this.singlePage = singlePage;
        }

        void refreshOuterIteratorIfNeeded() {
            try {
                if (outerIterator == null || !outerIterator.hasNext()) {
                    // outer iterator is iterated pagewise only
                    outerIterator = outerIterable.forCurrentPage();
                }
            } catch (ConcurrentModificationException e) {
                // underlying list has been changed
                outerIterator = outerIterable.forCurrentPage();
            }
        }

        void refreshInnerIteratorIfNeeded() {
            try {
                if (innerIterator == null || !innerIterator.hasNext()) {
                    innerIterator = innerIterable.iterator();
                }
            } catch (ConcurrentModificationException e) {
                // underlying list has been changed
                innerIterator = innerIterable.iterator();
            }
        }

        ListenableFuture<Void> outerFetchNextPage() {
            return outerIterable.fetchPage(outerIterable.currentPageInfo().nextPage());
        }

        ListenableFuture<Void> innerFetchNextPage() {
            return innerIterable.fetchPage(innerIterable.currentPageInfo().nextPage());
        }

        boolean innerNeedsToFetchMore() {
            return innerIterator != null && !innerIterator.hasNext() && !innerIterable.isComplete();
        }

        boolean outerNeedsToFetchMore() {
            return outerIterator != null && !outerIterator.hasNext() && !outerIterable.isComplete();
        }

        @Override
        public void close() throws IOException {
            outerIterable.close();
            innerIterable.close();
        }
    }

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
        this.limit = nestedLoopNode.limit() == TopN.NO_LIMIT ? Constants.DEFAULT_SELECT_LIMIT : nestedLoopNode.limit();
        this.offset = nestedLoopNode.offset();

        this.ramAccountingContext = ramAccountingContext;
        this.projectionToProjectorVisitor = projectionToProjectorVisitor;
        this.projections = nestedLoopNode.projections();
        this.taskExecutor = executor;

        int leftNumColumns = nestedLoopNode.left().outputTypes().size();
        int rightNumColumns = nestedLoopNode.right().outputTypes().size();
        if (leftNumColumns > 0 && rightNumColumns > 0) {
            if (nestedLoopNode.leftOuterLoop()) {
                rowCombinator = new BothRowCombinator(leftNumColumns, rightNumColumns);
            } else {
                rowCombinator = new BothRowSwitchingCombinator(leftNumColumns, rightNumColumns);
            }
        } else if (leftNumColumns == 0) {
            rowCombinator = RIGHT_COMBINATOR;
        } else if (rightNumColumns == 0) {
            rowCombinator = LEFT_COMBINATOR;
        }

        this.outerRelationTasks = outerTasks;
        this.innerRelationTasks = innerTasks;
    }

    private List<ListenableFuture<TaskResult>> executeChildTasks(List<Task> tasks, PageInfo pageInfo) {
        assert !tasks.isEmpty() : "nested loop child tasks are empty";
        if (tasks.size() == 1 &&
                tasks.get(0) instanceof PageableTask) {
            // one pageable task, page it
            PageableTask task = (PageableTask) tasks.get(0);
            logger.debug("[NestedLoop] fetching {} rows from source relation", pageInfo.size());
            task.start(pageInfo);
            return task.result();
        }
        return this.taskExecutor.execute(tasks, null);
    }

    public ListenableFuture<TaskResult> execute(final Optional<PageInfo> pageInfo) {
        FlatProjectorChain projectorChain = new FlatProjectorChain(projections, projectionToProjectorVisitor, ramAccountingContext);
        downstream(projectorChain.firstProjector());
        projectorChain.startProjections();

        if (limit == 0) {
            // shortcut
            return Futures.immediateFuture(
                    pageInfo.isPresent()
                            ? PageableTaskResult.EMPTY_PAGABLE_RESULT
                            : TaskResult.EMPTY_RESULT);
        }

        int rowsToProduce = limit + offset;
        // this size is set arbitrarily to a value between 1 and DEFAULT_PAGE_SIZE
        // we arbitrarily assume that the inner relation produces around 10 results
        // and we fetch all that is needed to fetch all others
        int outerPageSize = Math.max(1, Math.min(rowsToProduce / 10, DEFAULT_PAGE_SIZE));
        final PageInfo outerPageInfo = new PageInfo(0, outerPageSize);
        List<ListenableFuture<TaskResult>> outerResults = executeChildTasks(outerRelationTasks, outerPageInfo);

        int innerPageSize = rowsToProduce/outerPageSize;

        final PageInfo innerPageInfo = new PageInfo(0, innerPageSize);
        List<ListenableFuture<TaskResult>> innerResults = executeChildTasks(innerRelationTasks, innerPageInfo);

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
                                    innerIterable,
                                    false);
                            joinContext.refreshOuterIteratorIfNeeded(); // initialize outer iterator
                            executeAsync(joinContext, new FutureCallback<Void>() {

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
                                    close();
                                }

                                @Override
                                public void onFailure(Throwable t) {
                                    downstream.upstreamFailed(t);
                                    close();
                                }
                            });
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

        return Futures.transform(projectorChain.result(), new Function<Object[][], TaskResult>() {
            @Nullable
            @Override
            public TaskResult apply(Object[][] rows) {
                if (pageInfo.isPresent()) {
                    IterableBigArray<Object[]> wrappedRows = new MultiNativeArrayBigArray<Object[]>(0, rows.length, rows);
                    return new FetchedRowsPageableTaskResult(wrappedRows, 0L, pageInfo.get());
                } else {
                    return new QueryResult(rows);
                }
            }
        });
    }

    private void executeAsync(final JoinContext ctx, final FutureCallback<Void> callback) {
        boolean wantMore = true;

        Outer:
        while (ctx.outerIterator.hasNext()) {
            Object[] outerRow = ctx.outerIterator.next();

            ctx.refreshInnerIteratorIfNeeded();
            while (ctx.innerIterator.hasNext()) {
                Object[] innerRow = ctx.innerIterator.next();
                wantMore = downstream.setNextRow(
                        rowCombinator.combine(outerRow, innerRow)
                );
                if (!wantMore) {
                    break Outer;
                }
            }
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
}