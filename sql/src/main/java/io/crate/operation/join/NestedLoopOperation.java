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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.crate.breaker.RamAccountingContext;
import io.crate.executor.TaskResult;
import io.crate.executor.Task;
import io.crate.executor.TaskExecutor;
import io.crate.operation.ProjectorUpstream;
import io.crate.operation.projectors.FlatProjectorChain;
import io.crate.operation.projectors.ProjectionToProjectorVisitor;
import io.crate.operation.projectors.Projector;
import io.crate.planner.node.dql.join.NestedLoopNode;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import java.util.Iterator;
import java.util.List;
import java.util.UUID;


public class NestedLoopOperation implements ProjectorUpstream {

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

    static interface RelationIterable extends Iterable<Object[]> {
    }

    private static class FetchedRowsIterable implements RelationIterable {

        private final Object[][] rows;

        public FetchedRowsIterable(Object[][] rows) {
            this.rows = rows;
        }

        @Override
        public Iterator<Object[]> iterator() {
            return Iterators.forArray(rows);
        }
    }

    private final ESLogger logger = Loggers.getLogger(getClass());

    private final int limit;
    private final List<Task> outerRelationTasks;
    private final List<Task> innerRelationTasks;

    private final FlatProjectorChain projectorChain;
    private final TaskExecutor taskExecutor;
    private Projector downstream;

    private RowCombinator rowCombinator;

    /**
     * OPTIMIZATION: always let inner relation be the one with the smaller limit
     * and fewer records
     *
     * @param nestedLoopNode               must have outputTypes set
     * @param executor                     the executor to build and execute child-tasks
     * @param projectionToProjectorVisitor used for building the ProjectorChain
     * @param jobId                        identifies the job this operation is executed in
     */
    public NestedLoopOperation(NestedLoopNode nestedLoopNode,
                               TaskExecutor executor,
                               ProjectionToProjectorVisitor projectionToProjectorVisitor,
                               RamAccountingContext ramAccountingContext,
                               UUID jobId) {
        this.limit = nestedLoopNode.limit();

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
        this.taskExecutor = executor;
        // TODO: optimize for outer and inner being the same relation
        this.outerRelationTasks = this.taskExecutor.newTasks(nestedLoopNode.outer(), jobId);
        this.innerRelationTasks = this.taskExecutor.newTasks(nestedLoopNode.inner(), jobId);
        this.projectorChain = new FlatProjectorChain(nestedLoopNode.projections(), projectionToProjectorVisitor, ramAccountingContext);
    }

    public ListenableFuture<Object[][]> execute() {
        downstream(this.projectorChain.firstProjector());
        this.projectorChain.startProjections();

        if (limit == 0) {
            // shortcut
            return Futures.immediateFuture(TaskResult.EMPTY_ROWS);
        }

        List<ListenableFuture<TaskResult>> outerResults = this.taskExecutor.execute(outerRelationTasks, null);
        assert outerResults.size() == 1;
        List<ListenableFuture<TaskResult>> innerResults = this.taskExecutor.execute(innerRelationTasks, null);
        assert innerResults.size() == 1;

        Futures.addCallback(Futures.allAsList(ImmutableList.of(outerResults.get(0), innerResults.get(0))), new FutureCallback<List<TaskResult>>() {
            @Override
            public void onSuccess(List<TaskResult> results) {
                assert results.size() == 2;
                try {
                    RelationIterable outerIterable = new FetchedRowsIterable(results.get(0).rows());
                    RelationIterable innerIterable = new FetchedRowsIterable(results.get(1).rows());

                    doExecute(outerIterable, innerIterable);
                    projectorChain.firstProjector().upstreamFinished();
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
        return projectorChain.result();
    }

    private void doExecute(RelationIterable outerIterable, RelationIterable innerIterable) {
        boolean wantMore;
        Outer:
        for (Object[] outerRow : outerIterable) {
            for (Object[] innerRow : innerIterable) {
                wantMore = downstream.setNextRow(
                        rowCombinator.combine(outerRow, innerRow)
                );
                if (!wantMore) {
                    break Outer;
                }
            }
        }
    }

    @Override
    public void downstream(Projector downstream) {
        this.downstream = downstream;
        this.downstream.registerUpstream(this);
    }
}