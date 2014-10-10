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

package io.crate.operation.join;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import io.crate.Constants;
import io.crate.exceptions.TaskExecutionException;
import io.crate.executor.Executor;
import io.crate.executor.QueryResult;
import io.crate.executor.Task;
import io.crate.operation.ProjectorUpstream;
import io.crate.operation.projectors.FlatProjectorChain;
import io.crate.operation.projectors.ProjectionToProjectorVisitor;
import io.crate.operation.projectors.Projector;
import io.crate.planner.node.dql.AbstractDQLPlanNode;
import io.crate.planner.node.dql.join.NestedLoopNode;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * this operation performs the nested loop algorithm
 * to join two relations
 */
public class NestedLoopOperation implements ProjectorUpstream {

    /**
     * this iterator wraps a planNode with all the necessary information
     * for fetching rows from a relation
     * <p>
     * It will be transformed to a task and executed when new rows are needed during iteration.
     * <p>
     * The rows are fetched in pages.
     */
    private static class PagedRelationIterator extends AbstractIterator<Object[]> implements Iterable<Object[]> {

        private final AbstractDQLPlanNode dqlPlanNode;
        private final Executor executor;
        private final UUID jobId;
        private final int limit;
        private final int offset;
        private final int pageSize;

        private int rowsFetched = 0;
        private int rowsReturned = 0;
        private AtomicReference<Object[][]> page = new AtomicReference<>();
        private int currentPageSize = 0;
        private int currentPageIdx = 0;

        /**
         * create a new paged iterator for a {@linkplain io.crate.planner.node.dql.AbstractDQLPlanNode}
         *
         * @param executor the executor used to create and execute tasks
         * @param jobId the identifier of the job this iterator is used in (necessary for creating new tasks)
         * @param dqlPlanNode the planNode defining how to fetch rows
         * @param limit the maximum number of rows to fetch
         * @param offset the initial offset when fetching rows
         */
        public PagedRelationIterator(Executor executor, UUID jobId, AbstractDQLPlanNode dqlPlanNode, int limit, int offset) {
            this.executor = executor;
            this.jobId = jobId;
            this.dqlPlanNode = dqlPlanNode;
            this.limit = limit;
            this.offset = offset;
            this.pageSize = limit == NestedLoopNode.NO_LIMIT ? Constants.DEFAULT_SELECT_LIMIT : limit;
                            // TODO: use Math.min(limit, SOME_PAGE_SIZE);
                            // once we have real paging
        }

        @Override
        protected Object[] computeNext() {
            if (limit >= 0 && rowsReturned >= limit) {
                endOfData();
                return null;
            }
            if (page.get() == null || currentPageIdx == currentPageSize) {
                // create new task
                // use limit and offset for paging as long as we have no paging interface
                dqlPlanNode.limit(pageSize);
                dqlPlanNode.offset(offset + rowsFetched);

                @SuppressWarnings("unchecked")
                Task<QueryResult> task = (Task<QueryResult>)executor.newTask(dqlPlanNode, jobId);

                // execute task
                List<ListenableFuture<QueryResult>> futures = executor.execute(task, null);
                assert futures.size() == 1;
                try {
                    // blocking
                    QueryResult queryResult = futures.get(0).get();
                    if (queryResult.rowCount() > 0) {
                        // set new page
                        Object[][] rows = queryResult.rows();
                        page.set(rows);
                        currentPageIdx = 0;
                        currentPageSize = rows.length;
                        rowsFetched += rows.length;
                    } else {
                        // no results
                        endOfData();
                        return null; // ignored
                    }
                } catch (InterruptedException|ExecutionException e) {
                    throw new RuntimeException("error executing task to fetch a new page", e);
                }
            }
            rowsReturned++;
            return page.get()[currentPageIdx++];
        }

        @Override
        public Iterator<Object[]> iterator() {
            return this;
        }
    }


    private final int limit;
    private final int offset;
    private final int leftRowLength;
    private final int rightRowLength;

    private final NestedLoopNode nestedLoopNode;
    private final ThreadPool threadPool;
    private final PagedRelationIterator outerIterator;
    private final PagedRelationIterator innerIterator;
    private final FlatProjectorChain projectorChain;

    private Projector downstream;

    /**
     *
     * OPTIMIZATION: always let inner relation be the one with the smaller limit
     * and fewer records
     *
     * @param nestedLoopNode must have outputTypes set
     * @param executor the executor to build and execute child-tasks
     * @param projectionToProjectorVisitor used for building the ProjectorChain
     * @param jobId identifies the job this operation is executed in
     * @param limit maximum number of rows to produce
     * @param offset the number of rows to skip
     */
    public NestedLoopOperation(NestedLoopNode nestedLoopNode,
                               ThreadPool threadPool,
                               Executor executor,
                               ProjectionToProjectorVisitor projectionToProjectorVisitor,
                               UUID jobId) {
        this.nestedLoopNode = nestedLoopNode;
        this.threadPool = threadPool;
        this.limit = this.nestedLoopNode.limit();
        this.offset = this.nestedLoopNode.offset();
        this.leftRowLength = nestedLoopNode.left().outputTypes().size();
        this.rightRowLength = nestedLoopNode.right().outputTypes().size();
        this.projectorChain = new FlatProjectorChain(nestedLoopNode.projections(), projectionToProjectorVisitor);
        downstream(this.projectorChain.firstProjector());
        this.outerIterator = new PagedRelationIterator(executor, jobId, nestedLoopNode.outerNode(), effectiveLimit(limit, offset), 0);
        this.innerIterator = new PagedRelationIterator(executor, jobId, nestedLoopNode.innerNode(), effectiveLimit(limit, offset), 0);
    }

    private int effectiveLimit(int limit, int offset) {
        if (limit == NestedLoopNode.NO_LIMIT) {
            return NestedLoopNode.NO_LIMIT;
        } else {
            return limit + offset;
        }
    }

    public ListenableFuture<Object[][]> execute() {

        // dispatch to separate thread
        threadPool.generic().execute(new Runnable() {
            @Override
            public void run() {
                projectorChain.startProjections();
                if (nestedLoopNode.leftOuterLoop()) {
                    executeLeftOuter();
                } else {
                    executeRightOuter();
                }
                projectorChain.firstProjector().upstreamFinished();
            }
        });
        return projectorChain.result();
    }


    private void executeLeftOuter() {
        boolean wantMore;
        int rowsProduced = 0;
        int skip = offset;
        List<Object[]> inner = Lists.newArrayList((Iterator<Object[]>)innerIterator);
        Outer: for (Object[] outerRow : outerIterator) {

            for (Object[] innerRow : inner) {
                if (skip > 0) {
                    skip--;
                    continue;
                }
                wantMore = downstream.setNextRow(
                        combine(outerRow, innerRow)
                );
                rowsProduced++;
                if (!wantMore || limit > 0 && rowsProduced >= limit) {
                    break Outer;
                }

            }
        }
    }

    private void executeRightOuter() {
        boolean wantMore;
        int rowsProduced = 0;
        int skip = offset;
        List<Object[]> inner = Lists.newArrayList((Iterator<Object[]>)innerIterator);
        Outer: for (Object[] outerRow : outerIterator) {
            for (Object[] innerRow : inner) {
                if (skip > 0) {
                    skip--;
                    continue;
                }
                wantMore = downstream.setNextRow(
                        combine(innerRow, outerRow)
                );
                rowsProduced++;
                if (!wantMore || limit > 0 && rowsProduced >= limit) {
                    break Outer;
                }

            }
        }
    }

    private Object[] combine(Object[] left, Object[] right) {
        // TODO: avoid creating new array for each row?
        Object[] newRow = new Object[leftRowLength + rightRowLength];
        System.arraycopy(left, 0, newRow, 0, leftRowLength);
        System.arraycopy(right, 0, newRow, leftRowLength, rightRowLength);
        return newRow;
    }

    @Override
    public void downstream(Projector downstream) {
        this.downstream = downstream;
        this.downstream.registerUpstream(this);
    }

    @Override
    public Projector downstream() {
        return downstream;
    }
}
