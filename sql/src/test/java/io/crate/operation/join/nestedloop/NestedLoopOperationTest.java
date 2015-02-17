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

import com.carrotsearch.randomizedtesting.generators.RandomInts;
import com.carrotsearch.randomizedtesting.generators.RandomStrings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.Constants;
import io.crate.core.bigarray.IterableBigArray;
import io.crate.core.bigarray.MultiNativeArrayBigArray;
import io.crate.executor.*;
import io.crate.executor.task.join.NestedLoopTask;
import io.crate.executor.transport.TransportActionProvider;
import io.crate.metadata.Functions;
import io.crate.metadata.MetaDataModule;
import io.crate.metadata.ReferenceResolver;
import io.crate.operation.ImplementationSymbolVisitor;
import io.crate.operation.projectors.ProjectionToProjectorVisitor;
import io.crate.operation.projectors.TopN;
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.planner.Plan;
import io.crate.planner.PlanVisitor;
import io.crate.planner.RowGranularity;
import io.crate.planner.node.PlanNode;
import io.crate.planner.node.PlanNodeVisitor;
import io.crate.planner.node.dql.AbstractDQLPlanNode;
import io.crate.planner.node.dql.join.NestedLoopNode;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.TopNProjection;
import io.crate.planner.symbol.InputColumn;
import io.crate.planner.symbol.Symbol;
import io.crate.testing.MockedClusterServiceModule;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.Closeable;
import java.util.*;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

@RunWith(Parameterized.class)
public class NestedLoopOperationTest {

    private class ImmediateTestTask extends JobTask {

        private final List<ListenableFuture<TaskResult>> result;

        public ImmediateTestTask(Object[][] rows, int limit, int offset) {
            super(UUID.randomUUID());
            Object[][] limitedRows = Arrays.copyOfRange(rows,
                    Math.min(offset, rows.length),
                    limit < 0 ? rows.length : Math.min(limit, rows.length)
            );
            this.result = ImmutableList.of(
                    Futures.<TaskResult>immediateFuture(new QueryResult(limitedRows)));
        }


        @Override
        public void start() {
            // ignore
        }

        @Override
        public List<ListenableFuture<TaskResult>> result() {
            return result;
        }

        @Override
        public void upstreamResult(List result) {
            // ignore
        }
    }

    private class PageableTestTask extends JobTask implements PageableTask {

        private IterableBigArray<Object[]> backingArray;
        private SettableFuture<TaskResult> result;

        protected PageableTestTask(Object[][] rows, int limit, int offset) {
            super(UUID.randomUUID());
            backingArray = new MultiNativeArrayBigArray<Object[]>(offset, limit, rows);
            result = SettableFuture.create();
        }

        @Override
        public void start(PageInfo pageInfo) {
            result.set(
                    FetchedRowsPageableTaskResult.forArray(backingArray, 0, pageInfo)
            );
        }

        @Override
        public void fetchMore(PageInfo pageInfo, Closeable context, FutureCallback callback) {

        }

        @Override
        public void startCached(PageInfo pageInfo) {
            start(pageInfo);
        }

        @Override
        public void fetchNew(PageInfo pageInfo, Closeable context, FutureCallback callback) {

        }

        @Override
        public void start() {
            // ignore
        }

        @Override
        public List<ListenableFuture<TaskResult>> result() {
            return ImmutableList.<ListenableFuture<TaskResult>>of(result);
        }

        @Override
        public void upstreamResult(List<ListenableFuture<TaskResult>> result) {
            // ignore
        }
    }
    private class TestDQLNode extends AbstractDQLPlanNode implements Plan {

        private final Object[][] rows;

        private TestDQLNode(Object[][] rows) {
            this.rows = rows;
            if (rows.length > 0) {
                this.outputTypes(Collections.<DataType>nCopies(rows[0].length, DataTypes.UNDEFINED)); // could be any type
            }
        }

        @Override
        public Set<String> executionNodes() {
            return ImmutableSet.of();
        }

        @Override
        public <C, R> R accept(PlanNodeVisitor<C, R> visitor, C context) {
            return null;
        }

        public Task task() {
            if (pagedSources) {
                return new PageableTestTask(rows, rows.length, 0);
            } else {
                return new ImmediateTestTask(rows, rows.length, 0);
            }
        }

        @Override
        public <C, R> R accept(PlanVisitor<C, R> visitor, C context) {
            return null;
        }
    }


    private class TestExecutor implements TaskExecutor {

        @Override
        public List<Task> newTasks(PlanNode planNode, Job job) {
            return ImmutableList.of(((TestDQLNode) planNode).task());
        }

        @Override
        public List<ListenableFuture<TaskResult>> execute(Collection<Task> tasks) {
            return Iterables.getLast(tasks).result();
        }
    }

    @Parameterized.Parameters(name="{index}: leftOuterLoop={0}, sourcesPaged={1}, nestedLoopPaged={2}")
    public static Collection<Object[]> data() {
        boolean[] trueFalse = new boolean[]{true, false};
        List<Object[]> data = new ArrayList<>((int)Math.pow(4, 2));

        // build cross product
        for (boolean leftOuterLoop : trueFalse) {
            for (boolean pagedSources : trueFalse) {
                for (boolean nlPaged : trueFalse) {
                    data.add(new Object[]{leftOuterLoop, pagedSources, nlPaged});
                }
            }
        }
        return data;
    }

    private final boolean leftOuterNode;
    private final boolean pagedSources;
    private final boolean nestedLoopPaged;
    private final Random random;
    private NestedLoopExecutorService nestedLoopExecutorService;

    public NestedLoopOperationTest(boolean leftOuterNode,
                                   boolean pagedSources,
                                   boolean nestedLoopPaged) {
        this.leftOuterNode = leftOuterNode;
        this.pagedSources = pagedSources;
        this.nestedLoopPaged = nestedLoopPaged;
        this.random = new Random();
        this.nestedLoopExecutorService = new NestedLoopExecutorService(ImmutableSettings.EMPTY);
        this.nestedLoopExecutorService.doStart();
    }

    private ProjectionToProjectorVisitor projectionVisitor;

    @Before
    public void prepare() {
        ModulesBuilder builder = new ModulesBuilder()
                .add(new MockedClusterServiceModule())
                .add(new MetaDataModule())
                .add(new ScalarFunctionModule());
        Injector injector = builder.createInjector();
        Functions functions = injector.getInstance(Functions.class);
        ReferenceResolver referenceResolver = injector.getInstance(ReferenceResolver.class);
        ImplementationSymbolVisitor implementationSymbolVisitor = new ImplementationSymbolVisitor(referenceResolver, functions, RowGranularity.CLUSTER);
        TransportActionProvider transportActionProvider = mock(TransportActionProvider.class);
        projectionVisitor = new ProjectionToProjectorVisitor(mock(ClusterService.class),
                ImmutableSettings.EMPTY, transportActionProvider,
                implementationSymbolVisitor);
    }

    private void assertNestedLoop(Object[][] left, Object[][] right, int limit, int offset, int expectedRows) throws Exception {
        assertNestedLoop(left, right, limit, offset, expectedRows, true);
    }

    private void assertNestedLoop(Object[][] left, Object[][] right, int limit, int offset, int expectedRows, boolean applyTopN) throws Exception {
        TestDQLNode leftPlan = new TestDQLNode(left);
        TestDQLNode rightPlan = new TestDQLNode(right);

        NestedLoopNode node = new NestedLoopNode(leftPlan, rightPlan, leftOuterNode, limit, offset);
        int numColumns = (left.length > 0 ? left[0].length : 0) + (right.length > 0 ? right[0].length : 0);

        if (applyTopN) {
            TopNProjection projection = new TopNProjection(limit, offset);
            List<Symbol> outputs = new ArrayList<>(numColumns);
            for (int i = 0; i < numColumns; i++) {
                outputs.add(new InputColumn(i, DataTypes.UNDEFINED));
            }
            projection.outputs(outputs);
            node.projections(ImmutableList.<Projection>of(projection));
        }
        TaskExecutor taskExecutor = new TestExecutor();
        Job fakeJob = new Job();
        Job outerJob = new Job();
        outerJob.addTasks(taskExecutor.newTasks((leftOuterNode ? leftPlan : rightPlan), fakeJob));

        Job innerJob = new Job();
        innerJob.addTasks(taskExecutor.newTasks((leftOuterNode ? rightPlan : leftPlan), fakeJob));

        NestedLoopTask task = new NestedLoopTask(
                fakeJob.id(),
                "nestedloop",
                node,
                outerJob,
                innerJob,
                taskExecutor,
                nestedLoopExecutorService,
                projectionVisitor,
                mock(CircuitBreaker.class)
        );

        Object[][] result = executeNestedLoop(task, limit);

        int i = 0;
        int leftIdx = 0;
        int rightIdx = 0;
        int skip = offset;

        // skip offset
        while (skip > 0) {
            if (leftOuterNode) {
                rightIdx++;
                if (rightIdx == right.length) {
                    rightIdx = 0;
                    leftIdx++;
                }
            } else {
                leftIdx++;
                if (leftIdx == left.length) {
                    leftIdx = 0;
                    rightIdx++;
                }
            }
            skip--;
        }

        for (Object[] row : result) {
            int rowIdx = 0;


            if (leftOuterNode) {
                if (rightIdx == right.length) {
                    rightIdx = 0;
                    leftIdx++;
                }
            } else {
                if (leftIdx == left.length) {
                    leftIdx = 0;
                    rightIdx++;
                }
            }

            assertThat(row.length, is(left[leftIdx].length + right[rightIdx].length));


            for (int j = 0; j < left[leftIdx].length; j++) {
                assertThat(
                        String.format("expected result row (%d)  %s - %s, got %s", i, Arrays.toString(left[leftIdx]), Arrays.toString(right[rightIdx]), Arrays.toString(row)),
                        row[rowIdx], is(left[leftIdx][rowIdx]));
                rowIdx++;
            }
            for (int j = 0; j < right[rightIdx].length; j++) {
                assertThat(
                        String.format("expected result row (%d) %s, got %s - %s", i, Arrays.toString(row), Arrays.toString(left[leftIdx]), Arrays.toString(right[rightIdx])),
                        row[rowIdx], is(right[rightIdx][j])
                );
                rowIdx++;
            }
            i++;
            if (leftOuterNode) {
                rightIdx++;
            } else {
                leftIdx++;
            }
        }
        assertThat(i, is(expectedRows));
    }

    private void assertPagedFullNestedLoop(Object[][] left, Object[][] right) throws Exception {
        int expectedRows = left.length * right.length;
        assertNestedLoop(left, right, TopN.NO_LIMIT, 0, expectedRows, false);
    }

    private Object[][] executeNestedLoop(NestedLoopTask nestedLoop, int limit) throws Exception {

        if (nestedLoopPaged) {
            int actualLimit = (limit == TopN.NO_LIMIT ? Constants.DEFAULT_SELECT_LIMIT : limit);
            int pageSize = actualLimit >= 10 ? actualLimit/10 : Math.max(actualLimit, 1);
            PageInfo pageInfo = new PageInfo(0, pageSize);
            nestedLoop.start(pageInfo);
            TaskResult pageableTaskResult = nestedLoop.result().get(0).get();
            List<Object[]> rows = new ArrayList<>();
            while (pageableTaskResult.page().size() > 0L) {
                for (Object[] row : pageableTaskResult.page()) {
                    rows.add(row);
                }
                pageInfo = pageInfo.nextPage();
                pageableTaskResult = pageableTaskResult.fetch(pageInfo).get();
            }
            return rows.toArray(new Object[rows.size()][]);

        } else {
            nestedLoop.start();
            return nestedLoop.result().get(0).get().rows();
        }
    }

    private Object[][] randomRows(int numRows, int rowLength) {
        Object[][] rows = new Object[numRows][];
        for (int i = 0; i < numRows; i++) {
            rows[i] = randomRow(rowLength);
        }
        return rows;
    }

    private Object[] randomRow(int length) {
        Object[] row = new Object[length];
        for (int i = 0; i < length; i++) {
            switch (RandomInts.randomInt(random, Byte.MAX_VALUE) % 4) {
                case 0:
                    row[i] = RandomInts.randomInt(random, Integer.MAX_VALUE);
                    break;
                case 1:
                    row[i] = RandomStrings.randomAsciiOfLength(random, 10);
                    break;
                case 2:
                    row[i] = null;
                    break;
                case 3:
                    row[i] = (random.nextBoolean() ? -1 : 1) * random.nextDouble();
                    break;
            }
        }
        return row;
    }


    @Test
    public void testNoRows() throws Exception {
        assertNestedLoop(new Object[0][], new Object[0][], 100, 0, 0);
        assertNestedLoop(new Object[0][], new Object[][] {
                new Object[]{1,2,3},
                new Object[]{4,5,6}
        }, 100, 0, 0);
        assertNestedLoop(new Object[][] {
                new Object[]{1,2,3},
                new Object[]{4,5,6}
        }, new Object[0][], 100, 0, 0);

        if (nestedLoopPaged) {
            assertPagedFullNestedLoop(new Object[0][], new Object[0][]);
            assertPagedFullNestedLoop(new Object[][]{
                    new Object[]{1, 2, 3},
                    new Object[]{4, 5, 6}
            }, new Object[0][]);
        }
    }

    @Test
    public void testLimit0() throws Exception {
        Object[][] left = randomRows(10, 4);
        Object[][] right = randomRows(1, 2);

        assertNestedLoop(left, right, 0, 0, 0);

        if (nestedLoopPaged) {
            assertPagedFullNestedLoop(left, right);
        }
    }

    @Test
    public void testNoLimitRightNoRows() throws Exception {
        Object[][] left = randomRows(10, 4);
        Object[][] right = randomRows(0, 2);

        assertNestedLoop(left, right, TopN.NO_LIMIT, 0, 0);

        if (nestedLoopPaged) {
            assertPagedFullNestedLoop(left, right);
        }
    }

    @Test
    public void testNoLimitLeftNoRows() throws Exception {
        Object[][] left = randomRows(0, 4);
        Object[][] right = randomRows(4, 2);

        assertNestedLoop(left, right, TopN.NO_LIMIT, 0, 0);

        if (nestedLoopPaged) {
            assertPagedFullNestedLoop(left, right);
        }
    }

    @Test
    public void testLimit1() throws Exception {
        Object[][] left = randomRows(10, 4);
        Object[][] right = randomRows(1, 2);

        assertNestedLoop(left, right, 1, 0, 1);

        if (nestedLoopPaged) {
            assertPagedFullNestedLoop(left, right);
        }
    }

    @Test
    public void testEmptyRows() throws Exception {
        Object[][] left = new Object[][]{
                new Object[0],
                new Object[0],
                new Object[0]
        };
        Object[][] right = new Object[][]{
                new Object[0],
                new Object[0]
        };
        assertNestedLoop(left, right, 10, 0, 6);

        if (nestedLoopPaged) {
            assertPagedFullNestedLoop(left, right);
        }
    }

    @Test
    public void testLeftEmptyRows() throws Exception {
        Object[][] left = new Object[][]{
                new Object[0],
                new Object[0],
                new Object[0]
        };
        Object[][] right = randomRows(2, 1);
        assertNestedLoop(left, right, 10, 0, 6);

        if (nestedLoopPaged) {
            assertPagedFullNestedLoop(left, right);
        }
    }

    @Test
    public void testLimitBetween() throws Exception {
        Object[][] left = randomRows(10, 4);
        Object[][] right = randomRows(3, 2);

        assertNestedLoop(left, right, 4, 0, 4);

        if (nestedLoopPaged) {
            assertPagedFullNestedLoop(left, right);
        }
    }

    @Test
    public void testLimitSomeMore() throws Exception {
        Object[][] left = randomRows(10, 4);
        Object[][] right = randomRows(3, 2);

        assertNestedLoop(left, right, 32, 0, 30);

        if (nestedLoopPaged) {
            assertPagedFullNestedLoop(left, right);
        }
    }

    @Test
    public void testLimitNoLimit() throws Exception {
        Object[][] left = randomRows(10, 4);
        Object[][] right = randomRows(3, 2);

        assertNestedLoop(left, right, TopN.NO_LIMIT, 0, 30);

        if (nestedLoopPaged) {
            assertPagedFullNestedLoop(left, right);
        }
    }

    @Test
    public void testOffsetNoLimit() throws Exception {
        Object[][] left = new Object[][]{ new Object[]{1}, new Object[]{2}, new Object[]{3}, new Object[]{4} };
        Object[][] right = new Object[][]{ new Object[]{"a"}, new Object[]{"b"} };

        assertNestedLoop(left, right, TopN.NO_LIMIT, 2, 6);

        if (nestedLoopPaged) {
            assertPagedFullNestedLoop(left, right);
        }
    }

    @Test
    public void testOffsetSmallLimit() throws Exception {
        Object[][] left = new Object[][]{ new Object[]{1}, new Object[]{2}, new Object[]{3}, new Object[]{4} };
        Object[][] right = new Object[][]{ new Object[]{"a"}, new Object[]{"b"} };

        assertNestedLoop(left, right, 2, 2, 2);

        if (nestedLoopPaged) {
            assertPagedFullNestedLoop(left, right);
        }
    }

    @Test
    public void testOffsetLimitBetween() throws Exception {
        Object[][] left = new Object[][]{ new Object[]{1}, new Object[]{2}, new Object[]{3}, new Object[]{4} };
        Object[][] right = new Object[][]{ new Object[]{"a"}, new Object[]{"b"} };

        assertNestedLoop(left, right, 5, 2, 5);

        if (nestedLoopPaged) {
            assertPagedFullNestedLoop(left, right);
        }
    }

    @Test
    public void testOffsetBiggerLimit() throws Exception {
        Object[][] left = new Object[][]{ new Object[]{1}, new Object[]{2}, new Object[]{3}, new Object[]{4} };
        Object[][] right = new Object[][]{ new Object[]{"a"}, new Object[]{"b"} };

        assertNestedLoop(left, right, 7, 2, 6);

        if (nestedLoopPaged) {
            assertPagedFullNestedLoop(left, right);
        }
    }

    @Test
    public void testBiggerOffsetLimit() throws Exception {
        Object[][] left = new Object[][]{ new Object[]{1}, new Object[]{2}, new Object[]{3}, new Object[]{4} };
        Object[][] right = new Object[][]{ new Object[]{"a"}, new Object[]{"b"} };

        assertNestedLoop(left, right, 4, 8, 0);

        if (nestedLoopPaged) {
            assertPagedFullNestedLoop(left, right);
        }
    }

    @Test
    public void testMuchBiggerOffsetLimit() throws Exception {
        Object[][] left = new Object[][]{ new Object[]{1}, new Object[]{2}, new Object[]{3}, new Object[]{4} };
        Object[][] right = new Object[][]{ new Object[]{"a"}, new Object[]{"b"} };

        assertNestedLoop(left, right, 4, 1000, 0);

        if (nestedLoopPaged) {
            assertPagedFullNestedLoop(left, right);
        }
    }

    @Test
    public void testBiggerLimitThanDefaultPageSize() throws Exception {
        Object[][] left = randomRows(NestedLoopOperation.MAX_PAGE_SIZE + 10, 1);
        Object[][] right = randomRows(10, 1);

        assertNestedLoop(left, right, NestedLoopOperation.MAX_PAGE_SIZE*2, 10, NestedLoopOperation.MAX_PAGE_SIZE*2);

        if (nestedLoopPaged) {
            assertPagedFullNestedLoop(left, right);
        }
    }
}

