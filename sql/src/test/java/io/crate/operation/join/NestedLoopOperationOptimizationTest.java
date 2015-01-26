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

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListenableFuture;
import io.crate.breaker.RamAccountingContext;
import io.crate.executor.*;
import io.crate.executor.transport.TransportActionProvider;
import io.crate.metadata.Functions;
import io.crate.metadata.MetaDataModule;
import io.crate.metadata.ReferenceResolver;
import io.crate.operation.ImplementationSymbolVisitor;
import io.crate.operation.projectors.ProjectionToProjectorVisitor;
import io.crate.operation.projectors.TopN;
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.planner.RowGranularity;
import io.crate.planner.node.PlanNode;
import io.crate.planner.node.dql.join.NestedLoopNode;
import io.crate.testing.MockedClusterServiceModule;
import io.crate.testing.TestingHelpers;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class NestedLoopOperationOptimizationTest {

    static {
        Loggers.getLogger(NestedLoopOperation.class).setLevel("TRACE");
        Loggers.getLogger(JoinContext.class).setLevel("TRACE");
    }

    private static class OptimizationTestNode extends TestDQLNode {

        public OptimizationTestNode(Object[][] rows) {
            super(rows);
        }

        @Override
        public Task task() {
            return new PageableTestTask(rows, rows.length, 0);
        }
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
        projectionVisitor = new ProjectionToProjectorVisitor(mock(ClusterService.class), ImmutableSettings.EMPTY, transportActionProvider, implementationSymbolVisitor);
    }

    @Test
    public void testNotOptimized() throws Exception {
        Object[][] left = new Object[][]{
                new Object[]{"a"},
                new Object[]{"b"},
                new Object[]{"c"},
        };

        Object[][] right = new Object[][]{
                new Object[]{1},
                new Object[]{2},
                new Object[]{3},
        };

        PlanNode leftNode = new OptimizationTestNode(left);
        PlanNode rightNode = new OptimizationTestNode(right);

        NestedLoopNode node = new NestedLoopNode(leftNode, rightNode, true, TopN.NO_LIMIT, TopN.NO_OFFSET, true);

        TaskExecutor taskExecutor = new TestExecutor();
        UUID jobId = UUID.randomUUID();
        Task outerTask = taskExecutor.newTasks(leftNode, jobId).get(0);
        Task innerTask = taskExecutor.newTasks(rightNode, jobId).get(0);

        NestedLoopOperation nestedLoop = new NestedLoopOperation(
                node,
                Arrays.asList(outerTask),
                Arrays.asList(innerTask),
                new TestExecutor(),
                projectionVisitor,
                mock(RamAccountingContext.class));

        PageInfo pageInfo = new PageInfo(0, 4);
        ListenableFuture<TaskResult> resultFuture = nestedLoop.execute(Optional.of(pageInfo));
        TaskResult result = resultFuture.get();
        assertThat(result, instanceOf(NestedLoopOperation.NestedLoopPageableTaskResult.class));
        NestedLoopOperation.NestedLoopPageableTaskResult pageableResult = (NestedLoopOperation.NestedLoopPageableTaskResult)result;

        // reflection hack
        Field privateJoinContextField = NestedLoopOperation.NestedLoopPageableTaskResult.class.getDeclaredField("joinContext");
        privateJoinContextField.setAccessible(true);
        JoinContext joinContext = (JoinContext) privateJoinContextField.get(pageableResult);

        // optimization consists of selecting the right RelationIterable
        assertThat(joinContext.innerIterable, instanceOf(CollectingPageableTaskIterable.class));
        assertThat(joinContext.outerIterable, instanceOf(SinglePagePageableTaskIterable.class));

        assertThat(TestingHelpers.printedPage(pageableResult.page()), is(
                "a| 1\n" +
                "a| 2\n" +
                "a| 3\n" +
                "b| 1\n"));

        pageInfo = pageInfo.nextPage(2);
        PageableTaskResult pageableTaskResult = pageableResult.fetch(pageInfo).get();
        assertThat(TestingHelpers.printedPage(pageableTaskResult.page()), is(
                "b| 2\n" +
                "b| 3\n"
        ));
        pageInfo = pageInfo.nextPage(2);
        pageableTaskResult = pageableTaskResult.fetch(pageInfo).get();
        assertThat(TestingHelpers.printedPage(pageableTaskResult.page()), is(
                "c| 1\n" +
                "c| 2\n"
        ));
        pageInfo = pageInfo.nextPage(2);
        pageableTaskResult = pageableTaskResult.fetch(pageInfo).get();
        assertThat(TestingHelpers.printedPage(pageableTaskResult.page()), is(
                "c| 3\n"
        ));

        assertThat(pageableTaskResult.fetch(pageInfo.nextPage()).get().page().size(), is(0L));
    }

    @Test
    public void testOptimizedWithNoSorting() throws Exception {
        Object[][] left = new Object[][] {
                new Object[] { "a" },
                new Object[] { "b" },
                new Object[] { "c" },
        };

        Object[][] right = new Object[][] {
                new Object[] { 1 },
                new Object[] { 2 },
                new Object[] { 3 },
        };

        PlanNode leftNode = new OptimizationTestNode(left);
        PlanNode rightNode = new OptimizationTestNode(right);

        NestedLoopNode node = new NestedLoopNode(leftNode, rightNode, true, TopN.NO_LIMIT, TopN.NO_OFFSET, false);

        TaskExecutor taskExecutor = new TestExecutor();
        UUID jobId = UUID.randomUUID();
        Task outerTask = taskExecutor.newTasks(leftNode, jobId).get(0);
        Task innerTask = taskExecutor.newTasks(rightNode, jobId).get(0);

        NestedLoopOperation nestedLoop = new NestedLoopOperation(
                node,
                Arrays.asList(outerTask),
                Arrays.asList(innerTask),
                new TestExecutor(),
                projectionVisitor,
                mock(RamAccountingContext.class));

        PageInfo pageInfo = new PageInfo(0, 4);
        ListenableFuture<TaskResult> resultFuture = nestedLoop.execute(Optional.of(pageInfo));
        TaskResult result = resultFuture.get();
        assertThat(result, instanceOf(NestedLoopOperation.NestedLoopPageableTaskResult.class));
        NestedLoopOperation.NestedLoopPageableTaskResult pageableResult = (NestedLoopOperation.NestedLoopPageableTaskResult)result;

        // reflection hack
        Field privateJoinContextField = NestedLoopOperation.NestedLoopPageableTaskResult.class.getDeclaredField("joinContext");
        privateJoinContextField.setAccessible(true);
        JoinContext joinContext = (JoinContext) privateJoinContextField.get(pageableResult);

        // TODO: chose SinglePageIterable for inner too, when we can page backwards
        assertThat(joinContext.innerIterable, instanceOf(CollectingPageableTaskIterable.class));
        assertThat(joinContext.outerIterable, instanceOf(SinglePagePageableTaskIterable.class));

        assertThat(TestingHelpers.printedPage(pageableResult.page()), is(
                "a| 1\n" +
                "a| 2\n" +
                "a| 3\n" +
                "b| 1\n"));

        pageInfo = pageInfo.nextPage(2);
        PageableTaskResult pageableTaskResult = pageableResult.fetch(pageInfo).get();
        assertThat(TestingHelpers.printedPage(pageableTaskResult.page()), is(
                "b| 2\n" +
                "b| 3\n"
        ));
        pageInfo = pageInfo.nextPage(2);
        pageableTaskResult = pageableTaskResult.fetch(pageInfo).get();
        assertThat(TestingHelpers.printedPage(pageableTaskResult.page()), is(
                "c| 1\n" +
                "c| 2\n"
        ));
        pageInfo = pageInfo.nextPage(2);
        pageableTaskResult = pageableTaskResult.fetch(pageInfo).get();
        assertThat(TestingHelpers.printedPage(pageableTaskResult.page()), is(
                "c| 3\n"
        ));

        assertThat(pageableTaskResult.fetch(pageInfo.nextPage()).get().page().size(), is(0L));

    }
}
