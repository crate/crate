/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.operation.projectors;

import com.google.common.collect.ImmutableList;
import io.crate.analyze.EvaluatingNormalizer;
import io.crate.analyze.symbol.AggregateMode;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.InputColumn;
import io.crate.analyze.symbol.Literal;
import io.crate.breaker.RamAccountingContext;
import io.crate.data.BatchConsumer;
import io.crate.data.BatchIterator;
import io.crate.data.RowsBatchIterator;
import io.crate.exceptions.UnhandledServerException;
import io.crate.executor.transport.TransportActionProvider;
import io.crate.metadata.Functions;
import io.crate.metadata.ReplaceMode;
import io.crate.metadata.RowGranularity;
import io.crate.operation.InputFactory;
import io.crate.operation.operator.EqOperator;
import io.crate.planner.projection.FilterProjection;
import io.crate.planner.projection.GroupProjection;
import io.crate.planner.projection.WriterProjection;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.TestingBatchConsumer;
import io.crate.types.DataTypes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Answers;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static io.crate.testing.TestingHelpers.getFunctions;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class ProjectingBatchConsumerTest extends CrateUnitTest {

    private static final RamAccountingContext RAM_ACCOUNTING_CONTEXT =
        new RamAccountingContext("dummy", new NoopCircuitBreaker(CircuitBreaker.FIELDDATA));
    private Functions functions;
    private ThreadPool threadPool;
    private ProjectorFactory projectorFactory;


    @Before
    public void prepare() {
        functions = getFunctions();
        threadPool = new TestThreadPool(Thread.currentThread().getName());
        projectorFactory = new ProjectionToProjectorVisitor(
            mock(ClusterService.class),
            functions,
            new IndexNameExpressionResolver(Settings.EMPTY),
            threadPool,
            Settings.EMPTY,
            mock(TransportActionProvider.class, Answers.RETURNS_DEEP_STUBS.get()),
            new InputFactory(functions),
            new EvaluatingNormalizer(
                functions,
                RowGranularity.SHARD,
                ReplaceMode.COPY,
                r -> Literal.of(r.valueType(), r.valueType().value("1")),
                null),
            null,
            new ShardId("dummy", UUID.randomUUID().toString(), 0)
        );
    }

    @After
    public void after() throws Exception {
        threadPool.shutdown();
        threadPool.awaitTermination(1, TimeUnit.SECONDS);
    }

    private static class DummyBatchConsumer implements BatchConsumer {

        private final boolean requiresScroll;

        DummyBatchConsumer(boolean requiresScroll) {
            this.requiresScroll = requiresScroll;
        }

        @Override
        public void accept(BatchIterator iterator, @Nullable Throwable failure) {
        }

        @Override
        public boolean requiresScroll() {
            return requiresScroll;
        }
    }


    @Test
    public void testConsumerRequiresScrollAndProjectorsDontSupportScrolling() throws Exception {
        EqOperator op =
            (EqOperator) functions.getBuiltin(EqOperator.NAME, ImmutableList.of(DataTypes.INTEGER, DataTypes.INTEGER));
        Function function = new Function(op.info(), Arrays.asList(Literal.of(2), new InputColumn(1)));
        FilterProjection filterProjection = new FilterProjection(function,
            Arrays.asList(new InputColumn(0), new InputColumn(1)));

        BatchConsumer delegateConsumerRequiresScroll = new DummyBatchConsumer(true);

        BatchConsumer projectingConsumer = ProjectingBatchConsumer.create(delegateConsumerRequiresScroll,
            Collections.singletonList(filterProjection), UUID.randomUUID(), RAM_ACCOUNTING_CONTEXT, projectorFactory);

        assertThat(projectingConsumer.requiresScroll(), is(true));
    }

    @Test
    public void testConsumerRequiresScrollAndProjectorsSupportScrolling() throws Exception {
        GroupProjection groupProjection = new GroupProjection(
            new ArrayList<>(), new ArrayList<>(), AggregateMode.ITER_FINAL, RowGranularity.DOC);

        BatchConsumer delegateConsumerRequiresScroll = new DummyBatchConsumer(true);

        BatchConsumer projectingConsumer = ProjectingBatchConsumer.create(delegateConsumerRequiresScroll,
            Collections.singletonList(groupProjection), UUID.randomUUID(), RAM_ACCOUNTING_CONTEXT, projectorFactory);

        assertThat(projectingConsumer.requiresScroll(), is(false));
    }

    @Test
    public void testConsumerDoesNotRequireScrollYieldsProjectingConsumerWithoutScrollRequirements() throws Exception {
        GroupProjection groupProjection = new GroupProjection(
            new ArrayList<>(), new ArrayList<>(), AggregateMode.ITER_FINAL, RowGranularity.DOC);
        BatchConsumer delegateConsumerRequiresScroll = new DummyBatchConsumer(false);

        BatchConsumer projectingConsumer = ProjectingBatchConsumer.create(delegateConsumerRequiresScroll,
            Collections.singletonList(groupProjection), UUID.randomUUID(), RAM_ACCOUNTING_CONTEXT, projectorFactory);

        assertThat(projectingConsumer.requiresScroll(), is(false));
    }

    @Test
    public void testErrorHandlingIfProjectorApplicationFails() throws Exception {
        WriterProjection writerProjection = new WriterProjection(
            Collections.singletonList(new InputColumn(0, DataTypes.STRING)),
            Literal.of("/x/y/z/hopefully/invalid/on/your/system/"),
            null,
            Collections.emptyMap(),
            Collections.emptyList(),
            WriterProjection.OutputFormat.JSON_OBJECT);

        TestingBatchConsumer consumer = new TestingBatchConsumer();
        BatchConsumer batchConsumer = ProjectingBatchConsumer.create(
            consumer,
            Collections.singletonList(writerProjection),
            UUID.randomUUID(),
            RAM_ACCOUNTING_CONTEXT,
            projectorFactory
        );

        batchConsumer.accept(RowsBatchIterator.empty(), null);

        expectedException.expect(UnhandledServerException.class);
        expectedException.expectMessage("Failed to open output");
        consumer.getResult();
    }
}
