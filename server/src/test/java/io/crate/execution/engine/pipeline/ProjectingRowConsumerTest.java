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

package io.crate.execution.engine.pipeline;

import io.crate.breaker.RamAccounting;
import io.crate.data.BatchIterator;
import io.crate.data.InMemoryBatchIterator;
import io.crate.data.Row;
import io.crate.data.RowConsumer;
import io.crate.exceptions.UnhandledServerException;
import io.crate.execution.TransportActionProvider;
import io.crate.execution.dsl.projection.FilterProjection;
import io.crate.execution.dsl.projection.GroupProjection;
import io.crate.execution.dsl.projection.WriterProjection;
import io.crate.execution.jobs.NodeJobsCounter;
import io.crate.expression.InputFactory;
import io.crate.expression.eval.EvaluatingNormalizer;
import io.crate.expression.operator.EqOperator;
import io.crate.expression.symbol.AggregateMode;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.memory.OnHeapMemoryManager;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.Functions;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.SearchPath;
import io.crate.metadata.TransactionContext;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.TestingRowConsumer;
import io.crate.types.DataTypes;
import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Answers;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static io.crate.data.SentinelRow.SENTINEL;
import static io.crate.testing.TestingHelpers.getFunctions;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class ProjectingRowConsumerTest extends CrateDummyClusterServiceUnitTest {

    private Functions functions;
    private ProjectorFactory projectorFactory;
    private TransactionContext txnCtx = CoordinatorTxnCtx.systemTransactionContext();
    private OnHeapMemoryManager memoryManager;


    @Before
    public void prepare() {
        functions = getFunctions();
        memoryManager = new OnHeapMemoryManager(usedBytes -> {});
        projectorFactory = new ProjectionToProjectorVisitor(
            clusterService,
            new NodeJobsCounter(),
            functions,
            THREAD_POOL,
            Settings.EMPTY,
            mock(TransportActionProvider.class, Answers.RETURNS_DEEP_STUBS),
            new InputFactory(functions),
            new EvaluatingNormalizer(
                functions,
                RowGranularity.SHARD,
                r -> Literal.ofUnchecked(r.valueType(), r.valueType().value("1")),
                null),
            t -> null,
            t -> null,
            Version.CURRENT,
            new ShardId("dummy", UUID.randomUUID().toString(), 0)
        );
    }

    private static class DummyRowConsumer implements RowConsumer {

        private final boolean requiresScroll;

        DummyRowConsumer(boolean requiresScroll) {
            this.requiresScroll = requiresScroll;
        }

        @Override
        public void accept(BatchIterator<Row> iterator, @Nullable Throwable failure) {
        }

        @Override
        public CompletableFuture<?> completionFuture() {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public boolean requiresScroll() {
            return requiresScroll;
        }
    }

    @Test
    public void testConsumerRequiresScrollAndProjectorsDontSupportScrolling() {
        List<Symbol> arguments = Arrays.asList(Literal.of(2), new InputColumn(1, DataTypes.INTEGER));
        EqOperator op =
            (EqOperator) functions.get(null, EqOperator.NAME, arguments, SearchPath.pathWithPGCatalogAndDoc());
        Function function = new Function(op.info(), arguments);
        FilterProjection filterProjection = new FilterProjection(function,
            Arrays.asList(new InputColumn(0), new InputColumn(1)));

        RowConsumer delegateConsumerRequiresScroll = new DummyRowConsumer(true);

        RowConsumer projectingConsumer = ProjectingRowConsumer.create(
            delegateConsumerRequiresScroll,
            Collections.singletonList(filterProjection),
            UUID.randomUUID(),
            txnCtx,
            RamAccounting.NO_ACCOUNTING,
            memoryManager,
            projectorFactory
        );

        assertThat(projectingConsumer.requiresScroll(), is(true));
    }

    @Test
    public void testConsumerRequiresScrollAndProjectorsSupportScrolling() {
        GroupProjection groupProjection = new GroupProjection(
            new ArrayList<>(), new ArrayList<>(), AggregateMode.ITER_FINAL, RowGranularity.SHARD);

        RowConsumer delegateConsumerRequiresScroll = new DummyRowConsumer(true);

        RowConsumer projectingConsumer = ProjectingRowConsumer.create(
            delegateConsumerRequiresScroll,
            Collections.singletonList(groupProjection),
            UUID.randomUUID(),
            txnCtx,
            RamAccounting.NO_ACCOUNTING,
            memoryManager,
            projectorFactory
        );

        assertThat(projectingConsumer.requiresScroll(), is(false));
    }

    @Test
    public void testConsumerDoesNotRequireScrollYieldsProjectingConsumerWithoutScrollRequirements() {
        GroupProjection groupProjection = new GroupProjection(
            new ArrayList<>(), new ArrayList<>(), AggregateMode.ITER_FINAL, RowGranularity.DOC);
        RowConsumer delegateConsumerRequiresScroll = new DummyRowConsumer(false);

        RowConsumer projectingConsumer = ProjectingRowConsumer.create(
            delegateConsumerRequiresScroll,
            Collections.singletonList(groupProjection),
            UUID.randomUUID(),
            txnCtx,
            RamAccounting.NO_ACCOUNTING,
            memoryManager,
            projectorFactory
        );

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

        TestingRowConsumer consumer = new TestingRowConsumer();
        RowConsumer rowConsumer = ProjectingRowConsumer.create(
            consumer,
            Collections.singletonList(writerProjection),
            UUID.randomUUID(),
            txnCtx,
            RamAccounting.NO_ACCOUNTING,
            memoryManager,
            projectorFactory
        );

        rowConsumer.accept(InMemoryBatchIterator.empty(SENTINEL), null);

        expectedException.expect(UnhandledServerException.class);
        expectedException.expectMessage("Failed to open output");
        consumer.getResult();
    }
}
