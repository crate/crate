/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.execution.engine.pipeline;

import static io.crate.data.SentinelRow.SENTINEL;
import static io.crate.testing.TestingHelpers.createNodeContext;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.elasticsearch.Version;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.jetbrains.annotations.Nullable;
import org.junit.Before;
import org.junit.Test;

import io.crate.data.BatchIterator;
import io.crate.data.InMemoryBatchIterator;
import io.crate.data.Row;
import io.crate.data.RowConsumer;
import io.crate.data.breaker.RamAccounting;
import io.crate.data.testing.TestingRowConsumer;
import io.crate.exceptions.UnhandledServerException;
import io.crate.execution.dsl.projection.FilterProjection;
import io.crate.execution.dsl.projection.GroupProjection;
import io.crate.execution.dsl.projection.WriterProjection;
import io.crate.execution.engine.export.LocalFsFileOutputFactory;
import io.crate.execution.jobs.NodeLimits;
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
import io.crate.metadata.NodeContext;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.SearchPath;
import io.crate.metadata.TransactionContext;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.types.DataTypes;

public class ProjectingRowConsumerTest extends CrateDummyClusterServiceUnitTest {

    private ProjectorFactory projectorFactory;
    private TransactionContext txnCtx = CoordinatorTxnCtx.systemTransactionContext();
    private NodeContext nodeCtx;
    private OnHeapMemoryManager memoryManager;


    @Before
    public void prepare() {
        nodeCtx = createNodeContext();
        memoryManager = new OnHeapMemoryManager(usedBytes -> {});
        projectorFactory = new ProjectionToProjectorVisitor(
            clusterService,
            new NodeLimits(new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)),
            new NoneCircuitBreakerService(),
            nodeCtx,
            THREAD_POOL,
            Settings.EMPTY,
            mock(ElasticsearchClient.class),
            new InputFactory(nodeCtx),
            new EvaluatingNormalizer(
                nodeCtx,
                RowGranularity.SHARD,
                r -> Literal.ofUnchecked(r.valueType(), r.valueType().implicitCast("1")),
                null),
            t -> null,
            t -> null,
            Version.CURRENT,
            new ShardId("dummy", UUID.randomUUID().toString(), 0),
            Map.of(LocalFsFileOutputFactory.NAME, new LocalFsFileOutputFactory())
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
            (EqOperator) nodeCtx.functions().get(null, EqOperator.NAME, arguments, SearchPath.pathWithPGCatalogAndDoc());
        Function function = new Function(op.signature(), arguments, EqOperator.RETURN_TYPE);
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

        assertThat(projectingConsumer.requiresScroll()).isTrue();
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

        assertThat(projectingConsumer.requiresScroll()).isFalse();
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

        assertThat(projectingConsumer.requiresScroll()).isFalse();
    }

    @Test
    public void testErrorHandlingIfProjectorApplicationFails() throws Exception {
        WriterProjection writerProjection = new WriterProjection(
                Collections.singletonList(new InputColumn(0, DataTypes.STRING)),
                Literal.of("/x/y/z/hopefully/invalid/on/your/system/"),
                null,
                Collections.emptyMap(),
                Collections.emptyList(),
                WriterProjection.OutputFormat.JSON_OBJECT,
                Settings.EMPTY);

        TestingRowConsumer consumer = new TestingRowConsumer();
        RowConsumer rowConsumer = ProjectingRowConsumer.create(
                consumer,
                Collections.singletonList(writerProjection),
                UUID.randomUUID(),
                txnCtx,
                RamAccounting.NO_ACCOUNTING,
                memoryManager,
                projectorFactory);

        rowConsumer.accept(InMemoryBatchIterator.empty(SENTINEL), null);

        assertThatThrownBy(() -> consumer.getResult())
            .isExactlyInstanceOf(UnhandledServerException.class)
            .hasMessageStartingWith("Failed to open output");
    }
}
