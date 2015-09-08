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

package io.crate.operation.collect;

import com.google.common.collect.ImmutableList;
import io.crate.breaker.RamAccountingContext;
import io.crate.executor.transport.TransportActionProvider;
import io.crate.jobs.ExecutionState;
import io.crate.metadata.Functions;
import io.crate.metadata.MetaDataModule;
import io.crate.metadata.NestedReferenceResolver;
import io.crate.operation.ImplementationSymbolVisitor;
import io.crate.operation.RowDownstream;
import io.crate.operation.aggregation.impl.AggregationImplModule;
import io.crate.operation.aggregation.impl.CountAggregation;
import io.crate.operation.operator.OperatorModule;
import io.crate.testing.CollectingProjector;
import io.crate.operation.projectors.GroupingPipe;
import io.crate.operation.projectors.ProjectionToProjectorVisitor;
import io.crate.operation.projectors.SimpleTopNProjector;
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.planner.RowGranularity;
import io.crate.planner.projection.GroupProjection;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.TopNProjection;
import io.crate.planner.symbol.Aggregation;
import io.crate.planner.symbol.InputColumn;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Symbol;
import io.crate.test.integration.CrateUnitTest;
import io.crate.types.DataTypes;
import org.elasticsearch.action.admin.indices.template.put.TransportPutIndexTemplateAction;
import org.elasticsearch.action.bulk.BulkRetryCoordinatorPool;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.highlight.HighlightModule;
import org.elasticsearch.test.cluster.NoopClusterService;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static io.crate.operation.projectors.ProjectorAssertions.assertPipe;
import static io.crate.testing.TestingHelpers.newMockedThreadPool;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ShardProjectorChainTest extends CrateUnitTest {

    protected static final RamAccountingContext RAM_ACCOUNTING_CONTEXT =
            new RamAccountingContext("dummy", new NoopCircuitBreaker(CircuitBreaker.Name.FIELDDATA));

    private ProjectionToProjectorVisitor projectionToProjectorVisitor;
    private ThreadPool threadPool;

    private RowDownstream finalDownstream() {
        CollectingProjector projector = new CollectingProjector();
        projector.startProjection(mock(ExecutionState.class));
        return projector;
    }

    @Before
    public void prepare() {
        threadPool = newMockedThreadPool();
        ModulesBuilder builder = new ModulesBuilder().add(
                new ScalarFunctionModule(),
                new OperatorModule(),
                new AggregationImplModule(),
                new HighlightModule(),
                new TestModule(),
                new MetaDataModule());
        Injector injector = builder.createInjector();

        ImplementationSymbolVisitor implementationSymbolVisitor = new ImplementationSymbolVisitor(
                injector.getInstance(NestedReferenceResolver.class),
                injector.getInstance(Functions.class),
                RowGranularity.CLUSTER);
        projectionToProjectorVisitor = new ProjectionToProjectorVisitor(
                new NoopClusterService(),
                threadPool,
                ImmutableSettings.EMPTY,
                mock(TransportActionProvider.class),
                mock(BulkRetryCoordinatorPool.class),
                implementationSymbolVisitor,
                null
        );
    }

    private class TestModule extends AbstractModule {

        @Override
        protected void configure() {
            bind(ThreadPool.class).toInstance(threadPool);
            ClusterService clusterService = mock(ClusterService.class);
            ClusterState state = mock(ClusterState.class);
            MetaData metaData = mock(MetaData.class);
            when(metaData.concreteAllOpenIndices()).thenReturn(new String[0]);
            when(metaData.templates()).thenReturn(ImmutableOpenMap.<String, IndexTemplateMetaData>of());
            when(state.metaData()).thenReturn(metaData);
            when(clusterService.state()).thenReturn(state);
            bind(ClusterService.class).toInstance(clusterService);
            bind(TransportPutIndexTemplateAction.class).toInstance(mock(TransportPutIndexTemplateAction.class));
            bind(Settings.class).toInstance(ImmutableSettings.EMPTY);
        }
    }


    @After
    public void after() throws Exception {
        threadPool.shutdown();
        threadPool.awaitTermination(1, TimeUnit.SECONDS);
    }

    private Aggregation countAggregation() {
        return Aggregation.partialAggregation(
                CountAggregation.COUNT_STAR_FUNCTION,
                DataTypes.LONG,
                Collections.<Symbol>singletonList(new InputColumn(0)));
    }

    @Test
    public void testWithShardProjections() throws Exception {
        TopNProjection topN = new TopNProjection(0, 1);
        GroupProjection groupProjection = new GroupProjection(
                Arrays.<Symbol>asList(Literal.newLiteral(true)),
                Arrays.asList(countAggregation()));
        groupProjection.setRequiredGranularity(RowGranularity.SHARD);
        ShardProjectorChain chain = new ShardProjectorChain(
                UUID.randomUUID(),
                2,
                ImmutableList.of(groupProjection, topN),
                finalDownstream(),
                projectionToProjectorVisitor,
                RAM_ACCOUNTING_CONTEXT);

        assertThat(chain.nodeProjectors.size(), is(1));
        assertPipe(chain.nodeProjectors.get(0), SimpleTopNProjector.class);
        assertThat(chain.shardProjectors.size(), is(0));

        RowDownstream shardDownstream1 = chain.newShardDownstreamProjector(projectionToProjectorVisitor);
        RowDownstream shardDownstream2 = chain.newShardDownstreamProjector(projectionToProjectorVisitor);
        assertPipe(shardDownstream1, GroupingPipe.class);
        assertPipe(shardDownstream2, GroupingPipe.class);

        assertThat(chain.shardProjectors.size(), is(2));
    }

    @Test
    public void testWith2ShardProjections() throws Exception {
        TopNProjection topN = new TopNProjection(0, 1);
        GroupProjection groupProjection1 = new GroupProjection(
                Arrays.<Symbol>asList(Literal.newLiteral(true)),
                Arrays.asList(countAggregation()));
        groupProjection1.setRequiredGranularity(RowGranularity.SHARD);
        GroupProjection groupProjection2 = new GroupProjection(
                Arrays.<Symbol>asList(Literal.newLiteral(true)),
                Arrays.asList(countAggregation()));
        groupProjection2.setRequiredGranularity(RowGranularity.SHARD);
        ShardProjectorChain chain = new ShardProjectorChain(
                UUID.randomUUID(),
                2,
                ImmutableList.of(groupProjection1, groupProjection2, topN),
                finalDownstream(),
                projectionToProjectorVisitor,
                RAM_ACCOUNTING_CONTEXT);

        assertThat(chain.nodeProjectors.size(), is(2));
        assertPipe(chain.nodeProjectors.get(0), GroupingPipe.class);
        assertPipe(chain.nodeProjectors.get(1), SimpleTopNProjector.class);
        assertThat(chain.shardProjectors.size(), is(0));

        RowDownstream shardDownstream1 = chain.newShardDownstreamProjector(projectionToProjectorVisitor);
        RowDownstream shardDownstream2 = chain.newShardDownstreamProjector(projectionToProjectorVisitor);
        assertPipe(shardDownstream1, GroupingPipe.class);
        assertPipe(shardDownstream2, GroupingPipe.class);

        assertThat(chain.shardProjectors.size(), is(2));
    }

    @Test
    public void testWithoutShardProjections() throws Exception {
        TopNProjection topN = new TopNProjection(0, 1);
        GroupProjection groupProjection = new GroupProjection(
                Arrays.<Symbol>asList(Literal.newLiteral(true)),
                Arrays.asList(countAggregation()));
        ShardProjectorChain chain = new ShardProjectorChain(
                UUID.randomUUID(),
                2,
                ImmutableList.of(groupProjection, topN),
                finalDownstream(),
                projectionToProjectorVisitor,
                RAM_ACCOUNTING_CONTEXT);

        assertThat(chain.nodeProjectors.size(), is(2));
        assertPipe(chain.nodeProjectors.get(0), GroupingPipe.class);
        assertPipe(chain.nodeProjectors.get(1), SimpleTopNProjector.class);
        assertThat(chain.shardProjectors.size(), is(0));

        RowDownstream shardDownstream1 = chain.newShardDownstreamProjector(projectionToProjectorVisitor);
        RowDownstream shardDownstream2 = chain.newShardDownstreamProjector(projectionToProjectorVisitor);
        assertPipe(shardDownstream1, GroupingPipe.class);
        assertPipe(shardDownstream2, GroupingPipe.class);

        assertThat(chain.shardProjectors.size(), is(0));
    }

    @Test
    public void testZeroShards() throws Exception {
        TopNProjection topN = new TopNProjection(0, 1);
        GroupProjection groupProjection = new GroupProjection(
                Arrays.<Symbol>asList(Literal.newLiteral(true)),
                Arrays.asList(countAggregation()));
        ShardProjectorChain chain = new ShardProjectorChain(
                UUID.randomUUID(),
                0,
                ImmutableList.of(groupProjection, topN),
                finalDownstream(),
                projectionToProjectorVisitor,
                RAM_ACCOUNTING_CONTEXT);

        assertThat(chain.nodeProjectors.size(), is(2));
        assertPipe(chain.nodeProjectors.get(0), GroupingPipe.class);
        assertPipe(chain.nodeProjectors.get(1), SimpleTopNProjector.class);
        assertThat(chain.shardProjectors.size(), is(0));

        RowDownstream shardDownstream1 = chain.newShardDownstreamProjector(projectionToProjectorVisitor);
        RowDownstream shardDownstream2 = chain.newShardDownstreamProjector(projectionToProjectorVisitor);
        assertPipe(shardDownstream1, GroupingPipe.class);
        assertPipe(shardDownstream2, GroupingPipe.class);

        assertThat(chain.shardProjectors.size(), is(0));
    }

    @Test
    public void testOnlyShardProjections() throws Exception {
        GroupProjection groupProjection = new GroupProjection(
                Arrays.<Symbol>asList(Literal.newLiteral(true)),
                Arrays.asList(countAggregation()));
        groupProjection.setRequiredGranularity(RowGranularity.SHARD);
        ShardProjectorChain chain = new ShardProjectorChain(
                UUID.randomUUID(),
                0,
                ImmutableList.<Projection>of(groupProjection),
                finalDownstream(),
                projectionToProjectorVisitor,
                RAM_ACCOUNTING_CONTEXT);
        assertThat(chain.nodeProjectors.size(), is(0));
        assertThat(chain.shardProjectors.size(), is(0));

        RowDownstream shardDownstream1 = chain.newShardDownstreamProjector(projectionToProjectorVisitor);
        RowDownstream shardDownstream2 = chain.newShardDownstreamProjector(projectionToProjectorVisitor);
        assertPipe(shardDownstream1, GroupingPipe.class);
        assertPipe(shardDownstream2, GroupingPipe.class);
        assertThat(chain.shardProjectors.size(), is(2));
    }
}
