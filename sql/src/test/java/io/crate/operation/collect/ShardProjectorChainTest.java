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
import io.crate.executor.transport.TransportActionProvider;
import io.crate.metadata.Functions;
import io.crate.metadata.MetaDataModule;
import io.crate.metadata.ReferenceResolver;
import io.crate.operation.ImplementationSymbolVisitor;
import io.crate.operation.aggregation.impl.AggregationImplModule;
import io.crate.operation.aggregation.impl.CountAggregation;
import io.crate.operation.operator.OperatorModule;
import io.crate.operation.projectors.*;
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.planner.RowGranularity;
import io.crate.planner.projection.GroupProjection;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.TopNProjection;
import io.crate.planner.symbol.Aggregation;
import io.crate.planner.symbol.InputColumn;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Symbol;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.highlight.HighlightModule;
import org.elasticsearch.test.cluster.NoopClusterService;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;

public class ShardProjectorChainTest {

    private ProjectionToProjectorVisitor projectionToProjectorVisitor;

    private class TestModule extends AbstractModule {

        @Override
        protected void configure() {
            bind(Settings.class).toInstance(ImmutableSettings.EMPTY);

        }
    }

    @Before
    public void prepare() {
        ModulesBuilder builder = new ModulesBuilder().add(
                new ScalarFunctionModule(),
                new OperatorModule(),
                new AggregationImplModule(),
                new HighlightModule(),
                new TestModule(),
                new MetaDataModule());
        Injector injector = builder.createInjector();

        ImplementationSymbolVisitor implementationSymbolVisitor = new ImplementationSymbolVisitor(
                injector.getInstance(ReferenceResolver.class),
                injector.getInstance(Functions.class),
                RowGranularity.CLUSTER);
        projectionToProjectorVisitor = new ProjectionToProjectorVisitor(
                new NoopClusterService(),
                ImmutableSettings.EMPTY,
                mock(TransportActionProvider.class),
                implementationSymbolVisitor
        );
    }

    private Aggregation countAggregation() {
        return new Aggregation(
                CountAggregation.COUNT_STAR_FUNCTION,
                Arrays.<Symbol>asList(new InputColumn(0)),
                Aggregation.Step.ITER,
                Aggregation.Step.PARTIAL);
    }

    @Test
    public void testWithShardProjections() throws Exception {
        TopNProjection topN = new TopNProjection(0, 1);
        GroupProjection groupProjection = new GroupProjection(
                Arrays.<Symbol>asList(Literal.newLiteral(true)),
                Arrays.asList(countAggregation()));
        groupProjection.setRequiredGranularity(RowGranularity.SHARD);
        ShardProjectorChain chain = new ShardProjectorChain(2, ImmutableList.of(groupProjection, topN), projectionToProjectorVisitor);

        assertThat(chain.nodeProjectors.size(), is(1));
        assertThat(chain.nodeProjectors.get(0), is(instanceOf(SimpleTopNProjector.class)));
        assertThat(chain.shardProjectors.size(), is(0));

        Projector projector1 = chain.newShardDownstreamProjector(projectionToProjectorVisitor);
        Projector projector2 = chain.newShardDownstreamProjector(projectionToProjectorVisitor);
        assertThat(projector1, is(instanceOf(GroupingProjector.class)));
        assertThat(projector2, is(instanceOf(GroupingProjector.class)));

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
        ShardProjectorChain chain = new ShardProjectorChain(2, ImmutableList.of(groupProjection1, groupProjection2, topN), projectionToProjectorVisitor);

        assertThat(chain.nodeProjectors.size(), is(2));
        assertThat(chain.nodeProjectors.get(0), is(instanceOf(GroupingProjector.class)));
        assertThat(chain.nodeProjectors.get(1), is(instanceOf(SimpleTopNProjector.class)));
        assertThat(chain.shardProjectors.size(), is(0));

        Projector projector1 = chain.newShardDownstreamProjector(projectionToProjectorVisitor);
        Projector projector2 = chain.newShardDownstreamProjector(projectionToProjectorVisitor);
        assertThat(projector1, is(instanceOf(GroupingProjector.class)));
        assertThat(projector2, is(instanceOf(GroupingProjector.class)));

        assertThat(chain.shardProjectors.size(), is(2));
    }

    @Test
    public void testWithoutShardProjections() throws Exception {
        TopNProjection topN = new TopNProjection(0, 1);
        GroupProjection groupProjection = new GroupProjection(
                Arrays.<Symbol>asList(Literal.newLiteral(true)),
                Arrays.asList(countAggregation()));
        ShardProjectorChain chain = new ShardProjectorChain(2, ImmutableList.of(groupProjection, topN), projectionToProjectorVisitor);

        assertThat(chain.nodeProjectors.size(), is(2));
        assertThat(chain.nodeProjectors.get(0), is(instanceOf(GroupingProjector.class)));
        assertThat(chain.nodeProjectors.get(1), is(instanceOf(SimpleTopNProjector.class)));
        assertThat(chain.shardProjectors.size(), is(0));

        Projector projector1 = chain.newShardDownstreamProjector(projectionToProjectorVisitor);
        Projector projector2 = chain.newShardDownstreamProjector(projectionToProjectorVisitor);
        assertThat(projector1, is(instanceOf(GroupingProjector.class)));
        assertThat(projector2, is(instanceOf(GroupingProjector.class)));

        assertThat(chain.shardProjectors.size(), is(0));
    }

    @Test
    public void testNoProjections() throws Exception {
        ShardProjectorChain chain = new ShardProjectorChain(2, ImmutableList.<Projection>of(), projectionToProjectorVisitor);

        assertThat(chain.nodeProjectors.size(), is(1));
        assertThat(chain.nodeProjectors.get(0), is(instanceOf(CollectingProjector.class)));
        assertThat(chain.shardProjectors.size(), is(0));

        Projector projector1 = chain.newShardDownstreamProjector(projectionToProjectorVisitor);
        Projector projector2 = chain.newShardDownstreamProjector(projectionToProjectorVisitor);
        assertThat(projector1, is(instanceOf(CollectingProjector.class)));
        assertThat(projector2, is(instanceOf(CollectingProjector.class)));

        assertThat(chain.shardProjectors.size(), is(0));

    }

    @Test
    public void testZeroShards() throws Exception {
        TopNProjection topN = new TopNProjection(0, 1);
        GroupProjection groupProjection = new GroupProjection(
                Arrays.<Symbol>asList(Literal.newLiteral(true)),
                Arrays.asList(countAggregation()));
        ShardProjectorChain chain = new ShardProjectorChain(0, ImmutableList.of(groupProjection, topN), projectionToProjectorVisitor);

        assertThat(chain.nodeProjectors.size(), is(2));
        assertThat(chain.nodeProjectors.get(0), is(instanceOf(GroupingProjector.class)));
        assertThat(chain.nodeProjectors.get(1), is(instanceOf(SimpleTopNProjector.class)));
        assertThat(chain.shardProjectors.size(), is(0));

        Projector projector1 = chain.newShardDownstreamProjector(projectionToProjectorVisitor);
        Projector projector2 = chain.newShardDownstreamProjector(projectionToProjectorVisitor);
        assertThat(projector1, is(instanceOf(GroupingProjector.class)));
        assertThat(projector2, is(instanceOf(GroupingProjector.class)));

        assertThat(chain.shardProjectors.size(), is(0));
    }
}
