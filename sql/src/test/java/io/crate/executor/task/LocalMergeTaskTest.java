/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.executor.task;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.crate.metadata.*;
import io.crate.operation.ImplementationSymbolVisitor;
import io.crate.operation.aggregation.AggregationFunction;
import io.crate.operation.aggregation.impl.AggregationImplModule;
import io.crate.operation.aggregation.impl.MinimumAggregation;
import io.crate.operation.collect.StatsTables;
import io.crate.operation.projectors.TopN;
import io.crate.planner.RowGranularity;
import io.crate.planner.node.dql.MergeNode;
import io.crate.planner.projection.GroupProjection;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.TopNProjection;
import io.crate.planner.symbol.Aggregation;
import io.crate.planner.symbol.InputColumn;
import io.crate.planner.symbol.Symbol;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.bulk.TransportShardBulkAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class LocalMergeTaskTest {

    private ImplementationSymbolVisitor symbolVisitor;
    private AggregationFunction<MinimumAggregation.MinimumAggState> minAggFunction;
    private GroupProjection groupProjection;
    private Injector injector;

    @Before
    @SuppressWarnings("unchecked")
    public void prepare() {
        injector = new ModulesBuilder()
                .add(new AggregationImplModule())
                .add(new AbstractModule() {
                    @Override
                    protected void configure() {
                        bind(Client.class).toInstance(mock(Client.class));
                    }
                })
                .createInjector();
        Functions functions = injector.getInstance(Functions.class);
        ReferenceResolver referenceResolver = new GlobalReferenceResolver(Collections.<ReferenceIdent, ReferenceImplementation>emptyMap());
        symbolVisitor = new ImplementationSymbolVisitor(referenceResolver, functions, RowGranularity.CLUSTER);

        FunctionIdent minAggIdent = new FunctionIdent(MinimumAggregation.NAME, Arrays.<DataType>asList(DataTypes.DOUBLE));
        minAggFunction = (AggregationFunction<MinimumAggregation.MinimumAggState>) functions.get(minAggIdent);

        groupProjection = new GroupProjection();
        groupProjection.keys(Arrays.<Symbol>asList(new InputColumn(0)));
        groupProjection.values(Arrays.asList(
                new Aggregation(minAggFunction.info(), Arrays.<Symbol>asList(new InputColumn(1)), Aggregation.Step.PARTIAL, Aggregation.Step.FINAL)
        ));
    }

    private ListenableFuture<Object[][]> getUpstreamResult(int numRows) {
        Object[][] resultRows = new Object[numRows][];
        for (int i=0; i<numRows; i++) {
            double d = (double)numRows*i;
            MinimumAggregation.MinimumAggState aggState = minAggFunction.newState();
            aggState.setValue(d);
            resultRows[i] = new Object[]{
                    d % 4,
                    aggState
            };
        }
        return Futures.immediateFuture(resultRows);
    }

    @Test
    public void testLocalMerge() throws Exception {
        for (int run = 0; run < 100; run++) {
            TopNProjection topNProjection = new TopNProjection(3,
                    TopN.NO_OFFSET,
                    Arrays.<Symbol>asList(new InputColumn(0), new InputColumn(1)),
                    new boolean[]{true, true},
                    new Boolean[] { null, null });
            topNProjection.outputs(Arrays.<Symbol>asList(new InputColumn(0), new InputColumn(1)));

            MergeNode mergeNode = new MergeNode("merge", 2);
            mergeNode.projections(Arrays.<Projection>asList(
                    groupProjection,
                    topNProjection
            ));
            List<ListenableFuture<Object[][]>> upstreamResults = new ArrayList<>(1);
            for (int i=1; i<14; i++) {
                upstreamResults.add(getUpstreamResult(i));
            }

            ThreadPool threadPool = new ThreadPool();

            LocalMergeTask localMergeTask = new LocalMergeTask(
                    threadPool,
                    mock(ClusterService.class),
                    ImmutableSettings.EMPTY,
                    mock(TransportShardBulkAction.class),
                    mock(TransportCreateIndexAction.class),
                    symbolVisitor, mergeNode,
                    mock(StatsTables.class));
            localMergeTask.upstreamResult(upstreamResults);
            localMergeTask.start();

            ListenableFuture<List<Object[][]>> allAsList = Futures.allAsList(localMergeTask.result());
            Object[][] result = allAsList.get().get(0);

            assertThat(result.length, is(3));
            assertThat(result[0].length, is(2));

            assertThat((Double)result[0][0], is(3.0));
            assertThat((Double)result[0][1], is(3.0));

            assertThat((Double)result[1][0], is(2.0));
            assertThat((Double)result[1][1], is(2.0));

            assertThat((Double)result[2][0], is(1.0));
            assertThat((Double)result[2][1], is(5.0));
        }
    }
}
