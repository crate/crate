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

import com.google.common.base.Optional;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import io.crate.analyze.EvaluatingNormalizer;
import io.crate.breaker.RamAccountingContext;
import io.crate.core.collections.Bucket;
import io.crate.executor.QueryResult;
import io.crate.executor.TaskResult;
import io.crate.executor.transport.TransportActionProvider;
import io.crate.metadata.*;
import io.crate.operation.ImplementationSymbolVisitor;
import io.crate.operation.PageDownstream;
import io.crate.operation.aggregation.AggregationFunction;
import io.crate.operation.aggregation.impl.AggregationImplModule;
import io.crate.operation.aggregation.impl.MinimumAggregation;
import io.crate.operation.collect.StatsTables;
import io.crate.operation.PageDownstreamFactory;
import io.crate.operation.merge.NonSortingBucketMerger;
import io.crate.operation.projectors.FlatProjectorChain;
import io.crate.operation.projectors.ProjectionToProjectorVisitor;
import io.crate.operation.projectors.ResultProvider;
import io.crate.operation.projectors.TopN;
import io.crate.planner.RowGranularity;
import io.crate.planner.node.dql.MergeNode;
import io.crate.planner.projection.GroupProjection;
import io.crate.planner.projection.TopNProjection;
import io.crate.planner.symbol.Aggregation;
import io.crate.planner.symbol.InputColumn;
import io.crate.planner.symbol.Symbol;
import io.crate.test.integration.CrateUnitTest;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Answers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.*;

import static io.crate.testing.TestingHelpers.isRow;
import static org.hamcrest.Matchers.contains;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LocalMergeTaskTest extends CrateUnitTest {

    private ImplementationSymbolVisitor symbolVisitor;
    private GroupProjection groupProjection;
    private PageDownstreamFactory pageDownstreamFactory;
    private ThreadPool threadPool;

    @Before
    @SuppressWarnings("unchecked")
    public void prepare() {
        final Injector injector = new ModulesBuilder()
                .add(new AggregationImplModule())
                .add(new AbstractModule() {
                    @Override
                    protected void configure() {
                        bind(Client.class).toInstance(mock(Client.class));
                    }
                })
                .createInjector();
        final Functions functions = injector.getInstance(Functions.class);
        final ReferenceResolver referenceResolver = new GlobalReferenceResolver(
                Collections.<ReferenceIdent, ReferenceImplementation>emptyMap());
        symbolVisitor = new ImplementationSymbolVisitor(referenceResolver, functions, RowGranularity.CLUSTER);

        FunctionIdent minAggIdent = new FunctionIdent(MinimumAggregation.NAME, Arrays.<DataType>asList(DataTypes.DOUBLE));
        AggregationFunction<Double, Double> minAggFunction = (AggregationFunction) functions.get(minAggIdent);

        groupProjection = new GroupProjection();
        groupProjection.keys(Arrays.<Symbol>asList(new InputColumn(0, DataTypes.INTEGER)));
        groupProjection.values(Arrays.asList(
                new Aggregation(minAggFunction.info(), Arrays.<Symbol>asList(new InputColumn(1)), Aggregation.Step.PARTIAL, Aggregation.Step.FINAL)
        ));
        pageDownstreamFactory = mock(PageDownstreamFactory.class);
        when(pageDownstreamFactory.createMergeNodePageDownstream(any(MergeNode.class), any(ResultProvider.class), any(RamAccountingContext.class), any(Optional.class))).thenAnswer(new Answer<PageDownstream>() {
            @Override
            public PageDownstream answer(InvocationOnMock invocation) throws Throwable {
                NonSortingBucketMerger nonSortingBucketMerger = new NonSortingBucketMerger();
                MergeNode mergeNode = (MergeNode) invocation.getArguments()[0];
                ProjectionToProjectorVisitor projectionToProjectorVisitor = new ProjectionToProjectorVisitor(
                        mock(ClusterService.class),
                        ImmutableSettings.EMPTY,
                        mock(TransportActionProvider.class, Answers.RETURNS_DEEP_STUBS.get()),
                        symbolVisitor,
                        new EvaluatingNormalizer(functions, RowGranularity.CLUSTER, referenceResolver)
                );
                ResultProvider resultProvider = (ResultProvider)invocation.getArguments()[1];
                FlatProjectorChain projectorChain = new FlatProjectorChain(mergeNode.projections(), projectionToProjectorVisitor, mock(RamAccountingContext.class), Optional.of(resultProvider));
                nonSortingBucketMerger.downstream(projectorChain.firstProjector());
                projectorChain.startProjections();
                return nonSortingBucketMerger;
            }
        });
        threadPool = mock(ThreadPool.class);
        when(threadPool.executor(anyString())).thenReturn(MoreExecutors.sameThreadExecutor());
    }

    private ListenableFuture<TaskResult> getUpstreamResult(int numRows) {
        Object[][] rows = new Object[numRows][];
        for (int i = 0; i < numRows; i++) {
            double d = (double) numRows * i;
            rows[i] = new Object[]{
                    d % 4,
                    d
            };
        }
        return Futures.<TaskResult>immediateFuture(new QueryResult(rows));
    }

    @Test
    public void testLocalMerge() throws Exception {
        for (int run = 0; run < 100; run++) {
            TopNProjection topNProjection = new TopNProjection(3,
                    TopN.NO_OFFSET,
                    Arrays.<Symbol>asList(new InputColumn(0), new InputColumn(1)),
                    new boolean[]{true, true},
                    new Boolean[]{null, null});
            topNProjection.outputs(Arrays.<Symbol>asList(new InputColumn(0), new InputColumn(1)));

            MergeNode mergeNode = new MergeNode("merge", 2);
            mergeNode.projections(Arrays.asList(
                    groupProjection,
                    topNProjection
            ));
            List<ListenableFuture<TaskResult>> upstreamResults = new ArrayList<>(1);
            for (int i = 1; i < 14; i++) {
                upstreamResults.add(getUpstreamResult(i));
            }

            LocalMergeTask localMergeTask = new LocalMergeTask(
                    UUID.randomUUID(),
                    pageDownstreamFactory,
                    mergeNode,
                    mock(StatsTables.class),
                    new NoopCircuitBreaker(CircuitBreaker.Name.FIELDDATA), threadPool);
            localMergeTask.upstreamResult(upstreamResults);
            localMergeTask.start();

            ListenableFuture<List<TaskResult>> allAsList = Futures.allAsList(localMergeTask.result());
            Bucket result = allAsList.get().get(0).rows();

            assertThat(result, contains(
                    isRow(3.0, 3.0),
                    isRow(2.0, 2.0),
                    isRow(1.0, 5.0)
            ));
        }
    }
}
