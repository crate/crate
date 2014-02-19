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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.metadata.*;
import io.crate.operator.aggregation.AggregationFunction;
import io.crate.operator.aggregation.impl.AggregationImplModule;
import io.crate.operator.aggregation.impl.MinimumAggregation;
import io.crate.operator.operations.ImplementationSymbolVisitor;
import io.crate.operator.projectors.TopN;
import io.crate.planner.RowGranularity;
import io.crate.planner.node.MergeNode;
import io.crate.planner.projection.GroupProjection;
import io.crate.planner.projection.TopNProjection;
import io.crate.planner.symbol.Aggregation;
import io.crate.planner.symbol.InputColumn;
import io.crate.planner.symbol.Symbol;
import org.cratedb.DataType;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class LocalMergeTaskTest {

    private ImplementationSymbolVisitor symbolVisitor;
    private AggregationFunction<MinimumAggregation.MinimumAggState<Double>> minAggFunction;
    private GroupProjection groupProjection;

    @Before
    @SuppressWarnings("unchecked")
    public void prepare() {
        Injector injector = new ModulesBuilder().add(new AggregationImplModule()).createInjector();
        Functions functions = injector.getInstance(Functions.class);
        ReferenceResolver referenceResolver = new GlobalReferenceResolver(Collections.<ReferenceIdent, ReferenceImplementation>emptyMap());
        symbolVisitor = new ImplementationSymbolVisitor(referenceResolver, functions, RowGranularity.CLUSTER);

        FunctionIdent minAggIdent = new FunctionIdent(MinimumAggregation.NAME, Arrays.asList(DataType.DOUBLE));
        minAggFunction = (AggregationFunction<MinimumAggregation.MinimumAggState<Double>>) functions.get(minAggIdent);

        groupProjection = new GroupProjection();
        groupProjection.keys(Arrays.<Symbol>asList(new InputColumn(0)));
        groupProjection.values(Arrays.asList(
                new Aggregation(minAggFunction.info(), Arrays.<Symbol>asList(new InputColumn(1)), Aggregation.Step.PARTIAL, Aggregation.Step.FINAL)
        ));
    }

    private ListenableFuture<Object[][]> getUpstreamResult(int numRows) {
        SettableFuture<Object[][]> upstreamResult = SettableFuture.create();
        Object[][] resultRows = new Object[numRows][];
        for (int i=0; i<numRows; i++) {
            double d = (double)numRows*i;
            MinimumAggregation.MinimumAggState<Double> aggState = minAggFunction.newState();
            aggState.setValue(d);
            resultRows[i] = new Object[]{
                    d%4,
                    aggState
            };
        }
        upstreamResult.set(resultRows);
        return upstreamResult;
    }

    @Test
    public void testLocalMerge() throws Exception {
        TopNProjection topNProjection = new TopNProjection(3, TopN.NO_OFFSET, Arrays.<Symbol>asList(new InputColumn(0), new InputColumn(1)), new boolean[]{true, true});
        topNProjection.outputs(Arrays.<Symbol>asList(new InputColumn(0), new InputColumn(1)));

        MergeNode mergeNode = new MergeNode("merge", 2);
        mergeNode.projections(Arrays.asList(
                groupProjection,
                topNProjection
        ));
        List<ListenableFuture<Object[][]>> upstreamResults = new ArrayList<>(1);
        for (int i=1; i<14; i++) {
            upstreamResults.add(getUpstreamResult(i));
        }

        ThreadPool threadPool = new ThreadPool();

        LocalMergeTask localMergeTask = new LocalMergeTask(threadPool, symbolVisitor, mergeNode);
        localMergeTask.upstreamResult(upstreamResults);
        localMergeTask.start();

        List<ListenableFuture<Object[][]>> localMergeResults = localMergeTask.result();

        assertThat(localMergeResults.size(), is(1));
        Object[][] result = localMergeResults.get(0).get();
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
