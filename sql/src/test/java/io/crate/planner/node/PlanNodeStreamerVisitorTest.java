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

package io.crate.planner.node;

import io.crate.metadata.FunctionIdent;
import io.crate.metadata.Functions;
import io.crate.metadata.Routing;
import io.crate.operator.aggregation.impl.AggregationImplModule;
import io.crate.operator.aggregation.impl.MaximumAggregation;
import io.crate.planner.projection.AggregationProjection;
import io.crate.planner.projection.Projection;
import io.crate.planner.symbol.Aggregation;
import io.crate.planner.symbol.InputColumn;
import io.crate.planner.symbol.Symbol;
import org.cratedb.DataType;
import org.cratedb.sql.CrateException;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class PlanNodeStreamerVisitorTest {

    private PlanNodeStreamerVisitor visitor;
    private FunctionIdent maxIdent;

    @Before
    public void prepare() {
        Injector injector = new ModulesBuilder().add(new AggregationImplModule()).createInjector();
        Functions functions = injector.getInstance(Functions.class);
        visitor = new PlanNodeStreamerVisitor(functions);
        maxIdent = new FunctionIdent(MaximumAggregation.NAME, Arrays.asList(DataType.INTEGER));
    }

    @Test
    public void testGetOutputStreamersFromCollectNode() throws Exception {
        CollectNode collectNode = new CollectNode("bla", new Routing(new HashMap<String, Map<String, Set<Integer>>>()));
        collectNode.outputTypes(Arrays.asList(DataType.BOOLEAN, DataType.FLOAT, DataType.OBJECT));
        PlanNodeStreamerVisitor.Context ctx = visitor.process(collectNode);
        DataType.Streamer<?>[] streamers = ctx.outputStreamers();
        assertThat(streamers.length, is(3));
        assertThat(streamers[0], instanceOf(DataType.BOOLEAN.streamer().getClass()));
        assertThat(streamers[1], instanceOf(DataType.FLOAT.streamer().getClass()));
        assertThat(streamers[2], instanceOf(DataType.OBJECT.streamer().getClass()));
    }

    @Test(expected= CrateException.class)
    public void testGetOutputStreamersFromCollectNodeWithWrongNull() throws Exception {
        // null means we expect an aggstate here
        CollectNode collectNode = new CollectNode("bla", new Routing(new HashMap<String, Map<String, Set<Integer>>>()));
        collectNode.outputTypes(Arrays.asList(DataType.BOOLEAN, null, DataType.OBJECT));
        visitor.process(collectNode);
    }

    @Test
    public void testGetOutputStreamersFromCollectNodeWithAggregations() throws Exception {
        CollectNode collectNode = new CollectNode("bla", new Routing(new HashMap<String, Map<String, Set<Integer>>>()));
        collectNode.outputTypes(Arrays.asList(DataType.BOOLEAN, null, null, DataType.DOUBLE));
        AggregationProjection aggregationProjection = new AggregationProjection();
        aggregationProjection.aggregations(Arrays.asList( // not a real use case, only for test convenience, sorry
                new Aggregation(maxIdent, Arrays.<Symbol>asList(new InputColumn(0)), Aggregation.Step.ITER, Aggregation.Step.FINAL),
                new Aggregation(maxIdent, Arrays.<Symbol>asList(new InputColumn(1)), Aggregation.Step.ITER, Aggregation.Step.PARTIAL)
        ));
        collectNode.projections(Arrays.<Projection>asList(aggregationProjection));
        PlanNodeStreamerVisitor.Context ctx = visitor.process(collectNode);
        DataType.Streamer<?>[] streamers = ctx.outputStreamers();
        assertThat(streamers.length, is(4));
        assertThat(streamers[0], instanceOf(DataType.BOOLEAN.streamer().getClass()));
        assertThat(streamers[1], instanceOf(DataType.INTEGER.streamer().getClass()));
        assertThat(streamers[2], instanceOf(AggregationStateStreamer.class));
        assertThat(streamers[3], instanceOf(DataType.DOUBLE.streamer().getClass()));
    }

    @Test
    public void testGetInputStreamersForMergeNode() throws Exception {
        MergeNode mergeNode = new MergeNode("mörtsch", 2);
        mergeNode.inputTypes(Arrays.asList(DataType.BOOLEAN, DataType.SHORT, DataType.TIMESTAMP));
        PlanNodeStreamerVisitor.Context ctx = visitor.process(mergeNode);
        DataType.Streamer<?>[] streamers = ctx.inputStreamers();
        assertThat(streamers.length, is(3));
        assertThat(streamers[0], instanceOf(DataType.BOOLEAN.streamer().getClass()));
        assertThat(streamers[1], instanceOf(DataType.SHORT.streamer().getClass()));
        assertThat(streamers[2], instanceOf(DataType.TIMESTAMP.streamer().getClass()));
    }

    @Test(expected= IllegalStateException.class)
    public void testGetInputStreamersForMergeNodeWithWrongNull() throws Exception {
        MergeNode mergeNode = new MergeNode("mörtsch", 2);
        mergeNode.inputTypes(Arrays.asList(DataType.BOOLEAN, null, DataType.TIMESTAMP));
        visitor.process(mergeNode);
    }

    @Test
    public void testGetInputStreamersForMergeNodeWithAggregations() throws Exception {
        MergeNode mergeNode = new MergeNode("mörtsch", 2);
        mergeNode.inputTypes(Arrays.asList(DataType.NULL, DataType.TIMESTAMP));
        AggregationProjection aggregationProjection = new AggregationProjection();
        aggregationProjection.aggregations(Arrays.asList(
                new Aggregation(maxIdent, Arrays.<Symbol>asList(new InputColumn(0)), Aggregation.Step.PARTIAL, Aggregation.Step.FINAL)
        ));
        mergeNode.projections(Arrays.<Projection>asList(aggregationProjection));
        PlanNodeStreamerVisitor.Context ctx = visitor.process(mergeNode);
        DataType.Streamer<?>[] streamers = ctx.inputStreamers();
        assertThat(streamers.length, is(2));
        assertThat(streamers[0], instanceOf(AggregationStateStreamer.class));
        assertThat(streamers[1], instanceOf(DataType.TIMESTAMP.streamer().getClass()));
    }
}
