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

import com.google.common.collect.ImmutableList;
import io.crate.Streamer;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Functions;
import io.crate.metadata.Routing;
import io.crate.operation.aggregation.impl.AggregationImplModule;
import io.crate.operation.aggregation.impl.CountAggregation;
import io.crate.operation.aggregation.impl.MaximumAggregation;
import io.crate.planner.node.dql.CollectNode;
import io.crate.planner.node.dql.MergeNode;
import io.crate.planner.projection.AggregationProjection;
import io.crate.planner.projection.GroupProjection;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.TopNProjection;
import io.crate.planner.symbol.Aggregation;
import io.crate.planner.symbol.InputColumn;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Symbol;
import io.crate.test.integration.CrateUnitTest;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class StreamerVisitorTest extends CrateUnitTest {

    private StreamerVisitor visitor;
    private FunctionInfo maxInfo;
    private FunctionInfo countInfo;

    final static Routing EMPTY_ROUTING = new Routing(new TreeMap<String, Map<String, List<Integer>>>());

    @Before
    public void prepare() {
        Injector injector = new ModulesBuilder()
                .add(new AggregationImplModule())
                .createInjector();
        Functions functions = injector.getInstance(Functions.class);
        visitor = new StreamerVisitor(functions);
        maxInfo = new FunctionInfo(new FunctionIdent(MaximumAggregation.NAME, Arrays.<DataType>asList(DataTypes.INTEGER)), DataTypes.INTEGER);
        countInfo = new FunctionInfo(new FunctionIdent(CountAggregation.NAME, ImmutableList.<DataType>of()), DataTypes.LONG);
    }

    @Test
    public void testGetOutputStreamersFromCollectNode() throws Exception {
        CollectNode collectNode = new CollectNode(0, "bla", EMPTY_ROUTING);
        collectNode.outputTypes(Arrays.<DataType>asList(DataTypes.BOOLEAN, DataTypes.FLOAT, DataTypes.OBJECT));
        StreamerVisitor.Context ctx = visitor.processPlanNode(collectNode);
        Streamer<?>[] streamers = ctx.outputStreamers();
        assertThat(streamers.length, is(3));
        assertThat(streamers[0], instanceOf(DataTypes.BOOLEAN.streamer().getClass()));
        assertThat(streamers[1], instanceOf(DataTypes.FLOAT.streamer().getClass()));
        assertThat(streamers[2], instanceOf(DataTypes.OBJECT.streamer().getClass()));

        // as executionNode
        ctx = visitor.processExecutionNode(collectNode);
        streamers = ctx.outputStreamers();
        assertThat(streamers.length, is(3));
        assertThat(streamers[0], instanceOf(DataTypes.BOOLEAN.streamer().getClass()));
        assertThat(streamers[1], instanceOf(DataTypes.FLOAT.streamer().getClass()));
        assertThat(streamers[2], instanceOf(DataTypes.OBJECT.streamer().getClass()));
    }

    @Test
    public void testGetOutputStreamersFromCollectNodeWithWrongNull() throws Exception {
        // null means we expect an aggstate here
        CollectNode collectNode = new CollectNode(0, "bla", EMPTY_ROUTING);
        collectNode.outputTypes(Arrays.<DataType>asList(DataTypes.BOOLEAN, null, DataTypes.OBJECT));
        StreamerVisitor.Context ctx = visitor.processPlanNode(collectNode);
        // assume an unknown column
        assertEquals(DataTypes.UNDEFINED.streamer(), ctx.outputStreamers()[1]);

        // as executionNode
        ctx = visitor.processExecutionNode(collectNode);
        // assume an unknown column
        assertEquals(DataTypes.UNDEFINED.streamer(), ctx.outputStreamers()[1]);
    }

    @Test
    public void testGetOutputStreamersFromCollectNodeWithAggregations() throws Exception {
        CollectNode collectNode = new CollectNode(0, "bla", EMPTY_ROUTING);
        collectNode.outputTypes(Arrays.<DataType>asList(DataTypes.BOOLEAN, null, null, DataTypes.DOUBLE));
        AggregationProjection aggregationProjection = new AggregationProjection();
        aggregationProjection.aggregations(Arrays.asList( // not a real use case, only for test convenience, sorry
                new Aggregation(maxInfo, Arrays.<Symbol>asList(new InputColumn(0)), Aggregation.Step.ITER, Aggregation.Step.FINAL),
                new Aggregation(maxInfo, Arrays.<Symbol>asList(new InputColumn(1)), Aggregation.Step.ITER, Aggregation.Step.PARTIAL)
        ));
        collectNode.projections(Arrays.<Projection>asList(aggregationProjection));
        StreamerVisitor.Context ctx = visitor.processPlanNode(collectNode);
        Streamer<?>[] streamers = ctx.outputStreamers();
        assertThat(streamers.length, is(4));
        assertThat(streamers[0], instanceOf(DataTypes.BOOLEAN.streamer().getClass()));
        assertThat(streamers[1], instanceOf(DataTypes.INTEGER.streamer().getClass()));
        assertThat(streamers[2], instanceOf(DataTypes.INTEGER.streamer().getClass()));
        assertThat(streamers[3], instanceOf(DataTypes.DOUBLE.streamer().getClass()));
    }

    @Test
    public void testGetOutputStreamersFromCollectNodeWithGroupAndTopNProjection() throws Exception {
        CollectNode collectNode = new CollectNode(0, "mynode", EMPTY_ROUTING);
        collectNode.outputTypes(Arrays.<DataType>asList(DataTypes.UNDEFINED));
        GroupProjection groupProjection = new GroupProjection(
                Arrays.<Symbol>asList(Literal.newLiteral("key")),
                Arrays.asList(new Aggregation(
                        countInfo,
                        ImmutableList.<Symbol>of(),
                        Aggregation.Step.PARTIAL, Aggregation.Step.FINAL))
        );
        collectNode.projections(Arrays.<Projection>asList(groupProjection, new TopNProjection(10,0)));
        StreamerVisitor.Context ctx = visitor.processPlanNode(collectNode);
        Streamer<?>[] streamers = ctx.outputStreamers();
        assertThat(streamers.length, is(1));
        assertThat(streamers[0], instanceOf(DataTypes.LONG.streamer().getClass()));
    }

    @Test
    public void testGetInputStreamersForMergeNode() throws Exception {
        MergeNode mergeNode = new MergeNode(0, "mörtsch", 2);
        mergeNode.inputTypes(Arrays.<DataType>asList(DataTypes.BOOLEAN, DataTypes.SHORT, DataTypes.TIMESTAMP));
        StreamerVisitor.Context ctx = visitor.processPlanNode(mergeNode);
        Streamer<?>[] streamers = ctx.inputStreamers();
        assertThat(streamers.length, is(3));
        assertThat(streamers[0], instanceOf(DataTypes.BOOLEAN.streamer().getClass()));
        assertThat(streamers[1], instanceOf(DataTypes.SHORT.streamer().getClass()));
        assertThat(streamers[2], instanceOf(DataTypes.TIMESTAMP.streamer().getClass()));
    }

    @Test(expected= IllegalStateException.class)
    public void testGetInputStreamersForMergeNodeWithWrongNull() throws Exception {
        MergeNode mergeNode = new MergeNode(0, "mörtsch", 2);
        mergeNode.inputTypes(Arrays.<DataType>asList(DataTypes.BOOLEAN, null, DataTypes.TIMESTAMP));
        visitor.processPlanNode(mergeNode);
    }

    @Test
    public void testGetInputStreamersForMergeNodeWithAggregations() throws Exception {
        MergeNode mergeNode = new MergeNode(0, "mörtsch", 2);
        mergeNode.inputTypes(Arrays.<DataType>asList(DataTypes.UNDEFINED, DataTypes.TIMESTAMP));
        AggregationProjection aggregationProjection = new AggregationProjection();
        aggregationProjection.aggregations(Arrays.asList(
                new Aggregation(maxInfo, Arrays.<Symbol>asList(new InputColumn(0)), Aggregation.Step.PARTIAL, Aggregation.Step.FINAL)
        ));
        mergeNode.projections(Arrays.<Projection>asList(aggregationProjection));
        StreamerVisitor.Context ctx = visitor.processPlanNode(mergeNode);
        Streamer<?>[] streamers = ctx.inputStreamers();
        assertThat(streamers.length, is(2));
        assertThat(streamers[0], instanceOf(DataTypes.INTEGER.streamer().getClass()));
        assertThat(streamers[1], instanceOf(DataTypes.TIMESTAMP.streamer().getClass()));
    }

    @Test
    public void testOutputStreamerFromGroupByMergeNode() throws Exception {
        /**
         * select count(*), name ... group by name limit 2
         *
         * the groupProjection has  the outputs
         *      name, count(*)
         *
         * the topN projection swaps the outputs to
         *
         *      count(*), name
         *
         * so the streamers have to be
         *
         *      longStreamer,  stringStreamer
         */

        MergeNode mergeNode = new MergeNode(0, "mörtsch", 2);
        mergeNode.inputTypes(Arrays.<DataType>asList(DataTypes.STRING, DataTypes.UNDEFINED));
        GroupProjection groupProjection = new GroupProjection(
                Arrays.<Symbol>asList(Literal.newLiteral("key")),
                Arrays.asList(new Aggregation(
                        countInfo,
                        ImmutableList.<Symbol>of(),
                        Aggregation.Step.PARTIAL, Aggregation.Step.FINAL))
        );

        TopNProjection topNProjection = new TopNProjection(2, 0);
        topNProjection.outputs(Arrays.<Symbol>asList(
                new InputColumn(1),
                new InputColumn(0)
        ));

        mergeNode.projections(Arrays.asList(groupProjection, topNProjection));
        mergeNode.outputTypes(Arrays.<DataType>asList(DataTypes.LONG, DataTypes.STRING));
        StreamerVisitor.Context context = visitor.processPlanNode(mergeNode);
        assertSame(DataTypes.STRING.streamer(), context.outputStreamers()[1]);
        assertSame(DataTypes.LONG.streamer(), context.outputStreamers()[0]);
    }
}
