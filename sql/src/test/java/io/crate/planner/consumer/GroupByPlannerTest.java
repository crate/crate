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

package io.crate.planner.consumer;

import com.google.common.collect.Iterables;
import io.crate.analyze.symbol.*;
import io.crate.metadata.Reference;
import io.crate.metadata.RowGranularity;
import io.crate.operation.aggregation.impl.CountAggregation;
import io.crate.planner.node.dql.CollectAndMerge;
import io.crate.planner.node.dql.DistributedGroupBy;
import io.crate.planner.node.dql.MergePhase;
import io.crate.planner.node.dql.RoutedCollectPhase;
import io.crate.planner.projection.FilterProjection;
import io.crate.planner.projection.GroupProjection;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.TopNProjection;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.test.cluster.NoopClusterService;
import org.hamcrest.core.Is;
import org.junit.Test;

import static io.crate.testing.SymbolMatchers.isAggregation;
import static io.crate.testing.SymbolMatchers.isReference;
import static org.hamcrest.Matchers.*;

public class GroupByPlannerTest extends CrateUnitTest {

    private SQLExecutor e = SQLExecutor.builder(new NoopClusterService()).enableDefaultTables().build();

    @Test
    public void testGroupByWithAggregationStringLiteralArguments() {
        RoutedCollectPhase collectPhase = ((DistributedGroupBy) e.plan(
            "select count('foo'), name from users group by name")).collectNode();
        assertThat(collectPhase.toCollect(), contains(isReference("name")));
        GroupProjection groupProjection = (GroupProjection) collectPhase.projections().get(0);
        assertThat(groupProjection.values().get(0), isAggregation("count"));
    }

    @Test
    public void testGroupByWithAggregationPlan() throws Exception {
        DistributedGroupBy distributedGroupBy = e.plan(
            "select count(*), name from users group by name");

        // distributed collect
        RoutedCollectPhase collectPhase = distributedGroupBy.collectNode();
        assertThat(collectPhase.maxRowGranularity(), is(RowGranularity.DOC));
        assertThat(collectPhase.executionNodes().size(), is(2));
        assertThat(collectPhase.toCollect().size(), is(1));
        assertThat(collectPhase.projections().size(), is(1));
        assertThat(collectPhase.projections().get(0), instanceOf(GroupProjection.class));
        assertThat(collectPhase.outputTypes().size(), is(2));
        assertEquals(DataTypes.STRING, collectPhase.outputTypes().get(0));
        assertEquals(CountAggregation.LongStateType.INSTANCE, collectPhase.outputTypes().get(1));

        MergePhase mergeNode = distributedGroupBy.reducerMergeNode();

        assertThat(mergeNode.numUpstreams(), is(2));
        assertThat(mergeNode.executionNodes().size(), is(2));
        assertEquals(mergeNode.inputTypes(), collectPhase.outputTypes());
        assertThat(mergeNode.projections().size(), is(2)); // for the default limit there is always a TopNProjection
        assertThat(mergeNode.projections().get(1), instanceOf(TopNProjection.class));

        assertThat(mergeNode.projections().get(0), instanceOf(GroupProjection.class));
        GroupProjection groupProjection = (GroupProjection) mergeNode.projections().get(0);
        InputColumn inputColumn = (InputColumn) groupProjection.values().get(0).inputs().get(0);
        assertThat(inputColumn.index(), is(1));

        assertThat(mergeNode.outputTypes().size(), is(2));
        assertEquals(DataTypes.LONG, mergeNode.outputTypes().get(0));
        assertEquals(DataTypes.STRING, mergeNode.outputTypes().get(1));

        MergePhase localMerge = distributedGroupBy.localMergeNode();

        assertThat(localMerge.numUpstreams(), is(2));
        assertThat(localMerge.executionNodes().size(), is(1));
        assertThat(Iterables.getOnlyElement(localMerge.executionNodes()), is("noop_id"));
        assertEquals(mergeNode.outputTypes(), localMerge.inputTypes());

        assertThat(localMerge.projections().get(0), instanceOf(TopNProjection.class));
        TopNProjection topN = (TopNProjection) localMerge.projections().get(0);
        assertThat(topN.outputs().size(), is(2));

        assertEquals(DataTypes.LONG, localMerge.outputTypes().get(0));
        assertEquals(DataTypes.STRING, localMerge.outputTypes().get(1));
    }

    @Test
    public void testGroupByWithAggregationAndLimit() throws Exception {
        DistributedGroupBy distributedGroupBy = e.plan(
            "select count(*), name from users group by name limit 1 offset 1");

        // distributed merge
        MergePhase mergeNode = distributedGroupBy.reducerMergeNode();
        assertThat(mergeNode.projections().get(0), instanceOf(GroupProjection.class));
        assertThat(mergeNode.projections().get(1), instanceOf(TopNProjection.class));

        // limit must include offset because the real limit can only be applied on the handler
        // after all rows have been gathered.
        TopNProjection topN = (TopNProjection) mergeNode.projections().get(1);
        assertThat(topN.limit(), is(2));
        assertThat(topN.offset(), is(0));

        // local merge
        MergePhase mergePhase = distributedGroupBy.localMergeNode();
        assertThat(mergePhase.projections().get(0), instanceOf(TopNProjection.class));
        topN = (TopNProjection) mergePhase.projections().get(0);
        assertThat(topN.limit(), is(1));
        assertThat(topN.offset(), is(1));
        assertThat(topN.outputs().get(0), instanceOf(InputColumn.class));
        assertThat(((InputColumn) topN.outputs().get(0)).index(), is(0));
        assertThat(topN.outputs().get(1), instanceOf(InputColumn.class));
        assertThat(((InputColumn) topN.outputs().get(1)).index(), is(1));
    }

    @Test
    public void testGroupByOnNodeLevel() throws Exception {
        CollectAndMerge planNode = e.plan(
            "select count(*), name from sys.nodes group by name");
        RoutedCollectPhase collectPhase = ((RoutedCollectPhase) planNode.collectPhase());
        assertEquals(DataTypes.STRING, collectPhase.outputTypes().get(0));
        assertEquals(CountAggregation.LongStateType.INSTANCE, collectPhase.outputTypes().get(1));

        MergePhase mergeNode = planNode.localMerge();
        assertThat(mergeNode.numUpstreams(), is(1));
        assertThat(mergeNode.projections().size(), is(2));

        assertEquals(DataTypes.LONG, mergeNode.outputTypes().get(0));
        assertEquals(DataTypes.STRING, mergeNode.outputTypes().get(1));

        GroupProjection groupProjection = (GroupProjection) mergeNode.projections().get(0);
        assertThat(groupProjection.keys().size(), is(1));
        assertThat(((InputColumn) groupProjection.outputs().get(0)).index(), is(0));
        assertThat(groupProjection.outputs().get(1), is(instanceOf(Aggregation.class)));
        assertThat(((Aggregation) groupProjection.outputs().get(1)).functionIdent().name(), is("count"));
        assertThat(((Aggregation) groupProjection.outputs().get(1)).fromStep(), is(Aggregation.Step.PARTIAL));
        assertThat(((Aggregation) groupProjection.outputs().get(1)).toStep(), is(Aggregation.Step.FINAL));

        TopNProjection projection = (TopNProjection) mergeNode.projections().get(1);
        assertThat(((InputColumn) projection.outputs().get(0)).index(), is(1));
        assertThat(((InputColumn) projection.outputs().get(1)).index(), is(0));
    }

    @Test
    public void testNonDistributedGroupByOnClusteredColumn() throws Exception {
        CollectAndMerge planNode = e.plan(
            "select count(*), id from users group by id limit 20");
        RoutedCollectPhase collectPhase = ((RoutedCollectPhase) planNode.collectPhase());
        assertThat(collectPhase.projections().size(), is(2));
        assertThat(collectPhase.projections().get(1), instanceOf(TopNProjection.class));
        assertThat(collectPhase.projections().get(0).requiredGranularity(), is(RowGranularity.SHARD));
        MergePhase mergeNode = planNode.localMerge();
        assertThat(mergeNode.projections().size(), is(1));
    }

    @Test
    public void testNonDistributedGroupByOnClusteredColumnSorted() throws Exception {
        CollectAndMerge planNode = e.plan(
            "select count(*), id from users group by id order by 1 desc nulls last limit 20");
        RoutedCollectPhase collectPhase = ((RoutedCollectPhase) planNode.collectPhase());
        assertThat(collectPhase.projections().size(), is(2));
        assertThat(collectPhase.projections().get(1), instanceOf(TopNProjection.class));
        assertThat(((TopNProjection) collectPhase.projections().get(1)).orderBy().size(), is(1));

        assertThat(collectPhase.projections().get(0).requiredGranularity(), is(RowGranularity.SHARD));
        MergePhase mergeNode = planNode.localMerge();
        assertThat(mergeNode.projections().size(), is(1));
        TopNProjection projection = (TopNProjection) mergeNode.projections().get(0);
        assertThat(projection.orderBy(), is(nullValue()));
        assertThat(mergeNode.sortedInputOutput(), is(true));
        assertThat(mergeNode.orderByIndices().length, is(1));
        assertThat(mergeNode.orderByIndices()[0], is(0));
        assertThat(mergeNode.reverseFlags()[0], is(true));
        assertThat(mergeNode.nullsFirst()[0], is(false));
    }

    @Test
    public void testNonDistributedGroupByOnClusteredColumnSortedScalar() throws Exception {
        CollectAndMerge planNode = e.plan(
            "select count(*) + 1, id from users group by id order by count(*) + 1 limit 20");
        RoutedCollectPhase collectPhase = ((RoutedCollectPhase) planNode.collectPhase());
        assertThat(collectPhase.projections().size(), is(2));
        assertThat(collectPhase.projections().get(1), instanceOf(TopNProjection.class));
        assertThat(((TopNProjection) collectPhase.projections().get(1)).orderBy().size(), is(1));

        assertThat(collectPhase.projections().get(0).requiredGranularity(), is(RowGranularity.SHARD));
        MergePhase mergeNode = planNode.localMerge();
        assertThat(mergeNode.projections().size(), is(1));
        TopNProjection projection = (TopNProjection) mergeNode.projections().get(0);
        assertThat(projection.orderBy(), is(nullValue()));
        assertThat(mergeNode.sortedInputOutput(), is(true));
        assertThat(mergeNode.orderByIndices().length, is(1));
        assertThat(mergeNode.orderByIndices()[0], is(0));
        assertThat(mergeNode.reverseFlags()[0], is(false));
        assertThat(mergeNode.nullsFirst()[0], is(nullValue()));
    }

    @Test
    public void testGroupByWithOrderOnAggregate() throws Exception {
        DistributedGroupBy distributedGroupBy = e.plan(
            "select count(*), name from users group by name order by count(*)");

        // sort is on handler because there is no limit/offset
        // handler
        MergePhase mergeNode = distributedGroupBy.localMergeNode();
        assertThat(mergeNode.projections().size(), is(1));

        TopNProjection topNProjection = (TopNProjection) mergeNode.projections().get(0);
        Symbol orderBy = topNProjection.orderBy().get(0);
        assertThat(orderBy, instanceOf(InputColumn.class));

        assertThat(orderBy.valueType(), Is.<DataType>is(DataTypes.LONG));
    }

    @Test
    public void testHandlerSideRoutingGroupBy() throws Exception {
        CollectAndMerge planNode = e.plan(
            "select count(*) from sys.cluster group by name");
        // just testing the dispatching here.. making sure it is not a ESSearchNode
        RoutedCollectPhase collectPhase = ((RoutedCollectPhase) planNode.collectPhase());
        assertThat(collectPhase.toCollect().get(0), instanceOf(Reference.class));
        assertThat(collectPhase.toCollect().size(), is(1));

        MergePhase mergeNode = planNode.localMerge();
        assertThat(mergeNode.projections().size(), is(2));
        assertThat(mergeNode.projections().get(0), instanceOf(GroupProjection.class));
        assertThat(mergeNode.projections().get(1), instanceOf(TopNProjection.class));
    }

    @Test
    public void testCountDistinctWithGroupBy() throws Exception {
        DistributedGroupBy distributedGroupBy = e.plan(
            "select count(distinct id), name from users group by name order by count(distinct id)");
        RoutedCollectPhase collectPhase = distributedGroupBy.collectNode();

        // collect
        assertThat(collectPhase.toCollect().get(0), instanceOf(Reference.class));
        assertThat(collectPhase.toCollect().size(), is(2));
        assertThat(((Reference) collectPhase.toCollect().get(0)).ident().columnIdent().name(), is("id"));
        assertThat(((Reference) collectPhase.toCollect().get(1)).ident().columnIdent().name(), is("name"));
        Projection projection = collectPhase.projections().get(0);
        assertThat(projection, instanceOf(GroupProjection.class));
        GroupProjection groupProjection = (GroupProjection) projection;
        Symbol groupKey = groupProjection.keys().get(0);
        assertThat(groupKey, instanceOf(InputColumn.class));
        assertThat(((InputColumn) groupKey).index(), is(1));
        assertThat(groupProjection.values().size(), is(1));

        Aggregation aggregation = groupProjection.values().get(0);
        assertThat(aggregation.toStep(), is(Aggregation.Step.PARTIAL));
        Symbol aggregationInput = aggregation.inputs().get(0);
        assertThat(aggregationInput.symbolType(), is(SymbolType.INPUT_COLUMN));


        // reducer
        MergePhase mergeNode = distributedGroupBy.reducerMergeNode();
        assertThat(mergeNode.projections().size(), is(2));
        Projection groupProjection1 = mergeNode.projections().get(0);
        assertThat(groupProjection1, instanceOf(GroupProjection.class));
        groupProjection = (GroupProjection) groupProjection1;
        assertThat(groupProjection.keys().get(0), instanceOf(InputColumn.class));
        assertThat(((InputColumn) groupProjection.keys().get(0)).index(), is(0));

        assertThat(groupProjection.values().get(0), instanceOf(Aggregation.class));
        Aggregation aggregationStep2 = groupProjection.values().get(0);
        assertThat(aggregationStep2.toStep(), is(Aggregation.Step.FINAL));

        TopNProjection topNProjection = (TopNProjection) mergeNode.projections().get(1);
        Symbol collection_count = topNProjection.outputs().get(0);
        assertThat(collection_count, instanceOf(Function.class));


        // handler
        MergePhase localMergeNode = distributedGroupBy.localMergeNode();
        assertThat(localMergeNode.projections().size(), is(1));
        Projection localTopN = localMergeNode.projections().get(0);
        assertThat(localTopN, instanceOf(TopNProjection.class));
    }

    @Test
    public void testGroupByHavingNonDistributed() throws Exception {
        CollectAndMerge planNode = e.plan(
            "select id from users group by id having id > 0");
        RoutedCollectPhase collectPhase = ((RoutedCollectPhase) planNode.collectPhase());
        assertThat(collectPhase.projections().size(), is(2));
        assertThat(collectPhase.projections().get(0), instanceOf(GroupProjection.class));
        assertThat(collectPhase.projections().get(1), instanceOf(FilterProjection.class));

        FilterProjection filterProjection = (FilterProjection) collectPhase.projections().get(1);
        assertThat(filterProjection.requiredGranularity(), is(RowGranularity.SHARD));
        assertThat(filterProjection.outputs().size(), is(1));
        assertThat(filterProjection.outputs().get(0), instanceOf(InputColumn.class));
        InputColumn inputColumn = (InputColumn) filterProjection.outputs().get(0);
        assertThat(inputColumn.index(), is(0));

        MergePhase localMergeNode = planNode.localMerge();

        assertThat(localMergeNode.projections().size(), is(1));
        assertThat(localMergeNode.projections().get(0), instanceOf(TopNProjection.class));
    }

    @Test
    public void testGroupByWithHavingAndLimit() throws Exception {
        DistributedGroupBy planNode = e.plan(
            "select count(*), name from users group by name having count(*) > 1 limit 100");

        MergePhase mergeNode = planNode.reducerMergeNode(); // reducer

        Projection projection = mergeNode.projections().get(1);
        assertThat(projection, instanceOf(FilterProjection.class));
        FilterProjection filterProjection = (FilterProjection) projection;

        Symbol countArgument = ((Function) filterProjection.query()).arguments().get(0);
        assertThat(countArgument, instanceOf(InputColumn.class));
        assertThat(((InputColumn) countArgument).index(), is(1));  // pointing to second output from group projection

        // outputs: count(*), name
        TopNProjection topN = (TopNProjection) mergeNode.projections().get(2);
        assertThat(topN.outputs().get(0).valueType(), Is.<DataType>is(DataTypes.LONG));
        assertThat(topN.outputs().get(1).valueType(), Is.<DataType>is(DataTypes.STRING));


        MergePhase localMerge = planNode.localMergeNode();
        // topN projection
        //      outputs: count(*), name
        topN = (TopNProjection) localMerge.projections().get(0);
        assertThat(topN.outputs().get(0).valueType(), Is.<DataType>is(DataTypes.LONG));
        assertThat(topN.outputs().get(1).valueType(), Is.<DataType>is(DataTypes.STRING));
    }

    @Test
    public void testGroupByWithHavingAndNoLimit() throws Exception {
        DistributedGroupBy planNode = e.plan(
            "select count(*), name from users group by name having count(*) > 1");

        MergePhase mergeNode = planNode.reducerMergeNode(); // reducer

        // group projection
        //      outputs: name, count(*)

        Projection projection = mergeNode.projections().get(1);
        assertThat(projection, instanceOf(FilterProjection.class));
        FilterProjection filterProjection = (FilterProjection) projection;

        Symbol countArgument = ((Function) filterProjection.query()).arguments().get(0);
        assertThat(countArgument, instanceOf(InputColumn.class));
        assertThat(((InputColumn) countArgument).index(), is(1));  // pointing to second output from group projection

        assertThat(mergeNode.outputTypes().get(0), equalTo((DataType) DataTypes.LONG));
        assertThat(mergeNode.outputTypes().get(1), equalTo((DataType) DataTypes.STRING));

        mergeNode = planNode.localMergeNode();

        assertThat(mergeNode.outputTypes().get(0), equalTo((DataType) DataTypes.LONG));
        assertThat(mergeNode.outputTypes().get(1), equalTo((DataType) DataTypes.STRING));
    }

    @Test
    public void testGroupByWithHavingAndNoSelectListReordering() throws Exception {
        DistributedGroupBy planNode = e.plan(
            "select name, count(*) from users group by name having count(*) > 1");

        MergePhase mergeNode = planNode.reducerMergeNode(); // reducer

        // group projection
        //      outputs: name, count(*)
        // filter projection
        //      outputs: name, count(*)

        Projection projection = mergeNode.projections().get(1);
        assertThat(projection, instanceOf(FilterProjection.class));
        FilterProjection filterProjection = (FilterProjection) projection;

        Symbol countArgument = ((Function) filterProjection.query()).arguments().get(0);
        assertThat(countArgument, instanceOf(InputColumn.class));
        assertThat(((InputColumn) countArgument).index(), is(1));  // pointing to second output from group projection

        // outputs: name, count(*)
        assertThat(((InputColumn) filterProjection.outputs().get(0)).index(), is(0));
        assertThat(((InputColumn) filterProjection.outputs().get(1)).index(), is(1));

        MergePhase localMerge = planNode.localMergeNode();
        // topN projection
        //      outputs: name, count(*)
        TopNProjection topN = (TopNProjection) localMerge.projections().get(0);
        assertThat(((InputColumn) topN.outputs().get(0)).index(), is(0));
        assertThat(((InputColumn) topN.outputs().get(1)).index(), is(1));
    }

    @Test
    public void testGroupByHavingAndNoSelectListReOrderingWithLimit() throws Exception {
        DistributedGroupBy planNode = e.plan(
            "select name, count(*) from users group by name having count(*) > 1 limit 100");

        MergePhase mergeNode = planNode.reducerMergeNode(); // reducer

        // group projection
        //      outputs: name, count(*)
        // filter projection
        //      outputs: name, count(*)
        // topN projection
        //      outputs: name, count(*)

        Projection projection = mergeNode.projections().get(1);
        assertThat(projection, instanceOf(FilterProjection.class));
        FilterProjection filterProjection = (FilterProjection) projection;

        Symbol countArgument = ((Function) filterProjection.query()).arguments().get(0);
        assertThat(countArgument, instanceOf(InputColumn.class));
        assertThat(((InputColumn) countArgument).index(), is(1));  // pointing to second output from group projection

        // outputs: name, count(*)
        assertThat(((InputColumn) filterProjection.outputs().get(0)).index(), is(0));
        assertThat(((InputColumn) filterProjection.outputs().get(1)).index(), is(1));

        // outputs: name, count(*)
        TopNProjection topN = (TopNProjection) mergeNode.projections().get(2);
        assertThat(((InputColumn) topN.outputs().get(0)).index(), is(0));
        assertThat(((InputColumn) topN.outputs().get(1)).index(), is(1));


        MergePhase localMerge = planNode.localMergeNode();

        // topN projection
        //      outputs: name, count(*)
        topN = (TopNProjection) localMerge.projections().get(0);
        assertThat(((InputColumn) topN.outputs().get(0)).index(), is(0));
        assertThat(((InputColumn) topN.outputs().get(1)).index(), is(1));
    }

    @Test
    public void testGroupByOnAnalyzed() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Cannot GROUP BY 'text': grouping on analyzed/fulltext columns is not possible");
        e.plan("select text from users u group by 1");
    }

    @Test
    public void testGroupByOnIndexOff() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Cannot GROUP BY 'no_index': grouping on non-indexed columns is not possible");
        e.plan("select no_index from users u group by 1");
    }

    @Test
    public void testSelectAnalyzedReferenceInFunctionGroupBy() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Cannot GROUP BY 'text': grouping on analyzed/fulltext columns is not possible");
        e.plan("select substr(text, 0, 2) from users u group by 1");
    }

    @Test
    public void testSelectNonIndexedReferenceInFunctionGroupBy() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Cannot GROUP BY 'no_index': grouping on non-indexed columns is not possible");
        e.plan("select substr(no_index, 0, 2) from users u group by 1");
    }

    @Test
    public void testDistributedGroupByProjectionHasShardLevelGranularity() throws Exception {
        DistributedGroupBy distributedGroupBy = e.plan("select count(*) from users group by name");
        RoutedCollectPhase collectPhase = distributedGroupBy.collectNode();
        assertThat(collectPhase.projections().size(), is(1));
        assertThat(collectPhase.projections().get(0), instanceOf(GroupProjection.class));
        assertThat(collectPhase.projections().get(0).requiredGranularity(), is(RowGranularity.SHARD));
    }

    @Test
    public void testNonDistributedGroupByProjectionHasShardLevelGranularity() throws Exception {
        DistributedGroupBy distributedGroupBy = e.plan("select count(distinct id), name from users" +
                                                       " group by name order by count(distinct id)");
        RoutedCollectPhase collectPhase = distributedGroupBy.collectNode();
        assertThat(collectPhase.projections().size(), is(1));
        assertThat(collectPhase.projections().get(0), instanceOf(GroupProjection.class));
        assertThat(collectPhase.projections().get(0).requiredGranularity(), is(RowGranularity.SHARD));
    }
}
