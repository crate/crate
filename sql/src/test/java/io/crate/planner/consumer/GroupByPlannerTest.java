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
import io.crate.analyze.TableDefinitions;
import io.crate.analyze.symbol.*;
import io.crate.metadata.Reference;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.TableIdent;
import io.crate.metadata.table.TestingTableInfo;
import io.crate.operation.aggregation.impl.CountAggregation;
import io.crate.planner.Merge;
import io.crate.planner.Plan;
import io.crate.planner.PositionalOrderBy;
import io.crate.planner.node.dql.Collect;
import io.crate.planner.node.dql.DistributedGroupBy;
import io.crate.planner.node.dql.MergePhase;
import io.crate.planner.node.dql.RoutedCollectPhase;
import io.crate.planner.projection.*;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.transport.LocalTransportAddress;
import org.elasticsearch.test.ClusterServiceUtils;
import org.hamcrest.core.Is;
import org.junit.Before;
import org.junit.Test;

import static io.crate.analyze.TableDefinitions.shardRouting;
import static io.crate.testing.SymbolMatchers.isAggregation;
import static io.crate.testing.SymbolMatchers.isReference;
import static org.hamcrest.Matchers.*;

public class GroupByPlannerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;

    @Before
    public void prepare() {
        ClusterState state =
            ClusterState.builder(ClusterName.DEFAULT)
                        .nodes(DiscoveryNodes.builder()
                                             .add(new DiscoveryNode("noop_id", LocalTransportAddress.buildUnique(),
                                                                    Version.CURRENT))
                                             .localNodeId("noop_id")
                              )
                        .build();
        ClusterServiceUtils.setState(clusterService, state);

        e  = SQLExecutor.builder(
            clusterService)
            .enableDefaultTables()
            .addDocTable(TableDefinitions.CLUSTERED_PARTED)
            .addDocTable(TestingTableInfo.builder(
                new TableIdent("doc", "empty_parted"), shardRouting("empty_parted"))
                .add("id", DataTypes.INTEGER)
                .add("date", DataTypes.TIMESTAMP, null, true)
                .addPrimaryKey("id")
                .addPrimaryKey("date")
                .clusteredBy("id")
                .build()
            ).build();
    }

    @Test
    public void testGroupByWithAggregationStringLiteralArguments() {
        Merge distributedGroupByMerge = e.plan("select count('foo'), name from users group by name");
        RoutedCollectPhase collectPhase = ((DistributedGroupBy) distributedGroupByMerge.subPlan()).collectPhase();
        assertThat(collectPhase.toCollect(), contains(isReference("name")));
        GroupProjection groupProjection = (GroupProjection) collectPhase.projections().get(0);
        assertThat(groupProjection.values().get(0), isAggregation("count"));
    }

    @Test
    public void testGroupByWithAggregationPlan() throws Exception {
        Merge distributedGroupByMerge = e.plan(
            "select count(*), name from users group by name");

        DistributedGroupBy distributedGroupBy = (DistributedGroupBy) distributedGroupByMerge.subPlan();

        // distributed collect
        RoutedCollectPhase collectPhase = distributedGroupBy.collectPhase();
        assertThat(collectPhase.maxRowGranularity(), is(RowGranularity.DOC));
        assertThat(collectPhase.nodeIds().size(), is(2));
        assertThat(collectPhase.toCollect().size(), is(1));
        assertThat(collectPhase.projections().size(), is(1));
        assertThat(collectPhase.projections().get(0), instanceOf(GroupProjection.class));
        assertThat(collectPhase.outputTypes().size(), is(2));
        assertEquals(DataTypes.STRING, collectPhase.outputTypes().get(0));
        assertEquals(CountAggregation.LongStateType.INSTANCE, collectPhase.outputTypes().get(1));

        MergePhase mergePhase = distributedGroupBy.reducerMergeNode();

        assertThat(mergePhase.numUpstreams(), is(2));
        assertThat(mergePhase.nodeIds().size(), is(2));
        assertEquals(mergePhase.inputTypes(), collectPhase.outputTypes());
        // for function evaluation and column-reordering there is always a EvalProjection
        assertThat(mergePhase.projections().size(), is(2));
        assertThat(mergePhase.projections().get(1), instanceOf(EvalProjection.class));

        assertThat(mergePhase.projections().get(0), instanceOf(GroupProjection.class));
        GroupProjection groupProjection = (GroupProjection) mergePhase.projections().get(0);
        InputColumn inputColumn = (InputColumn) groupProjection.values().get(0).inputs().get(0);
        assertThat(inputColumn.index(), is(1));

        assertThat(mergePhase.outputTypes().size(), is(2));
        assertEquals(DataTypes.LONG, mergePhase.outputTypes().get(0));
        assertEquals(DataTypes.STRING, mergePhase.outputTypes().get(1));

        MergePhase localMerge = distributedGroupByMerge.mergePhase();

        assertThat(localMerge.numUpstreams(), is(2));
        assertThat(localMerge.nodeIds().size(), is(1));
        assertThat(Iterables.getOnlyElement(localMerge.nodeIds()), is("noop_id"));
        assertEquals(mergePhase.outputTypes(), localMerge.inputTypes());

        assertThat(localMerge.projections(), empty());
    }

    @Test
    public void testGroupByWithAggregationAndLimit() throws Exception {
        Merge distributedGroupByMerge = e.plan(
            "select count(*), name from users group by name limit 1 offset 1");

        DistributedGroupBy distributedGroupBy = (DistributedGroupBy) distributedGroupByMerge.subPlan();

        // distributed merge
        MergePhase distributedMergePhase = distributedGroupBy.reducerMergeNode();
        assertThat(distributedMergePhase.projections().get(0), instanceOf(GroupProjection.class));
        assertThat(distributedMergePhase.projections().get(1), instanceOf(TopNProjection.class));

        // limit must include offset because the real limit can only be applied on the handler
        // after all rows have been gathered.
        TopNProjection topN = (TopNProjection) distributedMergePhase.projections().get(1);
        assertThat(topN.limit(), is(2));
        assertThat(topN.offset(), is(0));

        // local merge
        MergePhase localMergePhase = distributedGroupByMerge.mergePhase();
        assertThat(localMergePhase.projections().get(0), instanceOf(TopNProjection.class));
        topN = (TopNProjection) localMergePhase.projections().get(0);
        assertThat(topN.limit(), is(1));
        assertThat(topN.offset(), is(1));
        assertThat(topN.outputs().get(0), instanceOf(InputColumn.class));
        assertThat(((InputColumn) topN.outputs().get(0)).index(), is(0));
        assertThat(topN.outputs().get(1), instanceOf(InputColumn.class));
        assertThat(((InputColumn) topN.outputs().get(1)).index(), is(1));
    }

    @Test
    public void testGroupByOnNodeLevel() throws Exception {
        Merge merge = e.plan(
            "select count(*), name from sys.nodes group by name");
        Collect collect = (Collect) merge.subPlan();
        RoutedCollectPhase collectPhase = ((RoutedCollectPhase) collect.collectPhase());

        assertThat(collectPhase.projections(), contains(instanceOf(GroupProjection.class)));

        assertThat(merge.resultDescription().streamOutputs(), contains(
            is(DataTypes.LONG),
            is(DataTypes.STRING)));

        GroupProjection firstGroupProjection = (GroupProjection) collectPhase.projections().get(0);
        assertThat(firstGroupProjection.mode(), is(AggregateMode.ITER_PARTIAL));
        assertThat(firstGroupProjection.keys().size(), is(1));
        assertThat(((InputColumn) firstGroupProjection.outputs().get(0)).index(), is(0));
        assertThat(firstGroupProjection.outputs().get(1), is(instanceOf(Aggregation.class)));
        assertThat(((Aggregation) firstGroupProjection.outputs().get(1)).functionIdent().name(), is("count"));

        assertThat(merge.mergePhase().projections(), contains(
            instanceOf(GroupProjection.class),
            instanceOf(EvalProjection.class)));

        Projection secondGroupProjection = merge.mergePhase().projections().get(0);
        assertThat(((GroupProjection) secondGroupProjection).mode(), is(AggregateMode.PARTIAL_FINAL));
    }

    @Test
    public void testNonDistributedGroupByOnClusteredColumn() throws Exception {
        Merge merge = e.plan(
            "select count(*), id from users group by id limit 20");
        Collect collect = ((Collect) merge.subPlan());
        RoutedCollectPhase collectPhase = ((RoutedCollectPhase) collect.collectPhase());
        assertThat(collectPhase.projections().size(), is(2));
        assertThat(collectPhase.projections().get(1), instanceOf(TopNProjection.class));
        assertThat(collectPhase.projections().get(0).requiredGranularity(), is(RowGranularity.SHARD));
        MergePhase mergePhase = merge.mergePhase();
        assertThat(mergePhase.projections().size(), is(1));
    }

    @Test
    public void testNonDistributedGroupByOnClusteredColumnSorted() throws Exception {
        Merge merge = e.plan(
            "select count(*), id from users group by id order by 1 desc nulls last limit 20");
        Collect collect = ((Collect) merge.subPlan());
        RoutedCollectPhase collectPhase = ((RoutedCollectPhase) collect.collectPhase());
        assertThat(collectPhase.projections().size(), is(2));
        assertThat(collectPhase.projections().get(1), instanceOf(OrderedTopNProjection.class));
        assertThat(((OrderedTopNProjection) collectPhase.projections().get(1)).orderBy().size(), is(1));

        assertThat(collectPhase.projections().get(0).requiredGranularity(), is(RowGranularity.SHARD));
        MergePhase mergePhase = merge.mergePhase();
        assertThat(mergePhase.projections().size(), is(1));
        assertThat(mergePhase.projections().get(0), instanceOf(TopNProjection.class));

        PositionalOrderBy positionalOrderBy = mergePhase.orderByPositions();
        assertThat(positionalOrderBy, notNullValue());
        assertThat(positionalOrderBy.indices().length, is(1));
        assertThat(positionalOrderBy.indices()[0], is(0));
        assertThat(positionalOrderBy.reverseFlags()[0], is(true));
        assertThat(positionalOrderBy.nullsFirst()[0], is(false));
    }

    @Test
    public void testNonDistributedGroupByOnClusteredColumnSortedScalar() throws Exception {
        Merge merge = e.plan(
            "select count(*) + 1, id from users group by id order by count(*) + 1 limit 20");
        Collect collect = (Collect) merge.subPlan();
        RoutedCollectPhase collectPhase = ((RoutedCollectPhase) collect.collectPhase());
        assertThat(collectPhase.projections().size(), is(2));
        assertThat(collectPhase.projections().get(1), instanceOf(OrderedTopNProjection.class));
        assertThat(((OrderedTopNProjection) collectPhase.projections().get(1)).orderBy().size(), is(1));

        assertThat(collectPhase.projections().get(0).requiredGranularity(), is(RowGranularity.SHARD));
        MergePhase mergePhase = merge.mergePhase();
        assertThat(mergePhase.projections().size(), is(1));
        assertThat(mergePhase.projections().get(0), instanceOf(TopNProjection.class));

        PositionalOrderBy positionalOrderBy = mergePhase.orderByPositions();
        assertThat(positionalOrderBy, notNullValue());
        assertThat(positionalOrderBy.indices().length, is(1));
        assertThat(positionalOrderBy.indices()[0], is(0));
        assertThat(positionalOrderBy.reverseFlags()[0], is(false));
        assertThat(positionalOrderBy.nullsFirst()[0], nullValue());
    }

    @Test
    public void testGroupByWithOrderOnAggregate() throws Exception {
        Merge merge = e.plan(
            "select count(*), name from users group by name order by count(*)");

        assertThat(merge.mergePhase().orderByPositions(), notNullValue());

        DistributedGroupBy distributedGroupBy = (DistributedGroupBy) merge.subPlan();
        MergePhase mergePhase = distributedGroupBy.reducerMergeNode();

        assertThat(mergePhase.projections(), contains(
            instanceOf(GroupProjection.class),
            instanceOf(OrderedTopNProjection.class)));
        OrderedTopNProjection topNProjection = (OrderedTopNProjection) mergePhase.projections().get(1);
        Symbol orderBy = topNProjection.orderBy().get(0);
        assertThat(orderBy, instanceOf(InputColumn.class));

        assertThat(orderBy.valueType(), Is.<DataType>is(DataTypes.LONG));
    }

    @Test
    public void testHandlerSideRoutingGroupBy() throws Exception {
        Merge merge = e.plan(
            "select count(*) from sys.cluster group by name");
        Collect collect = (Collect) merge.subPlan();
        // just testing the dispatching here.. making sure it is not a ESSearchNode
        RoutedCollectPhase collectPhase = ((RoutedCollectPhase) collect.collectPhase());
        assertThat(collectPhase.toCollect().get(0), instanceOf(Reference.class));
        assertThat(collectPhase.toCollect().size(), is(1));

        assertThat(collectPhase.projections(), contains(instanceOf(GroupProjection.class)));

        assertThat(merge.mergePhase().projections(), contains(
            instanceOf(GroupProjection.class),
            instanceOf(EvalProjection.class)));
    }

    @Test
    public void testCountDistinctWithGroupBy() throws Exception {
        Merge distributedGroupByMerge = e.plan(
            "select count(distinct id), name from users group by name order by count(distinct id)");
        DistributedGroupBy distributedGroupBy = (DistributedGroupBy) distributedGroupByMerge.subPlan();
        RoutedCollectPhase collectPhase = distributedGroupBy.collectPhase();

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
        assertThat(groupProjection.mode(), is(AggregateMode.ITER_PARTIAL));

        Aggregation aggregation = groupProjection.values().get(0);
        Symbol aggregationInput = aggregation.inputs().get(0);
        assertThat(aggregationInput.symbolType(), is(SymbolType.INPUT_COLUMN));


        // reducer
        MergePhase mergePhase = distributedGroupBy.reducerMergeNode();
        assertThat(mergePhase.projections().size(), is(2));
        Projection groupProjection1 = mergePhase.projections().get(0);
        assertThat(groupProjection1, instanceOf(GroupProjection.class));
        groupProjection = (GroupProjection) groupProjection1;
        assertThat(groupProjection.keys().get(0), instanceOf(InputColumn.class));
        assertThat(((InputColumn) groupProjection.keys().get(0)).index(), is(0));
        assertThat(groupProjection.mode(), is(AggregateMode.PARTIAL_FINAL));
        assertThat(groupProjection.values().get(0), instanceOf(Aggregation.class));

        OrderedTopNProjection topNProjection = (OrderedTopNProjection) mergePhase.projections().get(1);
        Symbol collection_count = topNProjection.outputs().get(0);
        assertThat(collection_count, instanceOf(Function.class));


        // handler
        MergePhase localMergeNode = distributedGroupByMerge.mergePhase();
        assertThat(localMergeNode.projections(), empty());
    }

    @Test
    public void testGroupByHavingNonDistributed() throws Exception {
        Merge merge = e.plan(
            "select id from users group by id having id > 0");
        Collect collect = (Collect) merge.subPlan();
        RoutedCollectPhase collectPhase = ((RoutedCollectPhase) collect.collectPhase());
        assertThat(collectPhase.projections(), contains(
            instanceOf(GroupProjection.class),
            instanceOf(FilterProjection.class),
            instanceOf(EvalProjection.class)
        ));

        FilterProjection filterProjection = (FilterProjection) collectPhase.projections().get(1);
        assertThat(filterProjection.requiredGranularity(), is(RowGranularity.SHARD));
        assertThat(filterProjection.outputs().size(), is(1));
        assertThat(filterProjection.outputs().get(0), instanceOf(InputColumn.class));
        InputColumn inputColumn = (InputColumn) filterProjection.outputs().get(0);
        assertThat(inputColumn.index(), is(0));

        MergePhase localMergeNode = merge.mergePhase();
        assertThat(localMergeNode.projections(), empty());
    }

    @Test
    public void testGroupByWithHavingAndLimit() throws Exception {
        Merge planNode = e.plan(
            "select count(*), name from users group by name having count(*) > 1 limit 100");

        DistributedGroupBy distributedGroupBy = (DistributedGroupBy) planNode.subPlan();


        MergePhase mergePhase = distributedGroupBy.reducerMergeNode(); // reducer

        Projection projection = mergePhase.projections().get(1);
        assertThat(projection, instanceOf(FilterProjection.class));
        FilterProjection filterProjection = (FilterProjection) projection;

        Symbol countArgument = ((Function) filterProjection.query()).arguments().get(0);
        assertThat(countArgument, instanceOf(InputColumn.class));
        assertThat(((InputColumn) countArgument).index(), is(1));  // pointing to second output from group projection

        // outputs: count(*), name
        TopNProjection topN = (TopNProjection) mergePhase.projections().get(2);
        assertThat(topN.outputs().get(0).valueType(), Is.<DataType>is(DataTypes.LONG));
        assertThat(topN.outputs().get(1).valueType(), Is.<DataType>is(DataTypes.STRING));


        MergePhase localMerge = planNode.mergePhase();
        // topN projection
        //      outputs: count(*), name
        topN = (TopNProjection) localMerge.projections().get(0);
        assertThat(topN.outputs().get(0).valueType(), Is.<DataType>is(DataTypes.LONG));
        assertThat(topN.outputs().get(1).valueType(), Is.<DataType>is(DataTypes.STRING));
    }

    @Test
    public void testGroupByWithHavingAndNoLimit() throws Exception {
        Merge planNode = e.plan(
            "select count(*), name from users group by name having count(*) > 1");
        DistributedGroupBy distributedGroupBy = (DistributedGroupBy) planNode.subPlan();

        MergePhase mergePhase = distributedGroupBy.reducerMergeNode(); // reducer

        // group projection
        //      outputs: name, count(*)

        Projection projection = mergePhase.projections().get(1);
        assertThat(projection, instanceOf(FilterProjection.class));
        FilterProjection filterProjection = (FilterProjection) projection;

        Symbol countArgument = ((Function) filterProjection.query()).arguments().get(0);
        assertThat(countArgument, instanceOf(InputColumn.class));
        assertThat(((InputColumn) countArgument).index(), is(1));  // pointing to second output from group projection

        assertThat(mergePhase.outputTypes().get(0), equalTo(DataTypes.LONG));
        assertThat(mergePhase.outputTypes().get(1), equalTo(DataTypes.STRING));

        mergePhase = planNode.mergePhase();

        assertThat(mergePhase.outputTypes().get(0), equalTo(DataTypes.LONG));
        assertThat(mergePhase.outputTypes().get(1), equalTo(DataTypes.STRING));
    }

    @Test
    public void testGroupByWithHavingAndNoSelectListReordering() throws Exception {
        Merge planNode = e.plan(
            "select name, count(*) from users group by name having count(*) > 1");
        DistributedGroupBy distributedGroupBy = (DistributedGroupBy) planNode.subPlan();

        MergePhase reducerMerge = distributedGroupBy.reducerMergeNode(); // reducer

        // group projection
        //      outputs: name, count(*)
        // filter projection
        //      outputs: name, count(*)

        assertThat(reducerMerge.projections(), contains(
            instanceOf(GroupProjection.class),
            instanceOf(FilterProjection.class),
            instanceOf(EvalProjection.class)
        ));
        Projection projection = reducerMerge.projections().get(1);
        assertThat(projection, instanceOf(FilterProjection.class));
        FilterProjection filterProjection = (FilterProjection) projection;

        Symbol countArgument = ((Function) filterProjection.query()).arguments().get(0);
        assertThat(countArgument, instanceOf(InputColumn.class));
        assertThat(((InputColumn) countArgument).index(), is(1));  // pointing to second output from group projection

        // outputs: name, count(*)
        assertThat(((InputColumn) filterProjection.outputs().get(0)).index(), is(0));
        assertThat(((InputColumn) filterProjection.outputs().get(1)).index(), is(1));

        // eval projection
        //      outputs: name, count(*)
        EvalProjection eval = (EvalProjection) reducerMerge.projections().get(2);
        assertThat(((InputColumn) eval.outputs().get(0)).index(), is(0));
        assertThat(((InputColumn) eval.outputs().get(1)).index(), is(1));

        MergePhase localMerge = planNode.mergePhase();
        assertThat(localMerge.projections(), empty());
    }

    @Test
    public void testGroupByHavingAndNoSelectListReOrderingWithLimit() throws Exception {
        Merge planNode = e.plan(
            "select name, count(*) from users group by name having count(*) > 1 limit 100");
        DistributedGroupBy distributedGroupBy = (DistributedGroupBy) planNode.subPlan();

        MergePhase mergePhase = distributedGroupBy.reducerMergeNode(); // reducer

        // group projection
        //      outputs: name, count(*)
        // filter projection
        //      outputs: name, count(*)
        // topN projection
        //      outputs: name, count(*)

        Projection projection = mergePhase.projections().get(1);
        assertThat(projection, instanceOf(FilterProjection.class));
        FilterProjection filterProjection = (FilterProjection) projection;

        Symbol countArgument = ((Function) filterProjection.query()).arguments().get(0);
        assertThat(countArgument, instanceOf(InputColumn.class));
        assertThat(((InputColumn) countArgument).index(), is(1));  // pointing to second output from group projection

        // outputs: name, count(*)
        assertThat(((InputColumn) filterProjection.outputs().get(0)).index(), is(0));
        assertThat(((InputColumn) filterProjection.outputs().get(1)).index(), is(1));

        // outputs: name, count(*)
        TopNProjection topN = (TopNProjection) mergePhase.projections().get(2);
        assertThat(((InputColumn) topN.outputs().get(0)).index(), is(0));
        assertThat(((InputColumn) topN.outputs().get(1)).index(), is(1));


        MergePhase localMerge = planNode.mergePhase();

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
        Merge distributedGroupByMerge = e.plan("select count(*) from users group by name");
        DistributedGroupBy distributedGroupBy = (DistributedGroupBy) distributedGroupByMerge.subPlan();
        RoutedCollectPhase collectPhase = distributedGroupBy.collectPhase();
        assertThat(collectPhase.projections().size(), is(1));
        assertThat(collectPhase.projections().get(0), instanceOf(GroupProjection.class));
        assertThat(collectPhase.projections().get(0).requiredGranularity(), is(RowGranularity.SHARD));
    }

    @Test
    public void testNonDistributedGroupByProjectionHasShardLevelGranularity() throws Exception {
        Merge distributedGroupByMerge = e.plan("select count(distinct id), name from users" +
                                                       " group by name order by count(distinct id)");
        DistributedGroupBy distributedGroupBy = (DistributedGroupBy) distributedGroupByMerge.subPlan();
        RoutedCollectPhase collectPhase = distributedGroupBy.collectPhase();
        assertThat(collectPhase.projections().size(), is(1));
        assertThat(collectPhase.projections().get(0), instanceOf(GroupProjection.class));
        assertThat(collectPhase.projections().get(0).requiredGranularity(), is(RowGranularity.SHARD));
    }

    @Test
    public void testNoDistributedGroupByOnAllPrimaryKeys() throws Exception {
        Merge merge = e.plan(
            "select count(*), id, date from empty_parted group by id, date limit 20");
        Collect collect = (Collect) merge.subPlan();
        RoutedCollectPhase collectPhase = ((RoutedCollectPhase) collect.collectPhase());
        assertThat(collectPhase.projections().size(), is(2));
        assertThat(collectPhase.projections().get(0), instanceOf(GroupProjection.class));
        assertThat(collectPhase.projections().get(0).requiredGranularity(), is(RowGranularity.SHARD));
        assertThat(collectPhase.projections().get(1), instanceOf(TopNProjection.class));
        MergePhase mergePhase = merge.mergePhase();
        assertThat(mergePhase.projections().size(), is(1));
        assertThat(mergePhase.projections().get(0), instanceOf(TopNProjection.class));
    }

    @Test
    public void testNonDistributedGroupByAggregationsWrappedInScalar() throws Exception {
        Merge planNode = e.plan(
            "select (count(*) + 1), id from empty_parted group by id");
        DistributedGroupBy distributedGroupBy = (DistributedGroupBy) planNode.subPlan();

        RoutedCollectPhase collectPhase = distributedGroupBy.collectPhase();
        assertThat(collectPhase.projections(), contains(
            instanceOf(GroupProjection.class)
        ));
        assertThat(collectPhase.projections().size(), is(1));
        assertThat(collectPhase.projections().get(0), instanceOf(GroupProjection.class));

        MergePhase mergePhase = planNode.mergePhase();
        assertThat(mergePhase.projections().size(), is(0));
    }

    @Test
    public void testGroupByHaving() throws Exception {
        Merge distributedGroupByMerge = e.plan(
            "select avg(date), name from users group by name having min(date) > '1970-01-01'");
        DistributedGroupBy distributedGroupBy = (DistributedGroupBy) distributedGroupByMerge.subPlan();
        RoutedCollectPhase collectPhase = distributedGroupBy.collectPhase();
        assertThat(collectPhase.projections().size(), is(1));
        assertThat(collectPhase.projections().get(0), instanceOf(GroupProjection.class));

        MergePhase mergePhase = distributedGroupBy.reducerMergeNode();

        assertThat(mergePhase.projections().size(), is(3));

        // grouping
        assertThat(mergePhase.projections().get(0), instanceOf(GroupProjection.class));
        GroupProjection groupProjection = (GroupProjection) mergePhase.projections().get(0);
        assertThat(groupProjection.values().size(), is(2));

        // filter the having clause
        assertThat(mergePhase.projections().get(1), instanceOf(FilterProjection.class));
        FilterProjection filterProjection = (FilterProjection) mergePhase.projections().get(1);

        assertThat(mergePhase.projections().get(2), instanceOf(EvalProjection.class));
        EvalProjection eval = (EvalProjection) mergePhase.projections().get(2);
        assertThat(eval.outputs().get(0).valueType(), Is.<DataType>is(DataTypes.DOUBLE));
        assertThat(eval.outputs().get(1).valueType(), Is.<DataType>is(DataTypes.STRING));
    }

    @Test
    public void testNestedGroupByAggregation() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Cannot create plan for: ");
        e.plan("select count(*) from (" +
             "  select max(load['1']) as maxLoad, hostname " +
             "  from sys.nodes " +
             "  group by hostname having max(load['1']) > 50) as nodes " +
             "group by hostname");
    }

    @Test
    public void testGroupByOnClusteredByColumnPartitionedOnePartition() throws Exception {
        // only one partition hit
        Merge optimizedPlan = e.plan("select count(*), city from clustered_parted where date=1395874800000 group by city");
        Collect collect = (Collect) optimizedPlan.subPlan();

        assertThat(collect.collectPhase().projections(), contains(
            instanceOf(GroupProjection.class),
            instanceOf(EvalProjection.class)));
        assertThat(collect.collectPhase().projections().get(0), instanceOf(GroupProjection.class));

        assertThat(optimizedPlan.mergePhase().projections().size(), is(0));

        // > 1 partition hit
        Plan plan = e.plan("select count(*), city from clustered_parted where date=1395874800000 or date=1395961200000 group by city");
        assertThat(plan, instanceOf(Merge.class));
        assertThat(((Merge) plan).subPlan(), instanceOf(DistributedGroupBy.class));
    }

    @Test
    public void testGroupByOrderByPartitionedClolumn() throws Exception {
        Merge plan = e.plan("select date from clustered_parted group by date order by date");
        DistributedGroupBy distributedGroupBy = (DistributedGroupBy)plan.subPlan();
        OrderedTopNProjection topNProjection = (OrderedTopNProjection)distributedGroupBy.reducerMergeNode().projections().get(1);

        Symbol orderBy = topNProjection.orderBy().get(0);
        assertThat(orderBy, instanceOf(InputColumn.class));
        assertThat(orderBy.valueType(), is(DataTypes.TIMESTAMP));
    }
}
