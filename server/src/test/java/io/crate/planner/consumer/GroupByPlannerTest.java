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

package io.crate.planner.consumer;

import com.carrotsearch.randomizedtesting.RandomizedTest;

import io.crate.analyze.TableDefinitions;
import io.crate.execution.dsl.phases.CollectPhase;
import io.crate.execution.dsl.phases.MergePhase;
import io.crate.execution.dsl.phases.RoutedCollectPhase;
import io.crate.execution.dsl.projection.EvalProjection;
import io.crate.execution.dsl.projection.FilterProjection;
import io.crate.execution.dsl.projection.GroupProjection;
import io.crate.execution.dsl.projection.OrderedTopNProjection;
import io.crate.execution.dsl.projection.Projection;
import io.crate.execution.dsl.projection.TopNProjection;
import io.crate.execution.engine.aggregation.impl.CountAggregation;
import io.crate.expression.symbol.AggregateMode;
import io.crate.expression.symbol.Aggregation;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolType;
import io.crate.expression.symbol.Symbols;
import io.crate.metadata.PartitionName;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.Merge;
import io.crate.planner.PositionalOrderBy;
import io.crate.planner.node.dql.Collect;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.testing.SymbolMatchers;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.hamcrest.Matchers;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static io.crate.testing.SymbolMatchers.isAggregation;
import static io.crate.testing.SymbolMatchers.isReference;
import static io.crate.testing.TestingHelpers.isSQL;
import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class GroupByPlannerTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void testGroupByWithAggregationStringLiteralArguments() throws IOException {
        var e = SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of())
            .addTable(TableDefinitions.USER_TABLE_DEFINITION)
            .build();

        Merge distributedGroupByMerge = e.plan("select count('foo'), name from users group by name");
        RoutedCollectPhase collectPhase =
            ((RoutedCollectPhase) ((Collect) ((Merge) distributedGroupByMerge.subPlan()).subPlan()).collectPhase());
        assertThat(collectPhase.toCollect(), contains(isReference("name")));
        GroupProjection groupProjection = (GroupProjection) collectPhase.projections().get(0);
        assertThat(groupProjection.values().get(0), isAggregation("count"));
    }

    @Test
    public void testGroupByWithAggregationPlan() throws Exception {
        var e = SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of())
            .addTable(TableDefinitions.USER_TABLE_DEFINITION)
            .build();
        Merge distributedGroupByMerge = e.plan(
            "select count(*), name from users group by name");

        Merge reducerMerge = (Merge) distributedGroupByMerge.subPlan();

        // distributed collect
        RoutedCollectPhase collectPhase = ((RoutedCollectPhase) ((Collect) reducerMerge.subPlan()).collectPhase());
        assertThat(collectPhase.maxRowGranularity(), is(RowGranularity.DOC));
        assertThat(collectPhase.nodeIds().size(), is(2));
        assertThat(collectPhase.toCollect().size(), is(1));
        assertThat(collectPhase.projections().size(), is(1));
        assertThat(collectPhase.projections().get(0), instanceOf(GroupProjection.class));
        assertThat(collectPhase.outputTypes().size(), is(2));
        assertEquals(DataTypes.STRING, collectPhase.outputTypes().get(0));
        assertEquals(CountAggregation.LongStateType.INSTANCE, collectPhase.outputTypes().get(1));

        MergePhase mergePhase = reducerMerge.mergePhase();

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
        assertThat(localMerge.nodeIds().iterator().next(), is(NODE_ID));
        assertEquals(mergePhase.outputTypes(), localMerge.inputTypes());

        assertThat(localMerge.projections(), empty());
    }

    @Test
    public void testGroupByWithAggregationAndLimit() throws Exception {
        var e = SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of())
            .addTable(TableDefinitions.USER_TABLE_DEFINITION)
            .build();
        Merge distributedGroupByMerge = e.plan(
            "select count(*), name from users group by name limit 1 offset 1");

        Merge reducerMerge = (Merge) distributedGroupByMerge.subPlan();

        // distributed merge
        MergePhase distributedMergePhase = reducerMerge.mergePhase();
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
        var e = SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of()).build();
        Collect collect = e.plan(
            "select count(*), name from sys.nodes group by name");

        assertThat("number of nodeIds must be 1, otherwise there must be a merge",
            collect.resultDescription().nodeIds().size(), is(1));

        RoutedCollectPhase collectPhase = ((RoutedCollectPhase) collect.collectPhase());
        assertThat(collectPhase.projections(), contains(
            instanceOf(GroupProjection.class),
            instanceOf(EvalProjection.class)));

        GroupProjection groupProjection = (GroupProjection) collectPhase.projections().get(0);

        assertThat(Symbols.typeView(groupProjection.outputs()), contains(
            is(DataTypes.STRING),
            is(DataTypes.LONG)));

        assertThat(Symbols.typeView(collectPhase.projections().get(1).outputs()), contains(
            is(DataTypes.LONG),
            is(DataTypes.STRING)));
    }

    @Test
    public void testNonDistributedGroupByOnClusteredColumn() throws Exception {
        var e = SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of())
            .addTable(TableDefinitions.USER_TABLE_DEFINITION)
            .build();
        Merge merge = e.plan(
            "select count(*), id from users group by id limit 20");
        Collect collect = ((Collect) merge.subPlan());
        RoutedCollectPhase collectPhase = ((RoutedCollectPhase) collect.collectPhase());
        assertThat(collectPhase.projections(), contains(
            instanceOf(GroupProjection.class),
            instanceOf(TopNProjection.class),
            instanceOf(EvalProjection.class) // swaps id, count(*) output from group by to count(*), id
        ));
        assertThat(collectPhase.projections().get(0).requiredGranularity(), is(RowGranularity.SHARD));
        MergePhase mergePhase = merge.mergePhase();
        assertThat(mergePhase.projections(), contains(
            instanceOf(TopNProjection.class))
        );
    }

    @Test
    public void testNonDistributedGroupByOnClusteredColumnSorted() throws Exception {
        var e = SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of())
            .addTable(TableDefinitions.USER_TABLE_DEFINITION)
            .build();
        Merge merge = e.plan(
            "select count(*), id from users group by id order by 1 desc nulls last limit 20");
        Collect collect = ((Collect) merge.subPlan());
        RoutedCollectPhase collectPhase = ((RoutedCollectPhase) collect.collectPhase());
        List<Projection> collectProjections = collectPhase.projections();
        assertThat(collectProjections, contains(
            instanceOf(GroupProjection.class),
            instanceOf(OrderedTopNProjection.class),
            instanceOf(EvalProjection.class) // swap id, count(*) -> count(*), id
        ));
        assertThat(collectProjections.get(1), instanceOf(OrderedTopNProjection.class));
        assertThat(((OrderedTopNProjection) collectProjections.get(1)).orderBy().size(), is(1));

        assertThat(collectProjections.get(0).requiredGranularity(), is(RowGranularity.SHARD));
        MergePhase mergePhase = merge.mergePhase();
        assertThat(mergePhase.projections(), contains(
            instanceOf(TopNProjection.class)
        ));

        PositionalOrderBy positionalOrderBy = mergePhase.orderByPositions();
        assertThat(positionalOrderBy, notNullValue());
        assertThat(positionalOrderBy.indices().length, is(1));
        assertThat(positionalOrderBy.indices()[0], is(0));
        assertThat(positionalOrderBy.reverseFlags()[0], is(true));
        assertThat(positionalOrderBy.nullsFirst()[0], is(false));
    }

    @Test
    public void testNonDistributedGroupByOnClusteredColumnSortedScalar() throws Exception {
        var e = SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of())
            .addTable(TableDefinitions.USER_TABLE_DEFINITION)
            .build();
        Merge merge = e.plan(
            "select count(*) + 1, id from users group by id order by count(*) + 1 limit 20");
        Collect collect = (Collect) merge.subPlan();
        RoutedCollectPhase collectPhase = ((RoutedCollectPhase) collect.collectPhase());
        assertThat(collectPhase.projections(), contains(
            instanceOf(GroupProjection.class),
            instanceOf(OrderedTopNProjection.class),
            instanceOf(EvalProjection.class)
        ));
        assertThat(((OrderedTopNProjection) collectPhase.projections().get(1)).orderBy().size(), is(1));

        assertThat(collectPhase.projections().get(0).requiredGranularity(), is(RowGranularity.SHARD));
        MergePhase mergePhase = merge.mergePhase();
        assertThat(mergePhase.projections(), contains(
            instanceOf(TopNProjection.class)
        ));

        PositionalOrderBy positionalOrderBy = mergePhase.orderByPositions();
        assertThat(positionalOrderBy, notNullValue());
        assertThat(positionalOrderBy.indices().length, is(1));
        assertThat(positionalOrderBy.indices()[0], is(0));
        assertThat(positionalOrderBy.reverseFlags()[0], is(false));
        assertThat(positionalOrderBy.nullsFirst()[0], is(false));
    }

    @Test
    public void testGroupByWithOrderOnAggregate() throws Exception {
        var e = SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of())
            .addTable(TableDefinitions.USER_TABLE_DEFINITION)
            .build();
        Merge merge = e.plan(
            "select count(*), name from users group by name order by count(*)");

        assertThat(merge.mergePhase().orderByPositions(), notNullValue());

        Merge reducerMerge = (Merge) merge.subPlan();
        MergePhase mergePhase = reducerMerge.mergePhase();

        assertThat(mergePhase.projections(), contains(
            instanceOf(GroupProjection.class),
            instanceOf(OrderedTopNProjection.class),
            instanceOf(EvalProjection.class)
        ));
        OrderedTopNProjection topNProjection = (OrderedTopNProjection) mergePhase.projections().get(1);
        Symbol orderBy = topNProjection.orderBy().get(0);
        assertThat(orderBy, instanceOf(InputColumn.class));

        assertThat(orderBy.valueType(), Is.is(DataTypes.LONG));
    }

    @Test
    public void testHandlerSideRoutingGroupBy() throws Exception {
        var e = SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of()).build();
        Collect collect = e.plan(
            "select count(*) from sys.cluster group by name");
        // just testing the dispatching here.. making sure it is not a ESSearchNode
        RoutedCollectPhase collectPhase = ((RoutedCollectPhase) collect.collectPhase());
        assertThat(collectPhase.toCollect().get(0), instanceOf(Reference.class));
        assertThat(collectPhase.toCollect().size(), is(1));

        assertThat(collectPhase.projections(), contains(
            instanceOf(GroupProjection.class),
            instanceOf(EvalProjection.class)
        ));
    }

    @Test
    public void testCountDistinctWithGroupBy() throws Exception {
        var e = SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of())
            .addTable(TableDefinitions.USER_TABLE_DEFINITION)
            .build();
        Merge distributedGroupByMerge = e.plan(
            "select count(distinct id), name from users group by name order by count(distinct id)");
        Merge reducerMerge = (Merge) distributedGroupByMerge.subPlan();
        CollectPhase collectPhase = ((Collect) reducerMerge.subPlan()).collectPhase();

        // collect
        assertThat(collectPhase.toCollect().get(0), instanceOf(Reference.class));
        assertThat(collectPhase.toCollect().size(), is(2));
        assertThat(((Reference) collectPhase.toCollect().get(0)).column().name(), is("id"));
        assertThat(((Reference) collectPhase.toCollect().get(1)).column().name(), is("name"));
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
        MergePhase mergePhase = reducerMerge.mergePhase();
        assertThat(mergePhase.projections(), contains(
            instanceOf(GroupProjection.class),
            instanceOf(OrderedTopNProjection.class),
            instanceOf(EvalProjection.class))
        );
        Projection groupProjection1 = mergePhase.projections().get(0);
        groupProjection = (GroupProjection) groupProjection1;
        assertThat(groupProjection.keys().get(0), instanceOf(InputColumn.class));
        assertThat(((InputColumn) groupProjection.keys().get(0)).index(), is(0));
        assertThat(groupProjection.mode(), is(AggregateMode.PARTIAL_FINAL));
        assertThat(groupProjection.values().get(0), instanceOf(Aggregation.class));

        OrderedTopNProjection topNProjection = (OrderedTopNProjection) mergePhase.projections().get(1);
        Symbol collection_count = topNProjection.outputs().get(0);
        assertThat(collection_count, SymbolMatchers.isInputColumn(0));


        // handler
        MergePhase localMergeNode = distributedGroupByMerge.mergePhase();
        assertThat(localMergeNode.projections(), Matchers.emptyIterable());
    }

    @Test
    public void testGroupByHavingNonDistributed() throws Exception {
        var e = SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of())
            .addTable(TableDefinitions.USER_TABLE_DEFINITION)
            .build();
        Merge merge = e.plan(
            "select id from users group by id having id > 0");
        Collect collect = (Collect) merge.subPlan();
        RoutedCollectPhase collectPhase = ((RoutedCollectPhase) collect.collectPhase());
        assertThat(collectPhase.where(), isSQL("(doc.users.id > 0::bigint)"));
        assertThat(collectPhase.projections(), contains(
            instanceOf(GroupProjection.class)
        ));

        MergePhase localMergeNode = merge.mergePhase();
        assertThat(localMergeNode.projections(), empty());
    }

    @Test
    public void testGroupByWithHavingAndLimit() throws Exception {
        var e = SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of())
            .addTable(TableDefinitions.USER_TABLE_DEFINITION)
            .build();
        Merge planNode = e.plan(
            "select count(*), name from users group by name having count(*) > 1 limit 100");
        Merge reducerMerge = (Merge) planNode.subPlan();
        MergePhase mergePhase = reducerMerge.mergePhase(); // reducer

        Projection projection = mergePhase.projections().get(1);
        assertThat(projection, instanceOf(FilterProjection.class));
        FilterProjection filterProjection = (FilterProjection) projection;

        Symbol countArgument = ((Function) filterProjection.query()).arguments().get(0);
        assertThat(countArgument, instanceOf(InputColumn.class));
        assertThat(((InputColumn) countArgument).index(), is(1));  // pointing to second output from group projection

        // outputs: name, count(*)
        TopNProjection topN = (TopNProjection) mergePhase.projections().get(2);
        assertThat(topN.outputs().get(0).valueType(), Is.is(DataTypes.STRING));
        assertThat(topN.outputs().get(1).valueType(), Is.<DataType>is(DataTypes.LONG));


        MergePhase localMerge = planNode.mergePhase();
        // topN projection
        //      outputs: count(*), name
        topN = (TopNProjection) localMerge.projections().get(0);
        assertThat(topN.outputs().get(0).valueType(), Is.<DataType>is(DataTypes.LONG));
        assertThat(topN.outputs().get(1).valueType(), Is.<DataType>is(DataTypes.STRING));
    }

    @Test
    public void testGroupByWithHavingAndNoLimit() throws Exception {
        var e = SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of())
            .addTable(TableDefinitions.USER_TABLE_DEFINITION)
            .build();
        Merge planNode = e.plan(
            "select count(*), name from users group by name having count(*) > 1");
        Merge reducerMerge = (Merge) planNode.subPlan();
        MergePhase mergePhase = reducerMerge.mergePhase(); // reducer

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
        var e = SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of())
            .addTable(TableDefinitions.USER_TABLE_DEFINITION)
            .build();
        Merge planNode = e.plan(
            "select name, count(*) from users group by name having count(*) > 1");
        Merge reducerMerge = (Merge) planNode.subPlan();

        MergePhase reduceMergePhase = reducerMerge.mergePhase();

        // group projection
        //      outputs: name, count(*)
        // filter projection
        //      outputs: name, count(*)

        assertThat(reduceMergePhase.projections(), contains(
            instanceOf(GroupProjection.class),
            instanceOf(FilterProjection.class)
        ));
        Projection projection = reduceMergePhase.projections().get(1);
        assertThat(projection, instanceOf(FilterProjection.class));
        FilterProjection filterProjection = (FilterProjection) projection;

        Symbol countArgument = ((Function) filterProjection.query()).arguments().get(0);
        assertThat(countArgument, instanceOf(InputColumn.class));
        assertThat(((InputColumn) countArgument).index(), is(1));  // pointing to second output from group projection

        // outputs: name, count(*)
        assertThat(((InputColumn) filterProjection.outputs().get(0)).index(), is(0));
        assertThat(((InputColumn) filterProjection.outputs().get(1)).index(), is(1));

        MergePhase localMerge = planNode.mergePhase();
        assertThat(localMerge.projections(), empty());
    }

    @Test
    public void testGroupByHavingAndNoSelectListReOrderingWithLimit() throws Exception {
        var e = SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of())
            .addTable(TableDefinitions.USER_TABLE_DEFINITION)
            .build();
        Merge planNode = e.plan(
            "select name, count(*) from users group by name having count(*) > 1 limit 100");
        Merge reducerMerge = (Merge) planNode.subPlan();

        MergePhase reducePhase = reducerMerge.mergePhase();

        // group projection
        //      outputs: name, count(*)
        // filter projection
        //      outputs: name, count(*)
        // topN projection
        //      outputs: name, count(*)

        Projection projection = reducePhase.projections().get(1);
        assertThat(projection, instanceOf(FilterProjection.class));
        FilterProjection filterProjection = (FilterProjection) projection;

        Symbol countArgument = ((Function) filterProjection.query()).arguments().get(0);
        assertThat(countArgument, instanceOf(InputColumn.class));
        assertThat(((InputColumn) countArgument).index(), is(1));  // pointing to second output from group projection

        // outputs: name, count(*)
        assertThat(((InputColumn) filterProjection.outputs().get(0)).index(), is(0));
        assertThat(((InputColumn) filterProjection.outputs().get(1)).index(), is(1));

        // outputs: name, count(*)
        TopNProjection topN = (TopNProjection) reducePhase.projections().get(2);
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
    public void testDistributedGroupByProjectionHasShardLevelGranularity() throws Exception {
        var e = SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of())
            .addTable(TableDefinitions.USER_TABLE_DEFINITION)
            .build();
        Merge distributedGroupByMerge = e.plan("select count(*) from users group by name");
        Merge reduceMerge = (Merge) distributedGroupByMerge.subPlan();
        CollectPhase collectPhase = ((Collect) reduceMerge.subPlan()).collectPhase();
        assertThat(collectPhase.projections().size(), is(1));
        assertThat(collectPhase.projections().get(0), instanceOf(GroupProjection.class));
        assertThat(collectPhase.projections().get(0).requiredGranularity(), is(RowGranularity.SHARD));
    }

    @Test
    public void testNonDistributedGroupByProjectionHasShardLevelGranularity() throws Exception {
        var e = SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of())
            .addTable(TableDefinitions.USER_TABLE_DEFINITION)
            .build();
        Merge distributedGroupByMerge = e.plan("select count(distinct id), name from users" +
                                               " group by name order by count(distinct id)");
        Merge reduceMerge = (Merge) distributedGroupByMerge.subPlan();
        CollectPhase collectPhase = ((Collect) reduceMerge.subPlan()).collectPhase();
        assertThat(collectPhase.projections().size(), is(1));
        assertThat(collectPhase.projections().get(0), instanceOf(GroupProjection.class));
        assertThat(collectPhase.projections().get(0).requiredGranularity(), is(RowGranularity.SHARD));
    }

    @Test
    public void testNoDistributedGroupByOnAllPrimaryKeys() throws Exception {
        var e = SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of())
            .addPartitionedTable(
                "create table doc.empty_parted (" +
                "   id integer primary key," +
                "   date timestamp with time zone primary key" +
                ") clustered by (id) partitioned by (date)"
            ).build();
        Collect collect = e.plan(
            "select count(*), id, date from empty_parted group by id, date limit 20");
        RoutedCollectPhase collectPhase = ((RoutedCollectPhase) collect.collectPhase());
        assertThat(collectPhase.projections().size(), is(3));
        assertThat(collectPhase.projections().get(0), instanceOf(GroupProjection.class));
        assertThat(collectPhase.projections().get(0).requiredGranularity(), is(RowGranularity.SHARD));
        assertThat(collectPhase.projections().get(1), instanceOf(TopNProjection.class));
        assertThat(collectPhase.projections().get(2), instanceOf(EvalProjection.class));
    }

    @Test
    public void testNonDistributedGroupByAggregationsWrappedInScalar() throws Exception {
        var e = SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of())
            .addPartitionedTable(
                "create table doc.empty_parted (" +
                "   id integer primary key," +
                "   date timestamp with time zone primary key" +
                ") clustered by (id) partitioned by (date)"
            ).build();
        Collect collect = e.plan(
            "select (count(*) + 1), id from empty_parted group by id");

        CollectPhase collectPhase = collect.collectPhase();
        assertThat(collectPhase.projections(), contains(
            instanceOf(GroupProjection.class), // shard level
            instanceOf(GroupProjection.class), // node level
            instanceOf(EvalProjection.class) // count(*) + 1
        ));
    }

    @Test
    public void testGroupByHaving() throws Exception {
        var e = SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of())
            .addTable(TableDefinitions.USER_TABLE_DEFINITION)
            .build();
        Merge distributedGroupByMerge = e.plan(
            "select avg(date), name from users group by name having min(date) > '1970-01-01'");
        Merge reduceMerge = (Merge) distributedGroupByMerge.subPlan();
        CollectPhase collectPhase = ((Collect) reduceMerge.subPlan()).collectPhase();
        assertThat(collectPhase.projections().size(), is(1));
        assertThat(collectPhase.projections().get(0), instanceOf(GroupProjection.class));

        MergePhase reducePhase = reduceMerge.mergePhase();

        assertThat(reducePhase.projections().size(), is(3));

        // grouping
        assertThat(reducePhase.projections().get(0), instanceOf(GroupProjection.class));
        GroupProjection groupProjection = (GroupProjection) reducePhase.projections().get(0);
        assertThat(groupProjection.values().size(), is(2));

        // filter the having clause
        assertThat(reducePhase.projections().get(1), instanceOf(FilterProjection.class));
        FilterProjection filterProjection = (FilterProjection) reducePhase.projections().get(1);

        assertThat(reducePhase.projections().get(2), instanceOf(EvalProjection.class));
        EvalProjection eval = (EvalProjection) reducePhase.projections().get(2);
        assertThat(eval.outputs().get(0).valueType(), Is.<DataType>is(DataTypes.DOUBLE));
        assertThat(eval.outputs().get(1).valueType(), Is.<DataType>is(DataTypes.STRING));
    }

    @Test
    public void testNestedGroupByAggregation() throws Exception {
        var e = SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of()).build();
        Collect collect = e.plan("select count(*) from (" +
                                 "  select max(load['1']) as maxLoad, hostname " +
                                 "  from sys.nodes " +
                                 "  group by hostname having max(load['1']) > 50) as nodes " +
                                 "group by hostname");
        assertThat("would require merge if more than 1 nodeIds", collect.nodeIds().size(), is(1));

        CollectPhase collectPhase = collect.collectPhase();
        assertThat(collectPhase.projections(), contains(
            instanceOf(GroupProjection.class),
            instanceOf(FilterProjection.class),
            instanceOf(EvalProjection.class),
            instanceOf(GroupProjection.class),
            instanceOf(EvalProjection.class)
        ));
        Projection firstGroupProjection = collectPhase.projections().get(0);
        assertThat(((GroupProjection) firstGroupProjection).mode(), is(AggregateMode.ITER_FINAL));

        Projection secondGroupProjection = collectPhase.projections().get(3);
        assertThat(((GroupProjection) secondGroupProjection).mode(), is(AggregateMode.ITER_FINAL));
    }

    @Test
    public void testGroupByOnClusteredByColumnPartitionedOnePartition() throws Exception {
        var e = SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of())
            .addPartitionedTable(
                "create table doc.clustered_parted (" +
                "   id integer," +
                "   date timestamp with time zone," +
                "   city string" +
                ") clustered by (city) partitioned by (date) ",
                new PartitionName(new RelationName("doc", "clustered_parted"), singletonList("1395874800000")).asIndexName(),
                new PartitionName(new RelationName("doc", "clustered_parted"), singletonList("1395961200000")).asIndexName()
            ).build();

        // only one partition hit
        Merge optimizedPlan = e.plan("select count(*), city from clustered_parted where date=1395874800000 group by city");
        Collect collect = (Collect) optimizedPlan.subPlan();

        assertThat(collect.collectPhase().projections(), contains(
            instanceOf(GroupProjection.class),
            instanceOf(EvalProjection.class)));
        assertThat(collect.collectPhase().projections().get(0), instanceOf(GroupProjection.class));

        assertThat(optimizedPlan.mergePhase().projections().size(), is(0));

        // > 1 partition hit
        ExecutionPlan executionPlan = e.plan("select count(*), city from clustered_parted where date=1395874800000 or date=1395961200000 group by city");
        assertThat(executionPlan, instanceOf(Merge.class));
        assertThat(((Merge) executionPlan).subPlan(), instanceOf(Merge.class));
    }

    @Test
    public void testGroupByOrderByPartitionedClolumn() throws Exception {
        var e = SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of())
            .addPartitionedTable(
                "create table doc.clustered_parted (" +
                "   id integer," +
                "   date timestamp with time zone," +
                "   city string" +
                ") clustered by (city) partitioned by (date) ",
                new PartitionName(new RelationName("doc", "clustered_parted"), singletonList("1395874800000")).asIndexName(),
                new PartitionName(new RelationName("doc", "clustered_parted"), singletonList("1395961200000")).asIndexName()
            ).build();
        Merge plan = e.plan("select date from clustered_parted group by date order by date");
        Merge reduceMerge = (Merge) plan.subPlan();
        OrderedTopNProjection topNProjection = (OrderedTopNProjection)reduceMerge.mergePhase().projections().get(1);

        Symbol orderBy = topNProjection.orderBy().get(0);
        assertThat(orderBy, instanceOf(InputColumn.class));
        assertThat(orderBy.valueType(), is(DataTypes.TIMESTAMPZ));
    }
}
