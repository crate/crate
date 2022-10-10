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

package io.crate.planner;

import static io.crate.planner.operators.LogicalPlannerTest.isPlan;
import static io.crate.testing.Asserts.assertThrowsMatches;
import static io.crate.testing.Asserts.isReference;
import static io.crate.testing.TestingHelpers.isSQL;
import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.hamcrest.Matchers;
import org.junit.Test;

import com.carrotsearch.hppc.IntIndexedContainer;
import com.carrotsearch.randomizedtesting.RandomizedTest;

import io.crate.analyze.TableDefinitions;
import io.crate.data.RowN;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.exceptions.VersioningValidationException;
import io.crate.execution.dsl.phases.ExecutionPhase;
import io.crate.execution.dsl.phases.MergePhase;
import io.crate.execution.dsl.phases.NodeOperation;
import io.crate.execution.dsl.phases.NodeOperationTree;
import io.crate.execution.dsl.phases.PKLookupPhase;
import io.crate.execution.dsl.phases.RoutedCollectPhase;
import io.crate.execution.dsl.projection.AggregationProjection;
import io.crate.execution.dsl.projection.EvalProjection;
import io.crate.execution.dsl.projection.FilterProjection;
import io.crate.execution.dsl.projection.GroupProjection;
import io.crate.execution.dsl.projection.MergeCountProjection;
import io.crate.execution.dsl.projection.OrderedLimitAndOffsetProjection;
import io.crate.execution.dsl.projection.ProjectSetProjection;
import io.crate.execution.dsl.projection.Projection;
import io.crate.execution.dsl.projection.ProjectionType;
import io.crate.execution.dsl.projection.LimitDistinctProjection;
import io.crate.execution.dsl.projection.LimitAndOffsetProjection;
import io.crate.execution.dsl.projection.WindowAggProjection;
import io.crate.execution.engine.NodeOperationTreeGenerator;
import io.crate.execution.engine.aggregation.impl.CountAggregation;
import io.crate.expression.symbol.AggregateMode;
import io.crate.expression.symbol.Aggregation;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolType;
import io.crate.metadata.PartitionName;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.metadata.Routing;
import io.crate.metadata.RowGranularity;
import io.crate.planner.node.dql.Collect;
import io.crate.planner.node.dql.CountPlan;
import io.crate.planner.node.dql.QueryThenFetch;
import io.crate.planner.node.dql.join.Join;
import io.crate.planner.node.dql.join.JoinType;
import io.crate.planner.operators.LogicalPlan;
import io.crate.statistics.Stats;
import io.crate.statistics.TableStats;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.Asserts;
import io.crate.testing.SQLExecutor;
import io.crate.testing.T3;
import io.crate.testing.TestingHelpers;
import io.crate.types.DataTypes;

public class SelectPlannerTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void testHandlerSideRouting() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService).build();
        // just testing the dispatching here.. making sure it is not a ESSearchNode
        e.plan("select * from sys.cluster");
    }

    @Test
    public void testWherePKAndMatchDoesNotResultInESGet() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of())
            .addTable(TableDefinitions.USER_TABLE_DEFINITION)
            .build();

        ExecutionPlan plan = e.plan("select * from users where id in (1, 2, 3) and match(text, 'Hello')");
        assertThat(plan, instanceOf(Merge.class));
        assertThat(((Merge) plan).subPlan(), instanceOf(Collect.class));
    }

    @Test
    public void testGetPlan() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of())
            .addTable(TableDefinitions.USER_TABLE_DEFINITION)
            .build();

        LogicalPlan plan = e.logicalPlan("select name from users where id = 1");
        assertThat(plan, isPlan(
            "Get[doc.users | name | DocKeys{1::bigint} | (id = 1::bigint)]"));
    }

    @Test
    public void test_filter_by_internal_id_result_in_get_plan() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of())
            .addTable(TableDefinitions.USER_TABLE_DEFINITION)
            .build();

        LogicalPlan plan = e.logicalPlan("select name from users where _id = 1");
        assertThat(plan, isPlan(
            "Get[doc.users | name | DocKeys{1} | (_cast(_id, 'integer') = 1)]"));
    }

    @Test
    public void testGetWithVersion() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of())
            .addTable(TableDefinitions.USER_TABLE_DEFINITION)
            .build();

        LogicalPlan plan = e.logicalPlan("select name from users where id = 1 and _version = 1");
        assertThat(plan, isPlan(
            "Get[doc.users | name | DocKeys{1::bigint, 1::bigint} | ((id = 1::bigint) AND (_version = 1::bigint))]"));
    }

    @Test
    public void testGetPlanStringLiteral() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of())
            .addTable(
                "create table doc.bystring (" +
                "  name text primary key," +
                "  score double precision" +
                ")")
            .build();

        LogicalPlan plan = e.logicalPlan("select name from bystring where name = 'one'");
        assertThat(plan, isPlan(
            "Get[doc.bystring | name | DocKeys{'one'} | (name = 'one')]"
        ));
    }

    @Test
    public void testGetPlanPartitioned() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of())
            .addPartitionedTable(
                TableDefinitions.PARTED_PKS_TABLE_DEFINITION,
                new PartitionName(new RelationName("doc", "parted_pks"), singletonList("1395874800000")).asIndexName(),
                new PartitionName(new RelationName("doc", "parted_pks"), singletonList("1395961200000")).asIndexName()
            )
            .build();

        LogicalPlan plan = e.logicalPlan("select name, date from parted_pks where id = 1 and date = 0");
        assertThat(plan, isPlan(
            "Get[doc.parted_pks | name, date | DocKeys{1, 0::bigint} | ((id = 1) AND (date = 0::bigint))]"
        ));
    }

    @Test
    public void testMultiGetPlan() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of())
            .addTable(TableDefinitions.USER_TABLE_DEFINITION)
            .build();

        LogicalPlan plan = e.logicalPlan("select name from users where id in (1, 2)");
        assertThat(plan, isPlan(
            "Get[doc.users | name | DocKeys{1::bigint; 2::bigint} | (id = ANY([1::bigint, 2::bigint]))]"
        ));
    }

    @Test
    public void testGlobalAggregationPlan() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of())
            .addTable(TableDefinitions.USER_TABLE_DEFINITION)
            .build();

        Merge globalAggregate = e.plan("select count(name) from users");
        Collect collect = (Collect) globalAggregate.subPlan();
        RoutedCollectPhase collectPhase = ((RoutedCollectPhase) collect.collectPhase());

        assertEquals(CountAggregation.LongStateType.INSTANCE, collectPhase.outputTypes().get(0));
        assertThat(collectPhase.maxRowGranularity(), is(RowGranularity.DOC));
        assertThat(collectPhase.projections().size(), is(1));
        assertThat(collectPhase.projections().get(0), instanceOf(AggregationProjection.class));
        assertThat(collectPhase.projections().get(0).requiredGranularity(), is(RowGranularity.SHARD));

        MergePhase mergePhase = globalAggregate.mergePhase();

        assertEquals(CountAggregation.LongStateType.INSTANCE, mergePhase.inputTypes().iterator().next());
        assertEquals(DataTypes.LONG, mergePhase.outputTypes().get(0));
    }

    @Test
    public void testShardSelectWithOrderBy() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of())
            // need to have at least one table so there are some shards to have a distributed plan
            .addTable(TableDefinitions.USER_TABLE_DEFINITION)
            .build();

        Merge merge = e.plan("select id from sys.shards order by id limit 10");
        Collect collect = (Collect) merge.subPlan();
        RoutedCollectPhase collectPhase = ((RoutedCollectPhase) collect.collectPhase());

        assertEquals(DataTypes.INTEGER, collectPhase.outputTypes().get(0));
        assertThat(collectPhase.maxRowGranularity(), is(RowGranularity.SHARD));

        assertThat(collectPhase.orderBy(), notNullValue());

        List<Projection> projections = collectPhase.projections();
        assertThat(projections, contains(
            instanceOf(LimitAndOffsetProjection.class)
        ));
    }

    @Test
    public void testCollectAndMergePlan() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of())
            .addTable(TableDefinitions.USER_TABLE_DEFINITION)
            .build();

        QueryThenFetch qtf = e.plan("select name from users where name = 'x' order by id limit 10");
        Merge merge = (Merge) qtf.subPlan();
        RoutedCollectPhase collectPhase = ((RoutedCollectPhase) ((Collect) merge.subPlan()).collectPhase());
        assertThat(collectPhase.where().toString(), is("(name = 'x')"));

        LimitAndOffsetProjection limitAndOffsetProjection = (LimitAndOffsetProjection) collectPhase.projections().get(0);
        assertThat(limitAndOffsetProjection.limit(), is(10));

        MergePhase mergePhase = merge.mergePhase();
        assertThat(mergePhase.outputTypes().size(), is(1));
        assertEquals(DataTypes.STRING, mergePhase.outputTypes().get(0));
    }

    @Test
    public void testCollectAndMergePlanNoFetch() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of())
            .addTable(TableDefinitions.USER_TABLE_DEFINITION)
            .build();

        // testing that a fetch projection is not added if all output symbols are included
        // at the orderBy symbols
        Merge merge = e.plan("select name from users where name = 'x' order by name limit 10");
        Collect collect = (Collect) merge.subPlan();
        RoutedCollectPhase collectPhase = ((RoutedCollectPhase) collect.collectPhase());
        assertThat(collectPhase.where().toString(), is("(name = 'x')"));

        MergePhase mergePhase = merge.mergePhase();
        assertThat(mergePhase.outputTypes().size(), is(1));
        assertEquals(DataTypes.STRING, mergePhase.outputTypes().get(0));

        assertTrue(mergePhase.finalProjection().isPresent());

        Projection lastProjection = mergePhase.finalProjection().get();
        assertThat(lastProjection, instanceOf(LimitAndOffsetProjection.class));
        LimitAndOffsetProjection limitAndOffsetProjection = (LimitAndOffsetProjection) lastProjection;
        assertThat(limitAndOffsetProjection.outputs().size(), is(1));
    }

    @Test
    public void testCollectAndMergePlanHighLimit() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of())
            .addTable(TableDefinitions.USER_TABLE_DEFINITION)
            .build();

        QueryThenFetch qtf = e.plan("select name from users limit 100000");
        Merge merge = (Merge) qtf.subPlan();
        RoutedCollectPhase collectPhase = ((RoutedCollectPhase) ((Collect) merge.subPlan()).collectPhase());
        assertThat(collectPhase.nodePageSizeHint(), is(100_000));

        MergePhase mergePhase = merge.mergePhase();
        assertThat(mergePhase.projections().size(), is(2));
        LimitAndOffsetProjection projection = (LimitAndOffsetProjection) mergePhase.projections().get(0);
        assertThat(projection.limit(), is(100_000));
        assertThat(projection.offset(), is(0));

        // with offset
        qtf = e.plan("select name from users limit 100000 offset 20");
        merge = ((Merge) qtf.subPlan());

        collectPhase = ((RoutedCollectPhase) ((Collect) merge.subPlan()).collectPhase());
        assertThat(collectPhase.nodePageSizeHint(), is(100_000 + 20));

        mergePhase = merge.mergePhase();
        assertThat(mergePhase.projections().size(), is(2));
        projection = (LimitAndOffsetProjection) mergePhase.projections().get(0);
        assertThat(projection.limit(), is(100_000));
        assertThat(projection.offset(), is(20));
    }


    @Test
    public void testCollectAndMergePlanPartitioned() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of())
            .addPartitionedTable(
                TableDefinitions.PARTED_PKS_TABLE_DEFINITION,
                new PartitionName(new RelationName("doc", "parted_pks"), singletonList("1395874800000")).asIndexName(),
                new PartitionName(new RelationName("doc", "parted_pks"), singletonList("1395961200000")).asIndexName()
            )
            .build();

        QueryThenFetch qtf = e.plan("select id, name, date from parted_pks where date > 0 and name = 'x' order by id limit 10");
        Merge merge = (Merge) qtf.subPlan();
        RoutedCollectPhase collectPhase = ((RoutedCollectPhase) ((Collect) merge.subPlan()).collectPhase());

        Set<String> indices = new HashSet<>();
        Map<String, Map<String, IntIndexedContainer>> locations = collectPhase.routing().locations();
        for (Map.Entry<String, Map<String, IntIndexedContainer>> entry : locations.entrySet()) {
            indices.addAll(entry.getValue().keySet());
        }
        assertThat(indices, Matchers.containsInAnyOrder(
            new PartitionName(new RelationName("doc", "parted_pks"), Arrays.asList("1395874800000")).asIndexName(),
            new PartitionName(new RelationName("doc", "parted_pks"), Arrays.asList("1395961200000")).asIndexName()));

        assertThat(collectPhase.where().toString(), is("(name = 'x')"));

        MergePhase mergePhase = merge.mergePhase();
        assertThat(mergePhase.outputTypes().size(), is(3));
    }

    @Test
    public void testCollectAndMergePlanFunction() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of())
            .addTable(TableDefinitions.USER_TABLE_DEFINITION)
            .build();

        QueryThenFetch qtf = e.plan("select format('Hi, my name is %s', name), name from users where name = 'x' order by id limit 10");
        Merge merge = (Merge) qtf.subPlan();
        RoutedCollectPhase collectPhase = ((RoutedCollectPhase) ((Collect) merge.subPlan()).collectPhase());

        assertThat(collectPhase.where().toString(), is("(name = 'x')"));

        MergePhase mergePhase = merge.mergePhase();
        assertThat(mergePhase.outputTypes().size(), is(2));
        assertEquals(DataTypes.STRING, mergePhase.outputTypes().get(0));
        assertEquals(DataTypes.STRING, mergePhase.outputTypes().get(1));
    }

    @Test
    public void testCountDistinctPlan() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of())
            .addTable(TableDefinitions.USER_TABLE_DEFINITION)
            .build();

        Merge globalAggregate = e.plan("select count(distinct name) from users");
        Collect collect = (Collect) globalAggregate.subPlan();

        RoutedCollectPhase collectPhase = ((RoutedCollectPhase) collect.collectPhase());
        Projection projection = collectPhase.projections().get(0);
        assertThat(projection, instanceOf(AggregationProjection.class));
        AggregationProjection aggregationProjection = (AggregationProjection) projection;
        assertThat(aggregationProjection.aggregations().size(), is(1));
        assertThat(aggregationProjection.mode(), is(AggregateMode.ITER_PARTIAL));

        Aggregation aggregation = aggregationProjection.aggregations().get(0);
        Symbol aggregationInput = aggregation.inputs().get(0);
        assertThat(aggregationInput.symbolType(), is(SymbolType.INPUT_COLUMN));

        assertThat(collectPhase.toCollect().get(0), instanceOf(Reference.class));
        assertThat(((Reference) collectPhase.toCollect().get(0)).column().name(), is("name"));

        MergePhase mergePhase = globalAggregate.mergePhase();
        assertThat(mergePhase.projections().size(), is(2));
        Projection projection1 = mergePhase.projections().get(1);

        assertThat(projection1, instanceOf(EvalProjection.class));
        Symbol collection_count = projection1.outputs().get(0);
        assertThat(collection_count, instanceOf(Function.class));
    }

    @Test
    public void testGlobalAggregationHaving() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of())
            .addTable(TableDefinitions.USER_TABLE_DEFINITION)
            .build();

        Merge globalAggregate = e.plan(
            "select avg(date) from users having min(date) > '1970-01-01'");
        Collect collect = (Collect) globalAggregate.subPlan();
        RoutedCollectPhase collectPhase = ((RoutedCollectPhase) collect.collectPhase());
        assertThat(collectPhase.projections().size(), is(1));
        assertThat(collectPhase.projections().get(0), instanceOf(AggregationProjection.class));

        MergePhase localMergeNode = globalAggregate.mergePhase();

        assertThat(localMergeNode.projections(), contains(
            instanceOf(AggregationProjection.class),
            instanceOf(FilterProjection.class),
            instanceOf(EvalProjection.class)));

        AggregationProjection aggregationProjection = (AggregationProjection) localMergeNode.projections().get(0);
        assertThat(aggregationProjection.aggregations().size(), is(2));

        FilterProjection filterProjection = (FilterProjection) localMergeNode.projections().get(1);
        assertThat(filterProjection.outputs().size(), is(2));
        assertThat(filterProjection.outputs().get(0), instanceOf(InputColumn.class));
        InputColumn inputColumn = (InputColumn) filterProjection.outputs().get(0);
        assertThat(inputColumn.index(), is(0));

        EvalProjection evalProjection = (EvalProjection) localMergeNode.projections().get(2);
        assertThat(evalProjection.outputs().size(), is(1));
    }

    @Test
    public void testCountOnPartitionedTable() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of())
            .addPartitionedTable(
                "create table parted (" +
                "   id int," +
                "   name string," +
                "   date timestamp without time zone," +
                "   obj object" +
                ") partitioned by (date) clustered into 1 shards ",
                new PartitionName(new RelationName("doc", "parted"), singletonList("1395874800000")).asIndexName(),
                new PartitionName(new RelationName("doc", "parted"), singletonList("1395961200000")).asIndexName(),
                new PartitionName(new RelationName("doc", "parted"), singletonList(null)).asIndexName()
            )
            .build();

        CountPlan plan = e.plan("select count(*) from parted where date = 1395874800000");
        assertThat(
            plan.countPhase().routing().locations().entrySet().stream()
                .flatMap(x -> x.getValue().keySet().stream())
                .collect(Collectors.toSet()),
            Matchers.contains(
                is(".partitioned.parted.04732cpp6ks3ed1o60o30c1g")
            )
        );
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSelectPartitionedTableOrderByPartitionedColumnInFunction() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of())
            .addPartitionedTable(
                "create table parted (" +
                "   id int," +
                "   name string," +
                "   date timestamp without time zone," +
                "   obj object" +
                ") partitioned by (date) clustered into 1 shards ",
                new PartitionName(new RelationName("doc", "parted"), singletonList("1395874800000")).asIndexName(),
                new PartitionName(new RelationName("doc", "parted"), singletonList("1395961200000")).asIndexName(),
                new PartitionName(new RelationName("doc", "parted"), singletonList(null)).asIndexName()
            )
            .build();

        e.plan("select name from parted order by year(date)");
    }

    @Test(expected = UnsupportedFeatureException.class)
    public void testQueryRequiresScalar() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of())
            .build();

        // only scalar functions are allowed on system tables because we have no lucene queries
        e.plan("select * from sys.shards where match(table_name, 'characters')");
    }

    @Test
    public void testSortOnUnknownColumn() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of())
            .addTable(
                "create table doc.ignored_nested (" +
                "  details object(ignored)" +
                ")")
            .build();

        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Cannot ORDER BY 'details['unknown_column']': invalid data type 'undefined'.");
        e.plan("select details from ignored_nested order by details['unknown_column']");
    }

    @Test
    public void testSelectAnalyzedReferenceInFunctionAggregation() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of())
            .addTable(TableDefinitions.USER_TABLE_DEFINITION)
            .build();

        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Cannot select analyzed column 'text' within grouping or aggregations");
        e.plan("select min(substr(text, 0, 2)) from users");
    }

    @Test
    public void testGlobalAggregateWithWhereOnPartitionColumn() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of())
            .addPartitionedTable(
                "create table parted (" +
                "   id int," +
                "   name string," +
                "   date timestamp without time zone," +
                "   obj object" +
                ") partitioned by (date) clustered into 1 shards ",
                new PartitionName(new RelationName("doc", "parted"), singletonList("1395874800000")).asIndexName(),
                new PartitionName(new RelationName("doc", "parted"), singletonList("1395961200000")).asIndexName(),
                new PartitionName(new RelationName("doc", "parted"), singletonList(null)).asIndexName()
            )
            .build();

        ExecutionPlan plan = e.plan(
            "select min(name) from parted where date >= 1395961200000");
        Collect collect;
        if (plan instanceof Merge) {
            collect = ((Collect) ((Merge) plan).subPlan());
        } else {
            collect = (Collect) plan;
        }
        Routing routing = ((RoutedCollectPhase) collect.collectPhase()).routing();

        assertThat(
            routing.locations().values()
                .stream()
                .flatMap(shardsByIndex -> shardsByIndex.keySet().stream())
                .collect(Collectors.toSet()),
            contains(
                is(".partitioned.parted.04732cpp6ksjcc9i60o30c1g")
            ));
    }

    @Test
    public void testHasNoResultFromHaving() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of())
            .addTable(TableDefinitions.USER_TABLE_DEFINITION)
            .build();

        Merge merge = e.plan("select min(name) from users having 1 = 2");
        assertThat(merge.mergePhase().projections().get(1), instanceOf(FilterProjection.class));
        assertThat(((FilterProjection) merge.mergePhase().projections().get(1)).query(), isSQL("false"));
    }

    @Test
    public void testShardQueueSizeCalculation() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of())
            .addTable(TableDefinitions.USER_TABLE_DEFINITION)
            .build();

        Merge merge = e.plan("select name from users order by name limit 500");
        Collect collect = (Collect) merge.subPlan();
        int shardQueueSize = ((RoutedCollectPhase) collect.collectPhase()).shardQueueSize(
            collect.collectPhase().nodeIds().iterator().next());
        assertThat(shardQueueSize, is(375));
    }

    @Test
    public void testQAFPagingIsEnabledOnHighLimit() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of())
            .addTable(TableDefinitions.USER_TABLE_DEFINITION)
            .build();

        Merge plan = e.plan("select name from users order by name limit 1000000");
        assertThat(plan.mergePhase().nodeIds().size(), is(1)); // mergePhase with executionNode = paging enabled

        Collect collect = (Collect) plan.subPlan();
        assertThat(((RoutedCollectPhase) collect.collectPhase()).nodePageSizeHint(), is(750000));
    }

    @Test
    public void testQAFPagingIsEnabledOnHighOffset() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of())
            .addTable(TableDefinitions.USER_TABLE_DEFINITION)
            .build();

        Merge merge = e.plan("select name from users order by name limit 10 offset 1000000");
        Collect collect = (Collect) merge.subPlan();
        assertThat(merge.mergePhase().nodeIds().size(), is(1)); // mergePhase with executionNode = paging enabled
        assertThat(((RoutedCollectPhase) collect.collectPhase()).nodePageSizeHint(), is(750007));
    }

    @Test
    public void testQTFPagingIsEnabledOnHighLimit() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of())
            .addTable(TableDefinitions.USER_TABLE_DEFINITION)
            .build();

        QueryThenFetch qtf = e.plan("select name, date from users order by name limit 1000000");
        Merge merge = (Merge) qtf.subPlan();
        RoutedCollectPhase collectPhase = ((RoutedCollectPhase) ((Collect) merge.subPlan()).collectPhase());
        assertThat(merge.mergePhase().nodeIds().size(), is(1)); // mergePhase with executionNode = paging enabled
        assertThat(collectPhase.nodePageSizeHint(), is(750000));
    }

    @Test
    public void testSelectFromUnnestResultsInTableFunctionPlan() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of())
            .build();

        Collect collect = e.plan("select * from unnest([1, 2], ['Arthur', 'Trillian'])");
        assertNotNull(collect);
        Asserts.assertThat(collect.collectPhase().toCollect()).satisfiesExactly(isReference("col1"), isReference("col2"));
    }

    @Test
    public void testReferenceToNestedAggregatedField() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .addTable(T3.T1_DEFINITION)
            .build();

        Collect collect = e.plan("select ii, xx from ( " +
                                 "  select i + i as ii, xx from (" +
                                 "    select i, sum(x) as xx from t1 group by i) as t) as tt " +
                                 "where (ii * 2) > 4 and (xx * 2) > 120");
        assertThat("would require merge with more than 1 nodeIds", collect.nodeIds().size(), is(1));
        List<Projection> projections = collect.collectPhase().projections();
        assertThat(projections, contains(
            instanceOf(GroupProjection.class), // parallel on shard-level
            instanceOf(GroupProjection.class), // node-level
            instanceOf(EvalProjection.class),
            instanceOf(FilterProjection.class),
            instanceOf(EvalProjection.class)
        ));
    }

    @Test
    public void test3TableJoinQuerySplitting() throws Exception {
        TableStats tableStats = new TableStats();
        tableStats.updateTableStats(
            Map.of(new RelationName("doc", "users"), new Stats(20, 20, Map.of())));
        SQLExecutor e = SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of())
            .addTable(TableDefinitions.USER_TABLE_DEFINITION)
            .setTableStats(tableStats)
            .build();

        Join outerNl = e.plan("select" +
                                    "  u1.id as u1, " +
                                    "  u2.id as u2, " +
                                    "  u3.id as u3 " +
                                    "from " +
                                    "  users u1," +
                                    "  users u2," +
                                    "  users u3 " +
                                    "where " +
                                    "  u1.name = 'Arthur'" +
                                    "  and u2.id = u1.id" +
                                    "  and u2.name = u1.name");
        Join innerNl = (Join) outerNl.left();

        assertThat(innerNl.joinPhase().joinCondition(), isSQL("((INPUT(0) = INPUT(2)) AND (INPUT(1) = INPUT(3)))"));
        assertThat(innerNl.joinPhase().projections().size(), is(1));
        assertThat(innerNl.joinPhase().projections().get(0), instanceOf(EvalProjection.class));

        assertThat(outerNl.joinPhase().joinCondition(), nullValue());
        assertThat(outerNl.joinPhase().projections().size(), is(2));
        assertThat(outerNl.joinPhase().projections(), contains(
            instanceOf(EvalProjection.class),
            instanceOf(EvalProjection.class)
        ));
    }

    @Test
    public void testOuterJoinToInnerJoinRewrite() throws Exception {
        TableStats tableStats = new TableStats();
        tableStats.updateTableStats(
            Map.of(new RelationName("doc", "users"), new Stats(20, 20, Map.of())));
        SQLExecutor e = SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of())
            .addTable(TableDefinitions.USER_TABLE_DEFINITION)
            .setTableStats(tableStats)
            .build();

        // disable hash joins otherwise it will be a distributed join and the plan differs
        e.getSessionSettings().setHashJoinEnabled(false);
        Merge merge = e.plan("select u1.text, concat(u2.text, '_foo') " +
                                    "from users u1 left join users u2 on u1.id = u2.id " +
                                    "where u2.name = 'Arthur'" +
                                    "and u2.id > 1 ");
        Join nl = (Join) merge.subPlan();
        assertThat(nl.joinPhase().joinType(), is(JoinType.INNER));
        Collect rightCM = (Collect) nl.right();
        assertThat(((RoutedCollectPhase) rightCM.collectPhase()).where(),
            isSQL("((doc.users.name = 'Arthur') AND (doc.users.id > 1::bigint))"));
    }

    @Test
    public void testShardSelect() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of())
            // Need to have at least one table to have some shards for a distributed plan
            .addTable(TableDefinitions.USER_TABLE_DEFINITION)
            .build();

        Merge merge = e.plan("select id from sys.shards");
        Collect collect = (Collect) merge.subPlan();
        RoutedCollectPhase collectPhase = ((RoutedCollectPhase) collect.collectPhase());
        assertThat(collectPhase.maxRowGranularity(), is(RowGranularity.SHARD));
    }

    @Test
    public void testGlobalCountPlan() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of())
            .addTable(TableDefinitions.USER_TABLE_DEFINITION)
            .build();

        CountPlan plan = e.plan("select count(*) from users");

        assertThat(plan.countPhase().where(), equalTo(Literal.BOOLEAN_TRUE));

        assertThat(plan.mergePhase().projections().size(), is(1));
        assertThat(plan.mergePhase().projections().get(0), instanceOf(MergeCountProjection.class));
    }

    @Test
    public void testLimitThatIsBiggerThanPageSizeCausesQTFPUshPlan() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of())
            .addTable(TableDefinitions.USER_TABLE_DEFINITION)
            .build();

        QueryThenFetch qtf = e.plan("select * from users limit 2147483647 ");
        Merge merge = (Merge) qtf.subPlan();
        assertThat(merge.mergePhase().nodeIds().size(), is(1));
        String localNodeId = merge.mergePhase().nodeIds().iterator().next();
        NodeOperationTree operationTree = NodeOperationTreeGenerator.fromPlan(merge, localNodeId);
        NodeOperation nodeOperation = operationTree.nodeOperations().iterator().next();
        // paging -> must not use direct response
        assertThat(nodeOperation.downstreamNodes(), not(contains(ExecutionPhase.DIRECT_RESPONSE)));


        qtf = e.plan("select * from users limit 2");
        merge = (Merge) qtf.subPlan();
        localNodeId = merge.subPlan().resultDescription().nodeIds().iterator().next();
        operationTree = NodeOperationTreeGenerator.fromPlan(merge, localNodeId);
        nodeOperation = operationTree.nodeOperations().iterator().next();
        // no paging -> can use direct response
        assertThat(nodeOperation.downstreamNodes(), contains(ExecutionPhase.DIRECT_RESPONSE));
    }

    @Test
    public void testAggregationOnGeneratedColumns() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of())
            .addTable(
                "create table doc.gc_table (" +
                "   revenue integer," +
                "   cost integer," +
                "   profit as revenue - cost" +
                ")"
            ).build();

        Merge merge = e.plan("select sum(profit) from gc_table");
        Collect collect = (Collect) merge.subPlan();
        List<Projection> projections = collect.collectPhase().projections();
        assertThat(projections, contains(
            instanceOf(AggregationProjection.class) // iter-partial on shard level
        ));
        assertThat(
            merge.mergePhase().projections(),
            contains(instanceOf(AggregationProjection.class))
        );
        assertThat(
            ((AggregationProjection)projections.get(0)).aggregations().get(0).inputs().get(0),
            isSQL("INPUT(0)"));
    }

    @Test
    public void testGlobalAggregationOn3TableJoinWithImplicitJoinConditions() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of())
            .addTable(TableDefinitions.USER_TABLE_DEFINITION)
            .build();

        Merge plan = e.plan("select count(*) from users t1, users t2, users t3 " +
                            "where t1.id = t2.id and t2.id = t3.id");
        assertThat(plan.subPlan(), instanceOf(Join.class));
        Join outerNL = (Join)plan.subPlan();
        assertThat(outerNL.joinPhase().joinCondition(), isSQL("(INPUT(1) = INPUT(2))"));
        assertThat(outerNL.joinPhase().projections().size(), is(2));
        assertThat(outerNL.joinPhase().projections().get(0), instanceOf(EvalProjection.class));
        assertThat(outerNL.joinPhase().projections().get(1), instanceOf(AggregationProjection.class));
        assertThat(outerNL.joinPhase().outputTypes().size(), is(1));
        assertThat(outerNL.joinPhase().outputTypes().get(0), is(CountAggregation.LongStateType.INSTANCE));

        Join innerNL = (Join) outerNL.left();
        assertThat(innerNL.joinPhase().joinCondition(), isSQL("(INPUT(0) = INPUT(1))"));
        assertThat(innerNL.joinPhase().projections().size(), is(1));
        assertThat(innerNL.joinPhase().projections().get(0), instanceOf(EvalProjection.class));
        assertThat(innerNL.joinPhase().outputTypes().size(), is(2));
        assertThat(innerNL.joinPhase().outputTypes().get(0), is(DataTypes.LONG));

        plan = e.plan("select count(t1.other_id) from users t1, users t2, users t3 " +
                      "where t1.id = t2.id and t2.id = t3.id");
        assertThat(plan.subPlan(), instanceOf(Join.class));
        outerNL = (Join)plan.subPlan();
        assertThat(outerNL.joinPhase().joinCondition(), isSQL("(INPUT(2) = INPUT(3))"));
        assertThat(outerNL.joinPhase().projections().size(), is(2));
        assertThat(outerNL.joinPhase().projections().get(0), instanceOf(EvalProjection.class));
        assertThat(outerNL.joinPhase().projections().get(1), instanceOf(AggregationProjection.class));
        assertThat(outerNL.joinPhase().outputTypes().size(), is(1));
        assertThat(outerNL.joinPhase().outputTypes().get(0), is(CountAggregation.LongStateType.INSTANCE));

        innerNL = (Join) outerNL.left();
        assertThat(innerNL.joinPhase().joinCondition(), isSQL("(INPUT(1) = INPUT(2))"));
        assertThat(innerNL.joinPhase().projections().size(), is(1));
        assertThat(innerNL.joinPhase().projections().get(0), instanceOf(EvalProjection.class));
        assertThat(innerNL.joinPhase().outputTypes().size(), is(3));
        assertThat(innerNL.joinPhase().outputTypes().get(0), is(DataTypes.LONG));
        assertThat(innerNL.joinPhase().outputTypes().get(1), is(DataTypes.LONG));
    }

    @Test
    public void test2TableJoinWithNoMatch() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of())
            .addTable(TableDefinitions.USER_TABLE_DEFINITION)
            .build();

        Join nl = e.plan("select * from users t1, users t2 WHERE 1=2");
        assertThat(nl.left(), instanceOf(Collect.class));
        assertThat(nl.right(), instanceOf(Collect.class));
        assertThat(((RoutedCollectPhase)((Collect)nl.left()).collectPhase()).where(), isSQL("false"));
        assertThat(((RoutedCollectPhase)((Collect)nl.right()).collectPhase()).where(), isSQL("false"));
    }

    @Test
    public void test3TableJoinWithNoMatch() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of())
            .addTable(TableDefinitions.USER_TABLE_DEFINITION)
            .build();

        Join outer = e.plan("select * from users t1, users t2, users t3 WHERE 1=2");
        assertThat(((RoutedCollectPhase)((Collect)outer.right()).collectPhase()).where(), isSQL("false"));
        Join inner = (Join) outer.left();
        Asserts.assertThat(((RoutedCollectPhase)((Collect)inner.left()).collectPhase()).where()).isLiteral(false);
        Asserts.assertThat(((RoutedCollectPhase)((Collect)inner.right()).collectPhase()).where()).isLiteral(false);
    }

    @Test
    public void testGlobalAggregateOn2TableJoinWithNoMatch() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of())
            .addTable(TableDefinitions.USER_TABLE_DEFINITION)
            .build();

        Join nl = e.plan("select count(*) from users t1, users t2 WHERE 1=2");
        assertThat(nl.left(), instanceOf(Collect.class));
        assertThat(nl.right(), instanceOf(Collect.class));
        Asserts.assertThat(((RoutedCollectPhase)((Collect)nl.left()).collectPhase()).where()).isLiteral(false);
        Asserts.assertThat(((RoutedCollectPhase)((Collect)nl.right()).collectPhase()).where()).isLiteral(false);
    }

    @Test
    public void testGlobalAggregateOn3TableJoinWithNoMatch() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of())
            .addTable(TableDefinitions.USER_TABLE_DEFINITION)
            .build();

        Join outer = e.plan("select count(*) from users t1, users t2, users t3 WHERE 1=2");
        Join inner = (Join) outer.left();
        Asserts.assertThat(((RoutedCollectPhase)((Collect)outer.right()).collectPhase()).where()).isLiteral(false);
        Asserts.assertThat(((RoutedCollectPhase)((Collect)inner.left()).collectPhase()).where()).isLiteral(false);
        Asserts.assertThat(((RoutedCollectPhase)((Collect)inner.right()).collectPhase()).where()).isLiteral(false);
    }

    @Test
    public void testFilterOnPKSubsetResultsInPKLookupPlanIfTheOtherPKPartIsGenerated() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of())
            .addTable(
                "create table t_pk_part_generated (" +
                "   ts timestamp with time zone," +
                "   p as date_trunc('day', ts)," +
                "   primary key (ts, p))")
            .build();

        LogicalPlan plan = e.logicalPlan("select 1 from t_pk_part_generated where ts = 0");
        assertThat(plan, isPlan(
            "Get[doc.t_pk_part_generated | 1 | DocKeys{0::bigint, 0::bigint} | ((ts = 0::bigint) AND (p AS date_trunc('day', ts) = 0::bigint))]"
        ));
    }

    @Test
    public void testInnerJoinResultsInHashJoinIfHashJoinIsEnabled() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of())
            .addTable(T3.T1_DEFINITION)
            .addTable(T3.T2_DEFINITION)
            .build();

        e.getSessionSettings().setHashJoinEnabled(true);
        Merge merge = e.plan("select t2.b, t1.a from t1 inner join t2 on t1.i = t2.i order by 1, 2");
        Join join = (Join) merge.subPlan();
        assertThat(join.joinPhase().type(), is(ExecutionPhase.Type.HASH_JOIN));
    }

    @Test
    public void testUnnestInSelectListResultsInPlanWithProjectSetOperator() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of())
            .build();

        LogicalPlan plan = e.logicalPlan("select unnest([1, 2])");
        assertThat(plan, isPlan(
            "ProjectSet[unnest([1, 2])]\n" +
            "  └ TableFunction[empty_row | [] | true]"));
        Symbol output = plan.outputs().get(0);
        assertThat(output.valueType(), is(DataTypes.INTEGER));
    }

    @Test
    public void testScalarCanBeUsedAroundTableGeneratingFunctionInSelectList() {
        SQLExecutor e = SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of())
            .build();

        LogicalPlan plan = e.logicalPlan("select unnest([1, 2]) + 1");
        assertThat(plan, isPlan(
            "Eval[(unnest([1, 2]) + 1)]\n" +
            "  └ ProjectSet[unnest([1, 2])]\n" +
            "    └ TableFunction[empty_row | [] | true]"));
    }

    @Test
    public void testAggregationOnTopOfTableFunctionIsNotPossibleWithoutSeparateSubQuery() {
        SQLExecutor e = SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of())
            .build();

        expectedException.expectMessage("Cannot use table functions inside aggregates");
        e.logicalPlan("select sum(unnest([1, 2]))");
    }

    @Test
    public void testTableFunctionIsExecutedAfterAggregation() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of())
            .addTable(TableDefinitions.USER_TABLE_DEFINITION)
            .build();

        LogicalPlan plan = e.logicalPlan("select count(*), generate_series(1, 2) from users");
        assertThat(plan, isPlan(
            "Eval[count(*), pg_catalog.generate_series(1, 2)]\n" +
            "  └ ProjectSet[pg_catalog.generate_series(1, 2), count(*)]\n" +
            "    └ Count[doc.users | true]"));
    }

    @Test
    public void testAggregationCanBeUsedAsArgumentToTableFunction() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of())
            .addTable(TableDefinitions.USER_TABLE_DEFINITION)
            .build();

        LogicalPlan plan = e.logicalPlan("select count(name), generate_series(1, count(name)) from users");
        assertThat(plan, isPlan(
            "Eval[count(name), pg_catalog.generate_series(1::bigint, count(name))]\n" +
            "  └ ProjectSet[pg_catalog.generate_series(1::bigint, count(name)), count(name)]\n" +
            "    └ HashAggregate[count(name)]\n" +
            "      └ Collect[doc.users | [name] | true]"));
    }

    @Test
    public void testOrderByOnTableFunctionMustOrderAfterProjectSet() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of())
            .build();

        LogicalPlan plan = e.logicalPlan("select unnest([1, 2]) from sys.nodes order by 1");
        assertThat(plan, isPlan(
            "OrderBy[unnest([1, 2]) ASC]\n" +
            "  └ ProjectSet[unnest([1, 2])]\n" +
            "    └ Collect[sys.nodes | [] | true]"));
    }

    @Test
    public void testWindowFunctionsWithPartitionByAreExecutedDistributed() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of())
            .addTable(TableDefinitions.USER_TABLE_DEFINITION)
            .build();

        Merge localMerge = e.plan("select sum(ints) OVER (partition by awesome) from users");
        Merge distMerge = (Merge) localMerge.subPlan();
        assertThat(distMerge.nodeIds().size(), is(2));
        assertThat(distMerge.mergePhase().projections(), contains(
            instanceOf(WindowAggProjection.class),
            instanceOf(EvalProjection.class)
        ));
        Collect collect = (Collect) distMerge.subPlan();
        assertThat(collect.nodeIds().size(), is(2));
    }

    @Test
    public void testSeqNoAndPrimaryTermFilteringRequirePrimaryKey() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of())
            .addTable(TableDefinitions.USER_TABLE_DEFINITION)
            .build();

        expectedException.expect(VersioningValidationException.class);
        expectedException.expectMessage(VersioningValidationException.SEQ_NO_AND_PRIMARY_TERM_USAGE_MSG);
        e.plan("select * from users where _seq_no = 2 and _primary_term = 1");
    }


    @Test
    public void testTablePartitionsAreNarrowedToMatchWhereClauseOfParentQuery() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of())
            .addPartitionedTable(
                "create table parted (" +
                "   id int," +
                "   name string," +
                "   date timestamp without time zone," +
                "   obj object" +
                ") partitioned by (date) clustered into 1 shards ",
                new PartitionName(new RelationName("doc", "parted"), singletonList("1395874800000")).asIndexName(),
                new PartitionName(new RelationName("doc", "parted"), singletonList("1395961200000")).asIndexName(),
                new PartitionName(new RelationName("doc", "parted"), singletonList(null)).asIndexName()
            )
            .build();

        String statement = "select * from (select * from parted) t where date is null";
        LogicalPlan logicalPlan = e.logicalPlan(statement);
        assertThat(logicalPlan, isPlan(
            "Rename[id, name, date, obj] AS t\n" +
            "  └ Collect[doc.parted | [id, name, date, obj] | (date IS NULL)]"));
        ExecutionPlan plan = e.plan(statement);
        Collect collect = plan instanceof Collect ? (Collect) plan : ((Collect) ((Merge) plan).subPlan());
        RoutedCollectPhase routedCollectPhase = (RoutedCollectPhase) collect.collectPhase();

        int numShards = 0;
        for (String node : routedCollectPhase.routing().nodes()) {
            numShards += routedCollectPhase.routing().numShards(node);
        }
        assertThat(numShards, is(1));
    }

    @Test
    public void test_match_used_on_table_with_alias_is_resolved_to_a_function() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of())
            .addTable(TableDefinitions.USER_TABLE_DEFINITION)
            .build();

        Merge merge = e.plan("select name from users as u where match(u.text, 'yalla') order by 1");
        Collect collect = (Collect) merge.subPlan();
        Asserts.assertThat(((RoutedCollectPhase) collect.collectPhase()).where()).isFunction("match");
    }

    @Test
    public void test_distinct_with_limit_is_optimized_to_limitandoffset_distinct() throws Exception {
        TableStats tableStats = new TableStats();
        tableStats.updateTableStats(
            Map.of(new RelationName("doc", "users"), new Stats(20, 20, Map.of())));
        SQLExecutor e = SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of())
            .addTable(TableDefinitions.USER_TABLE_DEFINITION)
            .setTableStats(tableStats)
            .build();

        String stmt = "select distinct name from users limit 1";
        LogicalPlan plan = e.logicalPlan(stmt);
        assertThat(plan, isPlan(
            "LimitDistinct[1::bigint;0 | [name]]\n" +
            "  └ Collect[doc.users | [name] | true]"));
    }

    @Test
    public void test_group_by_without_aggregates_and_with_limit_is_optimized_to_limitandoffset_distinct() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of())
            .addTable(TableDefinitions.USER_TABLE_DEFINITION)
            .build();

        String stmt = "select id, name from users group by id, name limit 1";
        LogicalPlan plan = e.logicalPlan(stmt);
        assertThat(plan, isPlan(
            "LimitDistinct[1::bigint;0 | [id, name]]\n" +
            "  └ Collect[doc.users | [id, name] | true]"));
    }

    @Test
    public void test_distinct_with_limit_and_offset_keeps_offset() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of())
            .addTable(TableDefinitions.USER_TABLE_DEFINITION)
            .build();

        String stmt = "select id, name from users group by id, name limit 1 offset 3";
        LogicalPlan plan = e.logicalPlan(stmt);
        assertThat(plan, isPlan(
            "LimitDistinct[1::bigint;3::bigint | [id, name]]\n" +
            "  └ Collect[doc.users | [id, name] | true]"));

        Merge merge = e.plan(stmt);
        List<Projection> collectProjections = ((Collect) merge.subPlan()).collectPhase().projections();;
        assertThat(
            collectProjections,
            contains(
                instanceOf(LimitDistinctProjection.class)
            )
        );
        List<Projection> mergeProjections = merge.mergePhase().projections();
        assertThat(
            mergeProjections,
            contains(
                instanceOf(LimitDistinctProjection.class),
                instanceOf(LimitAndOffsetProjection.class)
            )
        );
    }

    @Test
    public void test_group_by_on_subscript_on_obj_output_of_sub_relation() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of())
            .addTable(TableDefinitions.USER_TABLE_DEFINITION)
            .build();

        String stmt = "SELECT address['postcode'] FROM (SELECT address FROM users) AS u GROUP BY 1";
        LogicalPlan plan = e.logicalPlan(stmt);
        assertThat(plan, isPlan(
            "GroupHashAggregate[address['postcode']]\n" +
            "  └ Rename[address] AS u\n" +
            "    └ Collect[doc.users | [address] | true]"));
    }

    @Test
    public void test_order_by_on_subscript_on_obj_output_of_sub_relation() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of())
            .addTable(TableDefinitions.USER_TABLE_DEFINITION)
            .build();

        String stmt = "SELECT address['postcode'] FROM (SELECT address FROM users) AS u ORDER BY 1";
        LogicalPlan plan = e.logicalPlan(stmt);
        assertThat(plan, isPlan(
            "Eval[address['postcode']]\n" +
            "  └ OrderBy[address['postcode'] ASC]\n" +
            "    └ Rename[address] AS u\n" +
            "      └ Collect[doc.users | [address] | true]"));
        Merge merge = e.plan(stmt);
        Collect collect = (Collect) merge.subPlan();
        RoutedCollectPhase collectPhase = (RoutedCollectPhase) collect.collectPhase();
        assertThat(collectPhase.projections(), contains(
            instanceOf(OrderedLimitAndOffsetProjection.class),
            instanceOf(EvalProjection.class)
        ));
    }

    @Test
    public void test_join_with_no_match_where_clause_pushes_down_no_match() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of())
            .build();

        String stmt = "SELECT n.* " +
                      "FROM " +
                      "   pg_catalog.pg_namespace n," +
                      "   pg_catalog.pg_class c " +
                      "WHERE " +
                      "   n.nspname LIKE E'sys' " +
                      "   AND c.relnamespace = n.oid " +
                      "   AND (false)";
        LogicalPlan plan = e.logicalPlan(stmt);
        String expectedPlan =
            "NestedLoopJoin[CROSS]\n" +
            "  ├ Rename[nspacl, nspname, nspowner, oid] AS n\n" +
            "  │  └ Collect[pg_catalog.pg_namespace | [nspacl, nspname, nspowner, oid] | false]\n" +
            "  └ Rename[] AS c\n" +
            "    └ Collect[pg_catalog.pg_class | [] | false]";
        assertThat(plan, isPlan(expectedPlan));
    }

    @Test
    public void test_window_function_with_function_used_in_order_by_injects_eval_below_window_agg_ordering() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of())
            .build();

        // `WindowProjector.createUpdateProbeValueFunction` doesn't support function evaluation
        // because it is not using the InputFactory to evaluate the order by expressions
        // Injecting an Eval operator as a workaround
        String stmt =
            "SELECT\n" +
            "   unnest,\n" +
            "   sum(unnest) OVER(ORDER BY power(unnest, 2) RANGE BETWEEN 3 PRECEDING and CURRENT ROW)\n" +
            "FROM\n" +
            "   unnest(ARRAY[2.5, 4, 5, 6, 7.5, 8.5, 10, 12]) as t(unnest)";
        LogicalPlan plan = e.logicalPlan(stmt);
        String expectedPlan =
            "Eval[unnest, sum(unnest) OVER (ORDER BY power(unnest, 2.0) ASC RANGE BETWEEN 3 PRECEDING AND CURRENT ROW)]\n" +
            "  └ WindowAgg[unnest, power(unnest, 2.0), sum(unnest) OVER (ORDER BY power(unnest, 2.0) ASC RANGE BETWEEN 3 PRECEDING AND CURRENT ROW)]\n" +
            "    └ Eval[unnest, power(unnest, 2.0)]\n" +
            "      └ Rename[unnest] AS t\n" +
            "        └ TableFunction[unnest | [unnest] | true]";
        assertThat(plan, isPlan(expectedPlan));
    }

    @Test
    public void test_select_from_table_function_with_filter_on_not_selected_column() {
        SQLExecutor e = SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of())
            .build();

        String stmt =
            "SELECT word " +
            "FROM pg_catalog.pg_get_keywords() " +
            "WHERE catcode = 'R' " +
            "ORDER BY 1";
        LogicalPlan plan = e.logicalPlan(stmt);
        String expectedPlan =
            "Eval[word]\n" +
            "  └ OrderBy[word ASC]\n" +
            "    └ Filter[(catcode = 'R')]\n" +
            "      └ TableFunction[pg_get_keywords | [word, catcode] | true]";
        assertThat(plan, isPlan(expectedPlan));
    }

    @Test
    public void test_group_by_on_pk_lookup_uses_shard_projections() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of())
            .addTable(TableDefinitions.USER_TABLE_DEFINITION)
            .build();

        String stmt = "SELECT name, count(*) FROM users WHERE id in (1, 2, 3, 4, 5) GROUP BY name";
        LogicalPlan logicalPlan = e.logicalPlan(stmt);
        String expectedPlan =
            "GroupHashAggregate[name | count(*)]\n" +
            "  └ Get[doc.users | name | DocKeys{1::bigint; 2::bigint; 3::bigint; 4::bigint; 5::bigint} | (id = ANY([1::bigint, 2::bigint, 3::bigint, 4::bigint, 5::bigint]))]";
        assertThat(logicalPlan, isPlan(expectedPlan));
        Merge coordinatorMerge = e.plan(stmt);
        Merge distributedMerge = (Merge) coordinatorMerge.subPlan();
        Collect collect = (Collect) distributedMerge.subPlan();
        assertThat(
            collect.collectPhase().projections(),
            contains(instanceOf(GroupProjection.class))
        );
        assertThat(collect.collectPhase().projections().get(0).requiredGranularity(), is(RowGranularity.SHARD));
    }

    @Test
    public void test_order_by_on_aggregation_with_alias_in_select_list() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of())
            .addTable(TableDefinitions.USER_TABLE_DEFINITION)
            .build();

        String stmt = "SELECT count(id) as cnt FROM users GROUP BY name ORDER BY count(id) DESC";
        LogicalPlan plan = e.logicalPlan(stmt);
        String expectedPlan =
            "Eval[count(id) AS cnt]\n" +
            "  └ OrderBy[count(id) DESC]\n" +
            "    └ GroupHashAggregate[name | count(id)]\n" +
            "      └ Collect[doc.users | [id, name] | true]";
        assertThat(plan, isPlan(expectedPlan));
    }


    @Test
    public void test_equi_join_with_scalar_using_parameter_placeholders() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of())
            .addTable(TableDefinitions.USER_TABLE_DEFINITION)
            .build();

        String stmt = "SELECT u1.name FROM users u1 JOIN users u2 ON (u1.name || ?) = u2.name";
        LogicalPlan plan = e.logicalPlan(stmt);
        String expectedPlan =
            "Eval[name]\n" +
            "  └ HashJoin[(name = concat(name, $1))]\n" +
            "    ├ Rename[name] AS u1\n" +
            "    │  └ Collect[doc.users | [name] | true]\n" +
            "    └ Rename[name] AS u2\n" +
            "      └ Collect[doc.users | [name] | true]";
        assertThat(plan, isPlan(expectedPlan));

        // this must not fail
        e.plan(stmt, UUID.randomUUID(), 0, new RowN("foo"));
    }

    @Test
    public void test_non_euqi_join_with_scalar_using_parameter_placeholders() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of())
            .addTable(TableDefinitions.USER_TABLE_DEFINITION)
            .build();

        String stmt = "SELECT u1.name FROM users u1 JOIN users u2 ON (u1.name || ?) != u2.name";
        LogicalPlan plan = e.logicalPlan(stmt);
        String expectedPlan =
            "Eval[name]\n" +
            "  └ NestedLoopJoin[INNER | (NOT (name = concat(name, $1)))]\n" +
            "    ├ Rename[name] AS u1\n" +
            "    │  └ Collect[doc.users | [name] | true]\n" +
            "    └ Rename[name] AS u2\n" +
            "      └ Collect[doc.users | [name] | true]";
        assertThat(plan, isPlan(expectedPlan));

        // this must not fail
        e.plan(stmt, UUID.randomUUID(), 0, new RowN("foo"));
    }


    @Test
    public void test_columns_used_in_hash_join_condition_are_not_duplicated_in_outputs() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of())
            .addTable(T3.T1_DEFINITION)
            .addTable(T3.T2_DEFINITION)
            .build();

        String stmt =
            "SELECT * FROM " +
            "   (SELECT a FROM (SELECT * FROM t1) a1) v1 " +
            "   JOIN " +
            "   (SELECT b FROM (SELECT * FROM t2) a2) v2 " +
            "   ON (v1.a = v2.b) ";
        LogicalPlan plan = e.logicalPlan(stmt);
        String expectedPlan =
            "HashJoin[(a = b)]\n" +
            "  ├ Rename[a] AS v1\n" +
            "  │  └ Eval[a]\n" +
            "  │    └ Rename[a] AS a1\n" +
            "  │      └ Collect[doc.t1 | [a] | true]\n" +
            "  └ Rename[b] AS v2\n" +
            "    └ Eval[b]\n" +
            "      └ Rename[b] AS a2\n" +
            "        └ Collect[doc.t2 | [b] | true]";
        assertThat(plan, isPlan(expectedPlan));
    }

    @Test
    public void test_columns_used_in_nl_join_condition_are_not_duplicated_in_outputs() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of())
            .addTable(T3.T1_DEFINITION)
            .addTable(T3.T2_DEFINITION)
            .build();

        String stmt =
            "SELECT * FROM " +
            "   (SELECT a FROM (SELECT * FROM t1) a1) v1 " +
            "   JOIN " +
            "   (SELECT b FROM (SELECT * FROM t2) a2) v2 " +
            "   ON (v1.a > v2.b) ";
        LogicalPlan plan = e.logicalPlan(stmt);
        String expectedPlan =
            "NestedLoopJoin[INNER | (a > b)]\n" +
            "  ├ Rename[a] AS v1\n" +
            "  │  └ Eval[a]\n" +
            "  │    └ Rename[a] AS a1\n" +
            "  │      └ Collect[doc.t1 | [a] | true]\n" +
            "  └ Rename[b] AS v2\n" +
            "    └ Eval[b]\n" +
            "      └ Rename[b] AS a2\n" +
            "        └ Collect[doc.t2 | [b] | true]";
        assertThat(plan, isPlan(expectedPlan));
    }

    @Test
    public void test_collect_execution_plan_is_narrowed_to_matching_generated_partition_columns() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of())
            .addPartitionedTable(
                "create table doc.parted_by_generated (" +
                "   ts timestamp without time zone, " +
                "   p as date_trunc('month', ts) " +
                ") partitioned by (p)",
                new PartitionName(new RelationName("doc", "parted_by_generated"), singletonList("1577836800000")).asIndexName(),
                new PartitionName(new RelationName("doc", "parted_by_generated"), singletonList("1580515200000")).asIndexName())
            .build();

        String stmt = "SELECT * FROM parted_by_generated WHERE ts >= '2020-02-01'";
        LogicalPlan plan = e.logicalPlan(stmt);
        String expectedPlan =
            "Collect[doc.parted_by_generated | [ts, p AS date_trunc('month', ts)] | (ts >= 1580515200000::bigint)]";
        assertThat(plan, isPlan(expectedPlan));

        Collect collect = (Collect) ((Merge) e.plan(stmt)).subPlan();;
        RoutedCollectPhase routedCollectPhase = (RoutedCollectPhase) collect.collectPhase();
        Symbol where = routedCollectPhase.where();
        assertThat(where, TestingHelpers.isSQL("(doc.parted_by_generated.ts >= 1580515200000::bigint)"));
        assertThat(routedCollectPhase.routing().locations().values().stream()
            .flatMap(x -> x.keySet().stream())
            .collect(Collectors.toSet()),
            contains(
                ".partitioned.parted_by_generated.04732d9o60qj2d9i60o30c1g"
            )
        );
    }

    @Test
    public void test_select_where_id_and_seq_missing_primary_term() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of())
            .addTable(TableDefinitions.USER_TABLE_DEFINITION)
            .build();

        assertThrowsMatches(
            () -> e.plan("select id from users where id = 1 and _seq_no = 11"),
            VersioningValidationException.class,
            VersioningValidationException.SEQ_NO_AND_PRIMARY_TERM_USAGE_MSG
        );
    }

    @Test
    public void test_select_where_seq_and_primary_term_missing_id() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of())
            .addTable(TableDefinitions.USER_TABLE_DEFINITION)
            .build();

        assertThrowsMatches(
            () -> e.plan("select id from users where _seq_no = 11 and _primary_term = 1"),
            VersioningValidationException.class,
            VersioningValidationException.SEQ_NO_AND_PRIMARY_TERM_USAGE_MSG
        );
    }


    @Test
    public void test_filter_and_eval_on_get_operator_use_shard_projections() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of())
            .addTable(TableDefinitions.USER_TABLE_DEFINITION)
            .build();

        Merge merge = e.plan("""
            SELECT count(*) FROM (
                SELECT
                    name
                FROM
                    users
                WHERE
                    id = 10 AND (name = 'bar' or name IS NULL)
                ) u
            """);
        Collect collect = (Collect) merge.subPlan();
        var pkLookup = (PKLookupPhase) collect.collectPhase();
        assertThat(pkLookup.projections(), Matchers.contains(
            Matchers.instanceOf(FilterProjection.class),
            Matchers.instanceOf(EvalProjection.class),
            Matchers.instanceOf(AggregationProjection.class)
        ));
        for (var projection : pkLookup.projections()) {
            assertThat(projection.requiredGranularity(), is(RowGranularity.SHARD));
        }
    }

    @Test
    public void test_queries_in_count_operator_are_optimized() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .addTable("create table tbl (xs array(varchar(1)))")
            .build();

        CountPlan plan = e.plan("select count(*) from tbl where 'a' = ANY(xs)");
        assertThat(plan.countPhase().where(), isSQL("(_cast('a', 'text(1)') = ANY(doc.tbl.xs))"));
    }

    @Test
    public void test_collect_phase_narrows_shard_selection_based_on_clustered_by_columns() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .addTable("create table tbl (x int, y int) clustered by (x) into 2 shards")
            .build();

        Collect collect = e.plan("select * from tbl where x = 1");
        RoutedCollectPhase routedCollectPhase = (RoutedCollectPhase )collect.collectPhase();

        int numShards = routedCollectPhase.routing().locations().values().stream()
            .flatMap(x -> x.values().stream())
            .mapToInt(x -> x.size())
            .sum();
        assertThat(numShards, is(1));
    }

    @Test
    public void test_filter_on_aliased_subselect_output() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .addTable(T3.T1_DEFINITION)
            .build();

        var stmt = "select * from (select i, true as b from t1) it where b";
        Collect collect = e.plan(stmt);
        RoutedCollectPhase routedCollectPhase = (RoutedCollectPhase) collect.collectPhase();

        Asserts.assertThat(routedCollectPhase.where()).isLiteral(true);
    }

    @Test
    public void test_table_function_without_from_can_bind_parameters() {
        SQLExecutor e = SQLExecutor.builder(clusterService).build();
        String stmt = "SELECT UNNEST(?)";
        Collect collect = e.plan(stmt, UUID.randomUUID(), 0, new RowN(new Object[] {null}));

        assertThat(collect.collectPhase().projections().get(0).projectionType(), is(ProjectionType.PROJECT_SET));
        ProjectSetProjection projectSetProjection = (ProjectSetProjection) collect.collectPhase().projections().get(0);
        Asserts.assertThat(((Function) projectSetProjection.tableFunctions().get(0)).arguments().get(0))
            .isLiteral(null); // used to be unbound ParameterSymbol
    }

    @Test
    public void test_non_recursive_with_is_rewritten_to_nested_subselects() throws Exception {
        var e = SQLExecutor.builder(clusterService)
            .addTable("create table t1 (x int)")
            .build();
        var withPlan = e.logicalPlan(
            "WITH" +
                " r AS (SELECT x FROM t1)," +
                " s AS (SELECT x FROM r) " +
                "SELECT * FROM s");

        var subSelectPlan = e.logicalPlan(
                "SELECT * FROM (SELECT x FROM (SELECT x FROM t1) AS r) AS s");

        assertThat(withPlan, isPlan(subSelectPlan));
    }

    @Test
    public void test_non_recursive_nested_with() throws Exception {
        var e = SQLExecutor.builder(clusterService)
            .addTable("create table t1 (x int)")
            .addTable("create table t2 (x int)")
            .build();
        var withPlan = e.logicalPlan(
            "WITH" +
                " u AS (SELECT * FROM t1)," +
                " v AS (WITH u AS (SELECT * FROM t2) SELECT * FROM u) " +
                "SELECT * FROM u, v");

        var subSelectPlan = e.logicalPlan(
            "SELECT * FROM" +
                " (SELECT * FROM t1) AS u," +
                " (SELECT x FROM (SELECT x FROM t2) AS u) AS v");

        assertThat(withPlan, isPlan(subSelectPlan));
    }
}
