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

package io.crate.planner;

import com.google.common.collect.Iterables;
import io.crate.action.sql.SessionContext;
import io.crate.analyze.EvaluatingNormalizer;
import io.crate.analyze.QuerySpec;
import io.crate.analyze.TableDefinitions;
import io.crate.analyze.WhereClause;
import io.crate.analyze.symbol.AggregateMode;
import io.crate.analyze.symbol.Aggregation;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.InputColumn;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.SymbolType;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.exceptions.VersionInvalidException;
import io.crate.executor.transport.NodeOperationTreeGenerator;
import io.crate.metadata.PartitionName;
import io.crate.metadata.Reference;
import io.crate.metadata.Routing;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.Schemas;
import io.crate.metadata.TableIdent;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.TestingTableInfo;
import io.crate.operation.NodeOperation;
import io.crate.operation.NodeOperationTree;
import io.crate.operation.aggregation.impl.CountAggregation;
import io.crate.operation.projectors.TopN;
import io.crate.planner.node.ExecutionPhase;
import io.crate.planner.node.dql.Collect;
import io.crate.planner.node.dql.CountPlan;
import io.crate.planner.node.dql.ESGet;
import io.crate.planner.node.dql.MergePhase;
import io.crate.planner.node.dql.QueryThenFetch;
import io.crate.planner.node.dql.RoutedCollectPhase;
import io.crate.planner.node.dql.join.JoinType;
import io.crate.planner.node.dql.join.NestedLoop;
import io.crate.planner.projection.AggregationProjection;
import io.crate.planner.projection.EvalProjection;
import io.crate.planner.projection.FetchProjection;
import io.crate.planner.projection.FilterProjection;
import io.crate.planner.projection.GroupProjection;
import io.crate.planner.projection.MergeCountProjection;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.TopNProjection;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.testing.T3;
import io.crate.testing.TestingHelpers;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static io.crate.testing.SymbolMatchers.isFetchRef;
import static io.crate.testing.SymbolMatchers.isFunction;
import static io.crate.testing.SymbolMatchers.isReference;
import static io.crate.testing.TestingHelpers.isDocKey;
import static io.crate.testing.TestingHelpers.isSQL;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

public class SelectPlannerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;

    @Before
    public void prepare() {
        e = SQLExecutor.builder(clusterService)
            .addDocTable(TableDefinitions.USER_TABLE_INFO)
            .addDocTable(TableDefinitions.TEST_CLUSTER_BY_STRING_TABLE_INFO)
            .addDocTable(TableDefinitions.PARTED_PKS_TI)
            .addDocTable(TableDefinitions.TEST_PARTITIONED_TABLE_INFO)
            .addDocTable(TableDefinitions.IGNORED_NESTED_TABLE_INFO)
            .addDocTable(TableDefinitions.TEST_MULTIPLE_PARTITIONED_TABLE_INFO)
            .addDocTable(T3.T1_INFO)
            .addDocTable(bindGeneratedColumnTable())
            .build();
    }

    private DocTableInfo bindGeneratedColumnTable() {
        TableIdent generatedColumnTableIdent = new TableIdent(Schemas.DOC_SCHEMA_NAME, "gc_table");
        return new TestingTableInfo.Builder(
            generatedColumnTableIdent, new Routing(Collections.emptyMap()))
            .add("revenue", DataTypes.INTEGER, null)
            .add("cost", DataTypes.INTEGER, null)
            .addGeneratedColumn("profit", DataTypes.INTEGER, "subtract(revenue, cost)", false)
            .build(TestingHelpers.getFunctions());
    }

    @Test
    public void testHandlerSideRouting() throws Exception {
        // just testing the dispatching here.. making sure it is not a ESSearchNode
        e.plan("select * from sys.cluster");
    }

    @Test
    public void testWherePKAndMatchDoesNotResultInESGet() throws Exception {
        Plan plan = e.plan("select * from users where id in (1, 2, 3) and match(text, 'Hello')");
        assertThat(plan, instanceOf(QueryThenFetch.class));
    }

    @Test
    public void testGetPlan() throws Exception {
        ESGet esGet = e.plan("select name from users where id = 1");
        assertThat(esGet.tableInfo().ident().name(), is("users"));
        assertThat(esGet.docKeys().getOnlyKey(), isDocKey(1L));
        assertThat(esGet.outputs().size(), is(1));
    }

    @Test
    public void testGetWithVersion() throws Exception {
        expectedException.expect(VersionInvalidException.class);
        expectedException.expectMessage("\"_version\" column is not valid in the WHERE clause of a SELECT statement");
        e.plan("select name from users where id = 1 and _version = 1");
    }

    @Test
    public void testGetPlanStringLiteral() throws Exception {
        ESGet esGet = e.plan("select name from bystring where name = 'one'");
        assertThat(esGet.tableInfo().ident().name(), is("bystring"));
        assertThat(esGet.docKeys().getOnlyKey(), isDocKey("one"));
        assertThat(esGet.outputs().size(), is(1));
    }

    @Test
    public void testGetPlanPartitioned() throws Exception {
        ESGet esGet = e.plan("select name, date from parted_pks where id = 1 and date = 0");
        assertThat(esGet.tableInfo().ident().name(), is("parted_pks"));
        assertThat(esGet.docKeys().getOnlyKey(), isDocKey(1, 0L));

        //is(new PartitionName("parted", Arrays.asList(new BytesRef("0"))).asIndexName()));
        assertEquals(DataTypes.STRING, esGet.outputTypes().get(0));
        assertEquals(DataTypes.TIMESTAMP, esGet.outputTypes().get(1));
    }

    @Test
    public void testMultiGetPlan() throws Exception {
        ESGet esGet = e.plan("select name from users where id in (1, 2)");
        assertThat(esGet.docKeys().size(), is(2));
        assertThat(esGet.docKeys(), containsInAnyOrder(isDocKey(1L), isDocKey(2L)));
    }


    @Test
    public void testGlobalAggregationPlan() throws Exception {
        Merge globalAggregate = e.plan("select count(name) from users");
        Collect collect = (Collect) globalAggregate.subPlan();
        RoutedCollectPhase collectPhase = ((RoutedCollectPhase) collect.collectPhase());

        assertEquals(CountAggregation.LongStateType.INSTANCE, collectPhase.outputTypes().get(0));
        assertThat(collectPhase.maxRowGranularity(), is(RowGranularity.DOC));
        assertThat(collectPhase.projections().size(), is(1));
        assertThat(collectPhase.projections().get(0), instanceOf(AggregationProjection.class));
        assertThat(collectPhase.projections().get(0).requiredGranularity(), is(RowGranularity.SHARD));

        MergePhase mergePhase = globalAggregate.mergePhase();

        assertEquals(CountAggregation.LongStateType.INSTANCE, Iterables.get(mergePhase.inputTypes(), 0));
        assertEquals(DataTypes.LONG, mergePhase.outputTypes().get(0));
    }

    @Test
    public void testShardSelectWithOrderBy() throws Exception {
        Collect collect = e.plan("select id from sys.shards order by id limit 10");
        RoutedCollectPhase collectPhase = ((RoutedCollectPhase) collect.collectPhase());

        assertEquals(DataTypes.INTEGER, collectPhase.outputTypes().get(0));
        assertThat(collectPhase.maxRowGranularity(), is(RowGranularity.SHARD));

        assertThat(collectPhase.orderBy(), notNullValue());

        List<Projection> projections = collectPhase.projections();
        assertThat(projections, contains(
            instanceOf(TopNProjection.class),
            instanceOf(TopNProjection.class)
        ));
    }

    @Test
    public void testCollectAndMergePlan() throws Exception {
        QueryThenFetch qtf = e.plan("select name from users where name = 'x' order by id limit 10");
        Merge merge = (Merge) qtf.subPlan();
        RoutedCollectPhase collectPhase = ((RoutedCollectPhase) ((Collect) merge.subPlan()).collectPhase());
        assertTrue(collectPhase.whereClause().hasQuery());

        TopNProjection topNProjection = (TopNProjection) collectPhase.projections().get(0);
        assertThat(topNProjection.limit(), is(10));

        MergePhase mergePhase = merge.mergePhase();
        assertThat(mergePhase.outputTypes().size(), is(1));
        assertEquals(DataTypes.STRING, mergePhase.outputTypes().get(0));

        assertTrue(mergePhase.finalProjection().isPresent());

        Projection lastProjection = mergePhase.finalProjection().get();
        assertThat(lastProjection, instanceOf(FetchProjection.class));
        FetchProjection fetchProjection = (FetchProjection) lastProjection;
        assertThat(fetchProjection.outputs(), isSQL("FETCH(INPUT(0), doc.users._doc['name'])"));
    }

    @Test
    public void testCollectAndMergePlanNoFetch() throws Exception {
        // testing that a fetch projection is not added if all output symbols are included
        // at the orderBy symbols
        Merge merge = e.plan("select name from users where name = 'x' order by name limit 10");
        Collect collect = (Collect) merge.subPlan();
        RoutedCollectPhase collectPhase = ((RoutedCollectPhase) collect.collectPhase());
        assertTrue(collectPhase.whereClause().hasQuery());

        MergePhase mergePhase = merge.mergePhase();
        assertThat(mergePhase.outputTypes().size(), is(1));
        assertEquals(DataTypes.STRING, mergePhase.outputTypes().get(0));

        assertTrue(mergePhase.finalProjection().isPresent());

        Projection lastProjection = mergePhase.finalProjection().get();
        assertThat(lastProjection, instanceOf(TopNProjection.class));
        TopNProjection topNProjection = (TopNProjection) lastProjection;
        assertThat(topNProjection.outputs().size(), is(1));
    }

    @Test
    public void testCollectAndMergePlanHighLimit() throws Exception {
        QueryThenFetch qtf = e.plan("select name from users limit 100000");
        Merge merge = (Merge) qtf.subPlan();
        RoutedCollectPhase collectPhase = ((RoutedCollectPhase) ((Collect) merge.subPlan()).collectPhase());
        assertThat(collectPhase.nodePageSizeHint(), is(100_000));

        MergePhase mergePhase = merge.mergePhase();
        assertThat(mergePhase.projections().size(), is(2));
        assertThat(mergePhase.finalProjection().get(), instanceOf(FetchProjection.class));
        TopNProjection topN = (TopNProjection) mergePhase.projections().get(0);
        assertThat(topN.limit(), is(100_000));
        assertThat(topN.offset(), is(0));

        FetchProjection fetchProjection = (FetchProjection) mergePhase.projections().get(1);

        // with offset
        qtf = e.plan("select name from users limit 100000 offset 20");
        merge = ((Merge) qtf.subPlan());

        collectPhase = ((RoutedCollectPhase) ((Collect) merge.subPlan()).collectPhase());
        assertThat(collectPhase.nodePageSizeHint(), is(100_000 + 20));

        mergePhase = merge.mergePhase();
        assertThat(mergePhase.projections().size(), is(2));
        assertThat(mergePhase.finalProjection().get(), instanceOf(FetchProjection.class));
        topN = (TopNProjection) mergePhase.projections().get(0);
        assertThat(topN.limit(), is(100_000));
        assertThat(topN.offset(), is(20));

        fetchProjection = (FetchProjection) mergePhase.projections().get(1);
    }


    @Test
    public void testCollectAndMergePlanPartitioned() throws Exception {
        QueryThenFetch qtf = e.plan("select id, name, date from parted_pks where date > 0 and name = 'x' order by id limit 10");
        Merge merge = (Merge) qtf.subPlan();
        RoutedCollectPhase collectPhase = ((RoutedCollectPhase) ((Collect) merge.subPlan()).collectPhase());

        List<String> indices = new ArrayList<>();
        Map<String, Map<String, List<Integer>>> locations = collectPhase.routing().locations();
        for (Map.Entry<String, Map<String, List<Integer>>> entry : locations.entrySet()) {
            indices.addAll(entry.getValue().keySet());
        }
        assertThat(indices, Matchers.contains(
            new PartitionName("parted", Arrays.asList(new BytesRef("123"))).asIndexName()));

        assertTrue(collectPhase.whereClause().hasQuery());

        MergePhase mergePhase = merge.mergePhase();
        assertThat(mergePhase.outputTypes().size(), is(3));
    }

    @Test
    public void testCollectAndMergePlanFunction() throws Exception {
        QueryThenFetch qtf = e.plan("select format('Hi, my name is %s', name), name from users where name = 'x' order by id limit 10");
        Merge merge = (Merge) qtf.subPlan();
        RoutedCollectPhase collectPhase = ((RoutedCollectPhase) ((Collect) merge.subPlan()).collectPhase());

        assertTrue(collectPhase.whereClause().hasQuery());

        MergePhase mergePhase = merge.mergePhase();
        assertThat(mergePhase.outputTypes().size(), is(2));
        assertEquals(DataTypes.STRING, mergePhase.outputTypes().get(0));
        assertEquals(DataTypes.STRING, mergePhase.outputTypes().get(1));

        assertTrue(mergePhase.finalProjection().isPresent());

        Projection lastProjection = mergePhase.finalProjection().get();
        assertThat(lastProjection, instanceOf(FetchProjection.class));
        FetchProjection fetchProjection = (FetchProjection) lastProjection;
        assertThat(fetchProjection.outputs().size(), is(2));
        assertThat(fetchProjection.outputs().get(0), isFunction("format"));
        assertThat(fetchProjection.outputs().get(1), isFetchRef(0, "_doc['name']"));
    }

    @Test
    public void testCountDistinctPlan() throws Exception {
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
        assertThat(((Reference) collectPhase.toCollect().get(0)).ident().columnIdent().name(), is("name"));

        MergePhase mergePhase = globalAggregate.mergePhase();
        assertThat(mergePhase.projections().size(), is(2));
        Projection projection1 = mergePhase.projections().get(1);

        assertThat(projection1, instanceOf(EvalProjection.class));
        Symbol collection_count = projection1.outputs().get(0);
        assertThat(collection_count, instanceOf(Function.class));
    }

    @Test
    public void testGlobalAggregationHaving() throws Exception {
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
        CountPlan plan = e.plan("select count(*) from parted where date = 1395874800000");
        assertThat(plan.countPhase().whereClause().partitions(), containsInAnyOrder(".partitioned.parted.04732cpp6ks3ed1o60o30c1g"));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSelectPartitionedTableOrderByPartitionedColumn() throws Exception {
        e.plan("select name from parted order by date");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSelectPartitionedTableOrderByPartitionedColumnInFunction() throws Exception {
        e.plan("select name from parted order by year(date)");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSelectOrderByPartitionedNestedColumn() throws Exception {
        e.plan("select id from multi_parted order by obj['name']");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSelectOrderByPartitionedNestedColumnInFunction() throws Exception {
        e.plan("select id from multi_parted order by format('abc %s', obj['name'])");
    }

    @Test(expected = UnsupportedFeatureException.class)
    public void testQueryRequiresScalar() throws Exception {
        // only scalar functions are allowed on system tables because we have no lucene queries
        e.plan("select * from sys.shards where match(table_name, 'characters')");
    }

    @Test
    public void testOrderByOnAnalyzed() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Cannot ORDER BY 'text': sorting on analyzed/fulltext columns is not possible");
        e.plan("select text from users u order by 1");
    }

    @Test
    public void testSortOnUnknownColumn() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Cannot ORDER BY 'details['unknown_column']': invalid data type 'null'.");
        e.plan("select details from ignored_nested order by details['unknown_column']");
    }

    @Test
    public void testOrderByOnIndexOff() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Cannot ORDER BY 'no_index': sorting on non-indexed columns is not possible");
        e.plan("select no_index from users u order by 1");
    }

    @Test
    public void testSelectAnalyzedReferenceInFunctionAggregation() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Cannot select analyzed column 'text' within grouping or aggregations");
        e.plan("select min(substr(text, 0, 2)) from users");
    }

    @Test
    public void testSelectNonIndexedReferenceInFunctionAggregation() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Cannot select non-indexed column 'no_index' within grouping or aggregations");
        e.plan("select min(substr(no_index, 0, 2)) from users");
    }

    @Test
    public void testGlobalAggregateWithWhereOnPartitionColumn() throws Exception {
        Merge globalAggregate = e.plan(
            "select min(name) from parted where date > 1395961100000");
        Collect collect = (Collect) globalAggregate.subPlan();

        WhereClause whereClause = ((RoutedCollectPhase) collect.collectPhase()).whereClause();
        assertThat(whereClause.partitions().size(), is(1));
        assertThat(whereClause.noMatch(), is(false));
    }

    @Test
    public void testHasNoResultFromHaving() throws Exception {
        Merge merge = e.plan("select min(name) from users having 1 = 2");
        assertThat(merge.mergePhase().projections().get(1), instanceOf(FilterProjection.class));
        assertThat(((FilterProjection) merge.mergePhase().projections().get(1)).query(), isSQL("false"));
    }

    @Test
    public void testHasNoResultFromLimit() {
        CountPlan count = e.plan("select count(*) from users limit 1 offset 1");
        assertThat(count.mergePhase().projections().get(1), instanceOf(TopNProjection.class));
        assertThat(((TopNProjection) count.mergePhase().projections().get(1)).limit(), is(1));
        assertThat(((TopNProjection) count.mergePhase().projections().get(1)).offset(), is(1));

        count = e.plan("select count(*) from users limit 0");
        assertThat(count.mergePhase().projections().get(1), instanceOf(TopNProjection.class));
        assertThat(((TopNProjection) count.mergePhase().projections().get(1)).limit(), is(0));

        assertThat(e.plan("select * from users order by name limit 0"), instanceOf(NoopPlan.class));
        assertThat(e.plan("select * from users order by name limit 0 offset 0"), instanceOf(NoopPlan.class));
    }

    @Test
    public void testHasNoResultFromQuery() {
        assertThat(e.plan("select name from users where false"), instanceOf(NoopPlan.class));
    }

    @Test
    public void testShardQueueSizeCalculation() throws Exception {
        Merge merge = e.plan("select name from users order by name limit 500");
        Collect collect = (Collect) merge.subPlan();
        int shardQueueSize = ((RoutedCollectPhase) collect.collectPhase()).shardQueueSize(
            collect.collectPhase().nodeIds().iterator().next());
        assertThat(shardQueueSize, is(375));
    }

    @Test
    public void testQAFPagingIsEnabledOnHighLimit() throws Exception {
        Merge plan = e.plan("select name from users order by name limit 1000000");
        assertThat(plan.mergePhase().nodeIds().size(), is(1)); // mergePhase with executionNode = paging enabled

        Collect collect = (Collect) plan.subPlan();
        assertThat(((RoutedCollectPhase) collect.collectPhase()).nodePageSizeHint(), is(750000));
    }

    @Test
    public void testQAFPagingIsEnabledOnHighOffset() throws Exception {
        Merge merge = e.plan("select name from users order by name limit 10 offset 1000000");
        Collect collect = (Collect) merge.subPlan();
        assertThat(merge.mergePhase().nodeIds().size(), is(1)); // mergePhase with executionNode = paging enabled
        assertThat(((RoutedCollectPhase) collect.collectPhase()).nodePageSizeHint(), is(750007));
    }

    @Test
    public void testQTFPagingIsEnabledOnHighLimit() throws Exception {
        QueryThenFetch qtf = e.plan("select name, date from users order by name limit 1000000");
        Merge merge = (Merge) qtf.subPlan();
        RoutedCollectPhase collectPhase = ((RoutedCollectPhase) ((Collect) merge.subPlan()).collectPhase());
        assertThat(merge.mergePhase().nodeIds().size(), is(1)); // mergePhase with executionNode = paging enabled
        assertThat(collectPhase.nodePageSizeHint(), is(750000));
    }

    @Test
    public void testSelectFromUnnestResultsInTableFunctionPlan() throws Exception {
        Collect collect = e.plan("select * from unnest([1, 2], ['Arthur', 'Trillian'])");
        assertNotNull(collect);
        assertThat(collect.collectPhase().toCollect(), contains(isReference("col1"), isReference("col2")));
    }

    @Test
    public void testSoftLimitIsApplied() throws Exception {
        QueryThenFetch qtf = e.plan("select * from users", UUID.randomUUID(), 10, 0);
        Merge merge = (Merge) qtf.subPlan();
        assertThat(merge.mergePhase().projections(),
            contains(instanceOf(TopNProjection.class), instanceOf(FetchProjection.class)));
        TopNProjection topNProjection = (TopNProjection) merge.mergePhase().projections().get(0);
        assertThat(topNProjection.limit(), is(10));

        qtf = e.plan("select * from users limit 5", UUID.randomUUID(), 10, 0);
        merge = (Merge) qtf.subPlan();
        assertThat(merge.mergePhase().projections(), contains(instanceOf(TopNProjection.class), instanceOf(FetchProjection.class)));
        topNProjection = (TopNProjection) merge.mergePhase().projections().get(0);
        assertThat(topNProjection.limit(), is(5));
    }

    @Test
    public void testReferenceToNestedAggregatedField() throws Exception {
        Merge merge = e.plan("select ii, xx from ( " +
                             "  select i + i as ii, xx from (" +
                             "    select i, sum(x) as xx from t1 group by i) as t) as tt " +
                             "where (ii * 2) > 4 and (xx * 2) > 120");
        Plan subQuery = merge.subPlan();
        assertThat(subQuery, instanceOf(Collect.class));
        assertThat(((Collect) subQuery).collectPhase().projections(), contains(
            instanceOf(GroupProjection.class)
        ));
        assertThat(merge.mergePhase().projections(), contains(
            instanceOf(GroupProjection.class),
            instanceOf(EvalProjection.class),
            instanceOf(FilterProjection.class),
            instanceOf(EvalProjection.class)
        ));
    }

    @Test
    public void test3TableJoinQuerySplitting() throws Exception {
        QueryThenFetch qtf = e.plan("select" +
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
        NestedLoop outerNl = (NestedLoop) qtf.subPlan();
        NestedLoop innerNl = (NestedLoop) outerNl.left();

        assertThat(((FilterProjection) innerNl.nestedLoopPhase().projections().get(0)).query(),
            isSQL("((INPUT(0) = INPUT(2)) AND (INPUT(1) = INPUT(3)))"));
    }

    @Test
    public void testOuterJoinToInnerJoinRewrite() throws Exception {
        QueryThenFetch qtf = e.plan("select u1.text, u2.text " +
                                  "from users u1 left join users u2 on u1.id = u2.id " +
                                  "where u2.name = 'Arthur'" +
                                  "and u2.id > 1 ");
        NestedLoop nl = (NestedLoop) qtf.subPlan();
        assertThat(nl.nestedLoopPhase().joinType(), is(JoinType.INNER));
        Collect rightCM = (Collect) nl.right();
        assertThat(((RoutedCollectPhase) rightCM.collectPhase()).whereClause().query(),
            isSQL("((doc.users.name = 'Arthur') AND (doc.users.id > 1))"));

        // doesn't contain "name" because whereClause is pushed down,
        // but still contains "id" because it is in the joinCondition
        assertThat(rightCM.collectPhase().toCollect(), contains(isReference("_fetchid"), isReference("id")));
    }

    @Test
    public void testNoSoftLimitOnUnlimitedChildRelation() throws Exception {
        int softLimit = 10_000;
        EvaluatingNormalizer normalizer = EvaluatingNormalizer.functionOnlyNormalizer(e.functions());
        Planner.Context plannerContext = new Planner.Context(
            e.planner,
            clusterService,
            UUID.randomUUID(),
            null,
            normalizer,
            new TransactionContext(SessionContext.create()),
            softLimit,
            0);
        Limits limits = plannerContext.getLimits(new QuerySpec());
        assertThat(limits.finalLimit(), is(TopN.NO_LIMIT));
    }

    @Test
    public void testShardSelect() throws Exception {
        Collect collect = e.plan("select id from sys.shards");
        RoutedCollectPhase collectPhase = ((RoutedCollectPhase) collect.collectPhase());
        assertThat(collectPhase.maxRowGranularity(), is(RowGranularity.SHARD));
    }

    @Test
    public void testGlobalCountPlan() throws Exception {
        CountPlan plan = e.plan("select count(*) from users");

        assertThat(plan.countPhase().whereClause(), equalTo(WhereClause.MATCH_ALL));

        assertThat(plan.mergePhase().projections().size(), is(1));
        assertThat(plan.mergePhase().projections().get(0), instanceOf(MergeCountProjection.class));
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    public void testLimitThatIsBiggerThanPageSizeCausesQTFPUshPlan() throws Exception {
        QueryThenFetch qtf = e.plan("select * from users limit 2147483647 ");
        Merge merge = (Merge) qtf.subPlan();
        assertThat(merge.mergePhase().nodeIds().size(), is(1));
        String localNodeId = merge.mergePhase().nodeIds().iterator().next();
        NodeOperationTree operationTree = NodeOperationTreeGenerator.fromPlan(qtf, localNodeId);
        NodeOperation nodeOperation = operationTree.nodeOperations().iterator().next();
        // paging -> must not use direct response
        assertThat(nodeOperation.downstreamNodes(), not(contains(ExecutionPhase.DIRECT_RESPONSE)));


        qtf = e.plan("select * from users limit 2");
        localNodeId = qtf.subPlan().resultDescription().nodeIds().iterator().next();
        operationTree = NodeOperationTreeGenerator.fromPlan(qtf, localNodeId);
        nodeOperation = operationTree.nodeOperations().iterator().next();
        // no paging -> can use direct response
        assertThat(nodeOperation.downstreamNodes(), contains(ExecutionPhase.DIRECT_RESPONSE));
    }

    @Test
    public void testAggregationOnGeneratedColumns() throws Exception {
        Merge plan = e.plan("select sum(profit) from gc_table");

        List<Projection> projections = plan.mergePhase().projections();
        assertThat(projections.size(), is(2));
        assertThat(projections.get(0), instanceOf(AggregationProjection.class));
        assertThat(((AggregationProjection)projections.get(0)).aggregations().get(0).inputs().get(0),
                   isSQL("INPUT(0)"));

        projections = ((Collect)plan.subPlan()).collectPhase().projections();
        assertThat(projections.size(), is(1));
        assertThat(projections.get(0), instanceOf(AggregationProjection.class));
        assertThat(((AggregationProjection)projections.get(0)).aggregations().get(0).inputs().get(0),
                   isSQL("INPUT(0)"));
    }

    @Test
    public void testGlobalAggregationOn3TableJoinWithImplicitJoinConditions() {
        Merge plan = e.plan("select count(*) from users t1, users t2, users t3 " +
                            "where t1.id = t2.id and t2.id = t3.id");
        assertThat(plan.subPlan(), instanceOf(NestedLoop.class));
        NestedLoop outerNL = (NestedLoop)plan.subPlan();
        assertThat(outerNL.nestedLoopPhase().projections().get(0), instanceOf(FilterProjection.class));
        FilterProjection filterProjection = (FilterProjection) outerNL.nestedLoopPhase().projections().get(0);
        assertThat(filterProjection.outputs(), isSQL("INPUT(0), INPUT(1)"));
        assertThat(filterProjection.query(), isSQL("(INPUT(0) = INPUT(1))"));
        assertThat(outerNL.nestedLoopPhase().outputTypes().size(), is(1));
        assertThat(outerNL.nestedLoopPhase().outputTypes().get(0), is(CountAggregation.LongStateType.INSTANCE));

        NestedLoop innerNL = (NestedLoop) outerNL.left();
        assertThat(innerNL.nestedLoopPhase().projections().get(0), instanceOf(FilterProjection.class));
        filterProjection = (FilterProjection) innerNL.nestedLoopPhase().projections().get(0);
        assertThat(filterProjection.outputs(), isSQL("INPUT(0), INPUT(1)"));
        assertThat(filterProjection.query(), isSQL("(INPUT(0) = INPUT(1))"));
        assertThat(innerNL.nestedLoopPhase().outputTypes().size(), is(1));
        assertThat(innerNL.nestedLoopPhase().outputTypes().get(0), is(DataTypes.LONG));

        plan = e.plan("select count(t1.other_id) from users t1, users t2, users t3 " +
                            "where t1.id = t2.id and t2.id = t3.id");
        assertThat(plan.subPlan(), instanceOf(NestedLoop.class));
        outerNL = (NestedLoop)plan.subPlan();
        assertThat(outerNL.nestedLoopPhase().projections().get(0), instanceOf(FilterProjection.class));
        filterProjection = (FilterProjection) outerNL.nestedLoopPhase().projections().get(0);
        assertThat(filterProjection.outputs(), isSQL("INPUT(0), INPUT(1), INPUT(2)"));
        assertThat(filterProjection.query(), isSQL("(INPUT(1) = INPUT(2))"));
        assertThat(outerNL.nestedLoopPhase().outputTypes().size(), is(1));
        assertThat(outerNL.nestedLoopPhase().outputTypes().get(0), is(CountAggregation.LongStateType.INSTANCE));

        innerNL = (NestedLoop) outerNL.left();
        assertThat(innerNL.nestedLoopPhase().projections().get(0), instanceOf(FilterProjection.class));
        filterProjection = (FilterProjection) innerNL.nestedLoopPhase().projections().get(0);
        assertThat(filterProjection.outputs(), isSQL("INPUT(0), INPUT(1), INPUT(2)"));
        assertThat(filterProjection.query(), isSQL("(INPUT(0) = INPUT(2))"));
        assertThat(innerNL.nestedLoopPhase().outputTypes().size(), is(2));
        assertThat(innerNL.nestedLoopPhase().outputTypes().get(0), is(DataTypes.LONG));
        assertThat(innerNL.nestedLoopPhase().outputTypes().get(1), is(DataTypes.LONG));
    }

    @Test
    public void test2TableJoinWithNoMatch() throws Exception {
        QueryThenFetch qtf = e.plan("select * from users t1, users t2 WHERE 1=2");
        NestedLoop nl = (NestedLoop) qtf.subPlan();
        assertThat(nl.left(), instanceOf(Collect.class));
        assertThat(nl.right(), instanceOf(Collect.class));
        assertThat(((RoutedCollectPhase)((Collect)nl.left()).collectPhase()).whereClause().noMatch(), is(true));
        assertThat(((RoutedCollectPhase)((Collect)nl.right()).collectPhase()).whereClause().noMatch(), is(true));
    }

    @Test
    public void test3TableJoinWithNoMatch() throws Exception {
        QueryThenFetch qtf = e.plan("select * from users t1, users t2, users t3 WHERE 1=2");
        NestedLoop outer = (NestedLoop) qtf.subPlan();
        assertThat(((RoutedCollectPhase)((Collect)outer.right()).collectPhase()).whereClause().noMatch(), is(true));
        NestedLoop inner = (NestedLoop) outer.left();
        assertThat(((RoutedCollectPhase)((Collect)inner.left()).collectPhase()).whereClause().noMatch(), is(true));
        assertThat(((RoutedCollectPhase)((Collect)inner.right()).collectPhase()).whereClause().noMatch(), is(true));
    }

    @Test
    public void testGlobalAggregateOn2TableJoinWithNoMatch() throws Exception {
        NestedLoop nl = e.plan("select count(*) from users t1, users t2 WHERE 1=2");
        assertThat(nl.left(), instanceOf(Collect.class));
        assertThat(nl.right(), instanceOf(Collect.class));
        assertThat(((RoutedCollectPhase)((Collect)nl.left()).collectPhase()).whereClause().noMatch(), is(true));
        assertThat(((RoutedCollectPhase)((Collect)nl.right()).collectPhase()).whereClause().noMatch(), is(true));
    }

    @Test
    public void testGlobalAggregateOn3TableJoinWithNoMatch() throws Exception {
        NestedLoop outer = e.plan("select count(*) from users t1, users t2, users t3 WHERE 1=2");
        NestedLoop inner = (NestedLoop) outer.left();
        assertThat(((RoutedCollectPhase)((Collect)outer.right()).collectPhase()).whereClause().noMatch(), is(true));
        assertThat(((RoutedCollectPhase)((Collect)inner.left()).collectPhase()).whereClause().noMatch(), is(true));
        assertThat(((RoutedCollectPhase)((Collect)inner.right()).collectPhase()).whereClause().noMatch(), is(true));
    }
}
