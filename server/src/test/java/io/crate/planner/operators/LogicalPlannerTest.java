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

package io.crate.planner.operators;

import static io.crate.testing.MemoryLimits.assertMaxBytesAllocated;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.elasticsearch.common.unit.ByteSizeUnit;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.junit.Before;
import org.junit.Test;

import io.crate.analyze.TableDefinitions;
import io.crate.execution.dsl.projection.Projection;
import io.crate.execution.dsl.projection.LimitDistinctProjection;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.table.TableInfo;
import io.crate.statistics.ColumnStats;
import io.crate.statistics.MostCommonValues;
import io.crate.statistics.Stats;
import io.crate.statistics.TableStats;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.testing.T3;
import io.crate.types.DataTypes;

public class LogicalPlannerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor sqlExecutor;
    private TableStats tableStats;

    @Before
    public void prepare() throws IOException {
        tableStats = new TableStats();
        sqlExecutor = SQLExecutor.builder(clusterService)
            .addTable(TableDefinitions.USER_TABLE_DEFINITION)
            .addTable(T3.T1_DEFINITION)
            .addTable(T3.T2_DEFINITION)
            .setTableStats(tableStats)
            .addView(new RelationName("doc", "v2"), "select a, x from doc.t1")
            .addView(new RelationName("doc", "v3"), "select a, x from doc.t1")
            .build();
    }

    private LogicalPlan plan(String statement) {
        return assertMaxBytesAllocated(ByteSizeUnit.MB.toBytes(28), () -> sqlExecutor.logicalPlan(statement));
    }

    @Test
    public void test_collect_derives_estimated_size_per_row_from_stats_and_types() {
        // no stats -> size derived from fixed with type
        LogicalPlan plan = plan("select x from t1");
        assertThat(plan.estimatedRowSize(), is((long) DataTypes.INTEGER.fixedSize()));

        TableInfo t1 = sqlExecutor.resolveTableInfo("t1");
        ColumnStats<Integer> columnStats = new ColumnStats<>(
            0.0, 50L, 2, DataTypes.INTEGER, MostCommonValues.EMPTY, List.of());
        tableStats.updateTableStats(Map.of(t1.ident(), new Stats(2L, 100L, Map.of(new ColumnIdent("x"), columnStats))));

        // stats present -> size derived from them (although bogus fake stats in this case)
        plan = plan("select x from t1");
        assertThat(plan.estimatedRowSize(), is(50L));
    }

    @Test
    public void testAvgWindowFunction() {
        LogicalPlan plan = plan("select avg(x) OVER() from t1");
        assertThat(plan, isPlan(
            "Eval[avg(x) OVER ()]\n" +
            "  └ WindowAgg[x, avg(x) OVER ()]\n" +
            "    └ Collect[doc.t1 | [x] | true]"
        ));
    }

    @Test
    public void testAggregationOnTableFunction() throws Exception {
        LogicalPlan plan = plan("select max(unnest) from unnest([1, 2, 3])");
        assertThat(plan, isPlan(
            "HashAggregate[max(unnest)]\n" +
            "  └ TableFunction[unnest | [unnest] | true]"
        ));
    }

    @Test
    public void testQTFWithOrderBy() throws Exception {
        LogicalPlan plan = plan("select a, x from t1 order by a");
        assertThat(plan, isPlan(
            "Fetch[a, x]\n" +
            "  └ OrderBy[a ASC]\n" +
            "    └ Collect[doc.t1 | [_fetchid, a] | true]"
        ));
    }

    @Test
    public void testQTFWithOrderByAndAlias() throws Exception {
        LogicalPlan plan = plan("select a, x from t1 as t order by a");
        assertThat(plan, isPlan(
            "Fetch[a, x]\n" +
            "  └ Rename[t._fetchid, a] AS t\n" +
            "    └ OrderBy[a ASC]\n" +
            "      └ Collect[doc.t1 | [_fetchid, a] | true]"
        ));
    }

    @Test
    public void testQTFWithoutOrderBy() throws Exception {
        LogicalPlan plan = plan("select a, x from t1");
        assertThat(plan, isPlan("Collect[doc.t1 | [a, x] | true]"));
    }

    @Test
    public void testSimpleSelectQAFAndLimit() throws Exception {
        LogicalPlan plan = plan("select a from t1 order by a limit 10 offset 5");
        assertThat(plan, isPlan(
            "Limit[10::bigint;5::bigint]\n" +
            "  └ OrderBy[a ASC]\n" +
            "    └ Collect[doc.t1 | [a] | true]"
        ));
    }

    @Test
    public void testSelectOnVirtualTableWithOrderBy() throws Exception {
        LogicalPlan plan = plan("select a, x from (" +
                                "   select a, x from t1 order by a limit 3) tt " +
                                "order by x desc limit 1");
        assertThat(plan, isPlan(
            "Rename[a, x] AS tt\n" +
            "  └ Limit[1::bigint;0]\n" +
            "    └ OrderBy[x DESC]\n" +
            "      └ Fetch[a, x]\n" +
            "        └ Limit[3::bigint;0]\n" +
            "          └ OrderBy[a ASC]\n" +
            "            └ Collect[doc.t1 | [_fetchid, a] | true]"));
    }

    @Test
    public void testIntermediateFetch() throws Exception {
        LogicalPlan plan = plan("select sum(x) from (select x from t1 limit 10) tt");
        assertThat(plan, isPlan(
            "HashAggregate[sum(x)]\n" +
            "  └ Rename[x] AS tt\n" +
            "    └ Fetch[x]\n" +
            "      └ Limit[10::bigint;0]\n" +
            "        └ Collect[doc.t1 | [_fetchid] | true]"));
    }

    @Test
    public void testHavingGlobalAggregation() throws Exception {
        LogicalPlan plan = plan("select min(a), min(x) from t1 having min(x) < 33 and max(x) > 100");
        assertThat(plan, isPlan(
            "Eval[min(a), min(x)]\n" +
            "  └ Filter[((min(x) < 33) AND (max(x) > 100))]\n" +
            "    └ HashAggregate[min(a), min(x), max(x)]\n" +
            "      └ Collect[doc.t1 | [a, x] | true]"
        ));
    }

    @Test
    public void testHavingGlobalAggregationAndRelationAlias() throws Exception {
        LogicalPlan plan = plan("select min(a), min(x) from t1 as tt having min(tt.x) < 33 and max(tt.x) > 100");
        assertThat(plan, isPlan(
            "Eval[min(a), min(x)]\n" +
            "  └ Filter[((min(x) < 33) AND (max(x) > 100))]\n" +
            "    └ HashAggregate[min(a), min(x), max(x)]\n" +
            "      └ Rename[a, x] AS tt\n" +
            "        └ Collect[doc.t1 | [a, x] | true]"));
    }

    @Test
    public void testSelectCountStarIsOptimized() throws Exception {
        LogicalPlan plan = plan("select count(*) from t1 where x > 10");
        assertThat(plan, isPlan("Count[doc.t1 | (x > 10)]"));
    }

    @Test
    public void test_select_count_star_on_aliased_table_is_optimized() throws Exception {
        LogicalPlan plan = plan("select count(*) from t1 as t");
        assertThat(plan, isPlan("Count[doc.t1 | true]"));
    }

    @Test
    public void test_select_count_star_is_optimized_if_there_is_a_single_agg_in_select_list() {
        LogicalPlan plan = plan("SELECT COUNT(*), COUNT(x) FROM t1 WHERE x > 10");
        assertThat(plan, isPlan(
            "HashAggregate[count(*), count(x)]\n" +
            "  └ Collect[doc.t1 | [x] | (x > 10)]"));
    }

    @Test
    public void testSelectCountStarIsOptimizedOnNestedSubqueries() throws Exception {
        LogicalPlan plan = plan("select * from t1 where x > (select 1 from t1 where x > (select count(*) from t2 limit 1)::integer)");
        // instead of a Collect plan, this must result in a CountPlan through optimization
        assertThat(plan, isPlan(
            "MultiPhase\n" +
            "  └ Collect[doc.t1 | [a, x, i] | (x > (SELECT 1 FROM (doc.t1)))]\n" +
            "  └ Limit[2::bigint;0::bigint]\n" +
            "    └ MultiPhase\n" +
            "      └ Collect[doc.t1 | [1] | (x > cast((SELECT count(*) FROM (doc.t2)) AS integer))]\n" +
            "      └ Limit[2::bigint;0::bigint]\n" +
            "        └ Limit[1::bigint;0]\n" +
            "          └ Count[doc.t2 | true]"
        ));
    }

    @Test
    public void testSelectCountStarIsOptimizedInsideRelations() {
        LogicalPlan plan = plan("select t2.i, cnt from " +
                               " (select count(*) as cnt from t1) t1 " +
                               "join" +
                               " (select i from t2 limit 1) t2 " +
                               "on t1.cnt = t2.i::long ");
        assertThat(plan, isPlan(
            "Eval[i, cnt]\n" +
            "  └ HashJoin[(cnt = cast(i AS bigint))]\n" +
            "    ├ Rename[cnt] AS t1\n" +
            "    │  └ Eval[count(*) AS cnt]\n" +
            "    │    └ Count[doc.t1 | true]\n" +
            "    └ Rename[i] AS t2\n" +
            "      └ Fetch[i]\n" +
            "        └ Limit[1::bigint;0]\n" +
            "          └ Collect[doc.t2 | [_fetchid] | true]"));
    }

    @Test
    public void testJoinTwoTables() {
        LogicalPlan plan = plan("select " +
                                "   t1.x, t1.a, t2.y " +
                                "from " +
                                "   t1 " +
                                "   inner join t2 on t1.x = t2.y " +
                                "order by t1.x " +
                                "limit 10");
        assertThat(plan, isPlan(
            "Fetch[x, a, y]\n" +
            "  └ Limit[10::bigint;0]\n" +
            "    └ OrderBy[x ASC]\n" +
            "      └ HashJoin[(x = y)]\n" +
            "        ├ Collect[doc.t1 | [_fetchid, x] | true]\n" +
            "        └ Collect[doc.t2 | [y] | true]"));
    }

    @Test
    public void testScoreColumnIsCollectedNotFetched() throws Exception {
        LogicalPlan plan = plan("select x, _score from t1");
        assertThat(plan, isPlan("Collect[doc.t1 | [x, _score] | true]"));
    }

    @Test
    public void testInWithSubqueryOrderImplicitlyApplied() {
        LogicalPlan plan = plan("select x from t1 where x in (select x from t1)");
        assertThat(plan.dependencies().entrySet().size(), is(1));
        LogicalPlan subPlan = plan.dependencies().keySet().iterator().next();
        assertThat(subPlan, isPlan(
            "OrderBy[x ASC]\n" +
            "  └ Collect[doc.t1 | [x] | true]"));
    }

    @Test
    public void testInWithSubqueryOrderImplicitlyAppliedWithExistingOrderBy() {
        LogicalPlan plan = plan("select x from t1 where x in (select x from t1 order by 1 desc limit 10)");
        assertThat(plan.dependencies().entrySet().size(), is(1));
        LogicalPlan subPlan = plan.dependencies().keySet().iterator().next();
        assertThat(subPlan, isPlan(
            "Limit[10::bigint;0]\n" +
            "  └ OrderBy[x DESC]\n" +
            "    └ Collect[doc.t1 | [x] | true]"));
    }

    @Test
    public void testInWithSubqueryOrderImplicitlyAppliedWithExistingOrderByOnDifferentField() {
        LogicalPlan plan = plan("select x from t1 where x in (select x from t1 order by a desc limit 10)");
        assertThat(plan.dependencies().entrySet().size(), is(1));
        LogicalPlan subPlan = plan.dependencies().keySet().iterator().next();
        assertThat(subPlan, isPlan(
            "Eval[x]\n" +
            "  └ OrderBy[x ASC]\n" +
            "    └ Limit[10::bigint;0]\n" +
            "      └ OrderBy[a DESC]\n" +
            "        └ Collect[doc.t1 | [x, a] | true]"));
    }

    @Test
    public void test_optimize_for_in_subquery_only_operates_on_primitive_types() {
        LogicalPlan plan = plan("select array(select {a = x} from t1)");
        assertThat(plan.dependencies().entrySet().size(), is(1));
        LogicalPlan subPlan = plan.dependencies().keySet().iterator().next();
        assertThat(subPlan, isPlan("Collect[doc.t1 | [_map('a', x)] | true]"));
    }

    @Test
    public void testParentQueryIsPushedDownAndMergedIntoSubRelationWhereClause() {
        LogicalPlan plan = plan("select * from " +
                                " (select a, i from t1 order by a limit 5) t1 " +
                                "inner join" +
                                " (select b, i from t2 where b > '10') t2 " +
                                "on t1.i = t2.i where t1.a > '50' and t2.b > '100' " +
                                "limit 10");
        assertThat(plan, isPlan(
            "Fetch[a, i, b, i]\n" +
            "  └ Limit[10::bigint;0]\n" +
            "    └ HashJoin[(i = i)]\n" +
            "      ├ Rename[a, i] AS t1\n" +
            "      │  └ Filter[(a > '50')]\n" +
            "      │    └ Fetch[a, i]\n" +
            "      │      └ Limit[5::bigint;0]\n" +
            "      │        └ OrderBy[a ASC]\n" +
            "      │          └ Collect[doc.t1 | [_fetchid, a] | true]\n" +
            "      └ Rename[t2._fetchid, i] AS t2\n" +
            "        └ Collect[doc.t2 | [_fetchid, i] | ((b > '100') AND (b > '10'))]"));
    }

    @Test
    public void testPlanOfJoinedViewsHasBoundaryWithViewOutputs() {
        LogicalPlan plan = plan("SELECT v2.x, v2.a, v3.x, v3.a " +
                              "FROM v2 " +
                              "  INNER JOIN v3 " +
                              "  ON v2.x= v3.x");
        assertThat(plan, isPlan(
            "Eval[x, a, x, a]\n" +
            "  └ HashJoin[(x = x)]\n" +
            "    ├ Rename[a, x] AS doc.v2\n" +
            "    │  └ Collect[doc.t1 | [a, x] | true]\n" +
            "    └ Rename[a, x] AS doc.v3\n" +
            "      └ Collect[doc.t1 | [a, x] | true]"));
    }

    @Test
    public void testAliasedPrimaryKeyLookupHasGetPlan() {
        LogicalPlan plan = plan("select name from users u where id = 1");
        assertThat(plan, isPlan(
            "Rename[name] AS u\n" +
            "  └ Get[doc.users | name | DocKeys{1::bigint} | (id = 1::bigint)]"));
    }

    @Test
    public void test_limit_distinct_limits_outputs_to_the_group_keys_if_source_has_more_outputs() {
        String statement = "select name, other_id " +
                           "from (select name, awesome, other_id from users) u " +
                           "group by name, other_id limit 20";
        LogicalPlan plan = plan(
            statement);
        assertThat(
            plan,
            isPlan(
                "LimitDistinct[20::bigint;0 | [name, other_id]]\n" +
                "  └ Rename[name, other_id] AS u\n" +
                "    └ Collect[doc.users | [name, other_id] | true]"
            )
        );
        io.crate.planner.node.dql.Collect collect = sqlExecutor.plan(statement);
        List<Projection> projections = collect.collectPhase().projections();
        assertThat(projections, contains(
            instanceOf(LimitDistinctProjection.class),
            instanceOf(LimitDistinctProjection.class)
        ));
        assertThat(projections.get(0).requiredGranularity(), is(RowGranularity.SHARD));
        assertThat(projections.get(1).requiredGranularity(), is(RowGranularity.CLUSTER));
    }

    @Test
    public void test_limit_on_join_is_rewritten_to_query_then_fetch() {
        LogicalPlan plan = plan("select * from t1, t2 limit 3");
        assertThat(
            plan,
            isPlan(
                "Fetch[a, x, i, b, y, i]\n" +
                "  └ Limit[3::bigint;0]\n" +
                "    └ NestedLoopJoin[CROSS]\n" +
                "      ├ Collect[doc.t1 | [_fetchid] | true]\n" +
                "      └ Collect[doc.t2 | [_fetchid] | true]"
            )
        );
    }

    @Test
    public void test_limit_on_hash_join_is_rewritten_to_query_then_fetch() {
        LogicalPlan plan = plan("select * from t1 inner join t2 on t1.a = t2.b limit 3");
        assertThat(
            plan,
            isPlan(
                "Fetch[a, x, i, b, y, i]\n" +
                "  └ Limit[3::bigint;0]\n" +
                "    └ HashJoin[(a = b)]\n" +
                "      ├ Collect[doc.t1 | [_fetchid, a] | true]\n" +
                "      └ Collect[doc.t2 | [_fetchid, b] | true]"
            )
        );
    }

    @Test
    public void test_unused_table_function_in_subquery_is_not_pruned() {
        LogicalPlan plan = plan("select name from (select name, unnest(counters), text from users) u");
        assertThat(plan, isPlan(
            "Rename[name] AS u\n" +
            "  └ Eval[name]\n" +
            "    └ ProjectSet[unnest(counters), name]\n" +
            "      └ Collect[doc.users | [counters, name] | true]"
        ));
    }

    @Test
    public void test_group_by_with_alias_and_limit_distinct_rewrite_creates_valid_plan() {
        TableInfo t1 = sqlExecutor.resolveTableInfo("t1");
        tableStats.updateTableStats(Map.of(t1.ident(), new Stats(100L, 100L, Map.of())));
        LogicalPlan plan = plan("select a as b from doc.t1 group by a limit 10");
        assertThat(plan, isPlan(
            "Eval[a AS b]\n" +
            "  └ LimitDistinct[10::bigint;0 | [a]]\n" +
            "    └ Collect[doc.t1 | [a] | true]"
        ));
    }

    @Test
    public void test_query_uses_fetch_if_there_is_a_nested_loop_join_where_only_one_side_can_utilize_fetch() throws Exception {
        // (uses like to force NL instead of hashjoin)
        LogicalPlan plan = plan("""
            select * from (select distinct name from users) u
            inner join t1 on t1.a like u.name
            limit 10
            """);
        assertThat(plan, isPlan(
            "Fetch[name, a, x, i]\n" +
            "  └ Limit[10::bigint;0]\n" +
            "    └ NestedLoopJoin[INNER | (a LIKE name)]\n" +
            "      ├ Rename[name] AS u\n" +
            "      │  └ GroupHashAggregate[name]\n" +
            "      │    └ Collect[doc.users | [name] | true]\n" +
            "      └ Collect[doc.t1 | [_fetchid, a] | true]"
        ));
    }

    @Test
    public void test_query_uses_fetch_if_there_is_a_hash_join_where_only_one_side_can_utilize_fetch() throws Exception {
        LogicalPlan plan = plan("""
            select * from (select distinct name from users) u
            inner join t1 on t1.a = u.name
            limit 10
            """);
        assertThat(plan, isPlan(
            "Fetch[name, a, x, i]\n" +
            "  └ Limit[10::bigint;0]\n" +
            "    └ HashJoin[(a = name)]\n" +
            "      ├ Rename[name] AS u\n" +
            "      │  └ GroupHashAggregate[name]\n" +
            "      │    └ Collect[doc.users | [name] | true]\n" +
            "      └ Collect[doc.t1 | [_fetchid, a] | true]"
        ));
    }

    @Test
    public void test_orderBy_not_optimized_for_array_subquery_expression() {
        LogicalPlan plan = plan("select array(select x from t1 order by a desc limit 10)");
        assertThat(plan.dependencies().entrySet().size(), is(1));
        LogicalPlan subPlan = plan.dependencies().keySet().iterator().next();
        assertThat(subPlan, isPlan(
            "Eval[x]\n" +
            "  └ Limit[10::bigint;0]\n" +
            "    └ OrderBy[a DESC]\n" +
            "      └ Collect[doc.t1 | [x, a] | true]"));
    }

    @Test
    public void test_eval_qtf_doesnt_unwrap_non_fetchable_aliases() {
        // To reproduce https://github.com/crate/crate/issues/13414  we need:
        // 1. select at least one fetchable column (t1.i) to really kick in Query-Then-Fetch execution, just limit is not enough.
        // 2. select used in join aliased column
        // 3. use virtual table (view or subselect)
        // 4. use limit on the whole query
        LogicalPlan plan = plan("""
            select * from generate_series(1, 2)
            cross join
            (select t1.i, t2.y AS aliased from t1 inner join t2 on t1.x = t2.y) v
            limit 10
            """);
        assertThat(plan, isPlan(
            """
                Fetch[generate_series, i, aliased]
                  └ Limit[10::bigint;0]
                    └ NestedLoopJoin[CROSS]
                      ├ TableFunction[generate_series | [generate_series] | true]
                      └ Rename[v._fetchid, aliased] AS v
                        └ Eval[_fetchid, y AS aliased]
                          └ HashJoin[(x = y)]
                            ├ Collect[doc.t1 | [_fetchid, x] | true]
                            └ Collect[doc.t2 | [y] | true]"""
        ));
    }

    public static String printPlan(LogicalPlan logicalPlan) {
        var printContext = new PrintContext();
        logicalPlan.print(printContext);
        return printContext.toString();
    }

    public static Matcher<LogicalPlan> isPlan(String expectedPlan) {
        return new FeatureMatcher<>(equalTo(expectedPlan), "same output", "output ") {

            @Override
            protected String featureValueOf(LogicalPlan actual) {
                return printPlan(actual);
            }
        };
    }

    public static Matcher<LogicalPlan> isPlan(LogicalPlan expectedPlan) {
        return new FeatureMatcher<>(equalTo(printPlan(expectedPlan)), "same output", "output ") {

            @Override
            protected String featureValueOf(LogicalPlan actual) {
                return printPlan(actual);
            }
        };
    }

}
