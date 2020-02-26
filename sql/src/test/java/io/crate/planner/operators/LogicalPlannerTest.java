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

package io.crate.planner.operators;

import io.crate.analyze.OrderBy;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.format.SymbolPrinter;
import io.crate.metadata.Functions;
import io.crate.metadata.RelationName;
import io.crate.planner.PlannerContext;
import io.crate.planner.SubqueryPlanner;
import io.crate.statistics.TableStats;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import org.elasticsearch.cluster.service.ClusterService;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Collectors;

import static io.crate.testing.TestingHelpers.getFunctions;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class LogicalPlannerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor sqlExecutor;
    private TableStats tableStats;

    @Before
    public void prepare() throws IOException {
        sqlExecutor = SQLExecutor.builder(clusterService)
            .enableDefaultTables()
            .addView(new RelationName("doc", "v2"), "select a, x from doc.t1")
            .addView(new RelationName("doc", "v3"), "select a, x from doc.t1")
            .build();
        tableStats = new TableStats();
    }

    private LogicalPlan plan(String statement) {
        return plan(statement, sqlExecutor, clusterService, tableStats);
    }

    @Test
    public void testAvgWindowFunction() {
        LogicalPlan plan = plan("select avg(x) OVER() from t1");
        assertThat(plan, isPlan("Eval[avg(x)]\n" +
                                "WindowAgg[avg(x)]\n" +
                                "Collect[doc.t1 | [x] | true]\n"));
    }

    @Test
    public void testAggregationOnTableFunction() throws Exception {
        LogicalPlan plan = plan("select max(col1) from unnest([1, 2, 3])");
        assertThat(plan, isPlan("Aggregate[max(col1)]\n" +
                                "TableFunction[unnest | [col1] | true]\n"));
    }

    @Test
    public void testQTFWithOrderBy() throws Exception {
        LogicalPlan plan = plan("select a, x from t1 order by a");
        assertThat(plan, isPlan("OrderBy[a ASC]\n" +
                                "Collect[doc.t1 | [a, x] | true]\n"));
    }

    @Test
    public void testQTFWithOrderByAndAlias() throws Exception {
        LogicalPlan plan = plan("select a, x from t1 as t order by a");
        assertThat(plan, isPlan(
            "Rename[a, x] AS t\n" +
            "OrderBy[a ASC]\n" +
            "Collect[doc.t1 | [a, x] | true]\n"));
    }

    @Test
    public void testQTFWithoutOrderBy() throws Exception {
        LogicalPlan plan = plan("select a, x from t1");
        assertThat(plan, isPlan("Collect[doc.t1 | [a, x] | true]\n"));
    }

    @Test
    public void testSimpleSelectQAFAndLimit() throws Exception {
        LogicalPlan plan = plan("select a from t1 order by a limit 10 offset 5");
        assertThat(plan, isPlan("Limit[10;5]\n" +
                                "OrderBy[a ASC]\n" +
                                "Collect[doc.t1 | [a] | true]\n"));
    }

    @Test
    public void testSelectOnVirtualTableWithOrderBy() throws Exception {
        LogicalPlan plan = plan("select a, x from (" +
                                "   select a, x from t1 order by a limit 3) tt " +
                                "order by x desc limit 1");
        assertThat(plan, isPlan("Limit[1;0]\n" +
                                "Rename[a, x] AS tt\n" +
                                "OrderBy[x DESC]\n" +
                                "Limit[3;0]\n" +
                                "OrderBy[a ASC]\n" +
                                "Collect[doc.t1 | [a, x] | true]\n"));
    }

    @Test
    public void testIntermediateFetch() throws Exception {
        LogicalPlan plan = plan("select sum(x) from (select x from t1 limit 10) tt");
        assertThat(plan, isPlan("Aggregate[sum(x)]\n" +
                                "Rename[x] AS tt\n" +       // Aliased relation boundary
                                "Limit[10;0]\n" +
                                "Collect[doc.t1 | [x] | true]\n"));
    }

    @Test
    public void testHavingGlobalAggregation() throws Exception {
        LogicalPlan plan = plan("select min(a), min(x) from t1 having min(x) < 33 and max(x) > 100");
        assertThat(plan, isPlan("Eval[min(a), min(x)]\n" +
                                "Filter[((min(x) < 33) AND (max(x) > 100))]\n" +
                                "Aggregate[min(a), min(x), max(x)]\n" +
                                "Collect[doc.t1 | [a, x] | true]\n"));
    }

    @Test
    public void testHavingGlobalAggregationAndRelationAlias() throws Exception {
        LogicalPlan plan = plan("select min(a), min(x) from t1 as tt having min(tt.x) < 33 and max(tt.x) > 100");
        assertThat(plan, isPlan("Eval[min(a), min(x)]\n" +
                                "Filter[((min(x) < 33) AND (max(x) > 100))]\n" +
                                "Aggregate[min(a), min(x), max(x)]\n" +
                                "Rename[a, x] AS tt\n" +
                                "Collect[doc.t1 | [a, x] | true]\n"));
    }

    @Test
    public void testSelectCountStarIsOptimized() throws Exception {
        LogicalPlan plan = plan("select count(*) from t1 where x > 10");
        assertThat(plan, isPlan("Count[doc.t1 | (x > 10)]\n"));
    }

    @Test
    public void test_select_count_star_is_optimized_if_there_is_a_single_agg_in_select_list() {
        LogicalPlan plan = plan("SELECT COUNT(*), COUNT(x) FROM t1 WHERE x > 10");
        assertThat(plan, isPlan("Aggregate[count(*), count(x)]\n" +
                                "Collect[doc.t1 | [x] | (x > 10)]\n"));
    }

    @Test
    public void testSelectCountStarIsOptimizedOnNestedSubqueries() throws Exception {
        LogicalPlan plan = plan("select * from t1 where x > (select 1 from t1 where x > (select count(*) from t2 limit 1)::integer)");
        // instead of a Collect plan, this must result in a CountPlan through optimization
        assertThat(plan, isPlan("MultiPhase[\n" +
                                "    subQueries[\n" +
                                "        RootBoundary[1]\n" +
                                "        Limit[2;0]\n" + // implicitly added limit to enforce max1row
                                "        MultiPhase[\n" +
                                "            subQueries[\n" +
                                "                RootBoundary[count(*)]\n" +
                                "                Limit[2;0]\n" +
                                "                Limit[1;0]\n" +
                                "                Count[doc.t2 | true]\n" +
                                "            ]\n" +
                                "            Collect[doc.t1 | [1] | (x > cast(SelectSymbol{bigint_array} AS integer))]\n" +
                                "        ]\n" +
                                "    ]\n" +
                                "    Collect[doc.t1 | [a, x, i] | (x > cast(SelectSymbol{bigint_array} AS integer))]\n" +
                                "]\n"));
    }

    @Test
    public void testSelectCountStarIsOptimizedInsideRelations() {
        LogicalPlan plan = plan("select t2.i, cnt from " +
                               " (select count(*) as cnt from t1) t1 " +
                               "join" +
                               " (select i from t2 limit 1) t2 " +
                               "on t1.cnt = t2.i::long ");
        assertThat(plan, isPlan("Eval[i, cnt]\n" +
                                "HashJoin[\n" +
                                "    Rename[cnt] AS t1\n" +     // Aliased relation boundary
                                "    Eval[count(*) AS cnt]\n" +
                                "    Count[doc.t1 | true]\n" +
                                "    --- INNER ---\n" +
                                "    Rename[i] AS t2\n" +       // Aliased relation boundary
                                "    Limit[1;0]\n" +
                                "    Collect[doc.t2 | [i] | true]\n" +
                                "]\n"));
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
        assertThat(plan, isPlan("Limit[10;0]\n" +
                                "OrderBy[x ASC]\n" +
                                "HashJoin[\n" +
                                "    Collect[doc.t1 | [x, a] | true]\n" +
                                "    --- INNER ---\n" +
                                "    Collect[doc.t2 | [y] | true]\n" +
                                "]\n"));
    }

    @Test
    public void testScoreColumnIsCollectedNotFetched() throws Exception {
        LogicalPlan plan = plan("select x, _score from t1");
        assertThat(plan, isPlan("Collect[doc.t1 | [x, _score] | true]\n"));
    }

    @Test
    public void testInWithSubqueryOrderImplicitlyApplied() {
        LogicalPlan plan = plan("select x from t1 where x in (select x from t1)");
        assertThat(plan.dependencies().entrySet().size(), is(1));
        LogicalPlan subPlan = plan.dependencies().keySet().iterator().next();
        assertThat(subPlan, isPlan("RootBoundary[x]\n" +
                                   "OrderBy[x ASC]\n" +
                                   "Collect[doc.t1 | [x] | true]\n"));
    }

    @Test
    public void testInWithSubqueryOrderImplicitlyAppliedWithExistingOrderBy() {
        LogicalPlan plan = plan("select x from t1 where x in (select x from t1 order by 1 desc limit 10)");
        assertThat(plan.dependencies().entrySet().size(), is(1));
        LogicalPlan subPlan = plan.dependencies().keySet().iterator().next();
        assertThat(subPlan, isPlan("RootBoundary[x]\n" +
                                   "Limit[10;0]\n" +
                                   "OrderBy[x DESC]\n" +
                                   "Collect[doc.t1 | [x] | true]\n"));
    }

    @Test
    public void testInWithSubqueryOrderImplicitlyAppliedWithExistingOrderByOnDifferentField() {
        LogicalPlan plan = plan("select x from t1 where x in (select x from t1 order by a desc limit 10)");
        assertThat(plan.dependencies().entrySet().size(), is(1));
        LogicalPlan subPlan = plan.dependencies().keySet().iterator().next();
        assertThat(subPlan, isPlan("RootBoundary[x]\n" +
                                   "Eval[x]\n" +
                                   "OrderBy[x ASC]\n" +
                                   "Limit[10;0]\n" +
                                   "OrderBy[a DESC]\n" +
                                   "Collect[doc.t1 | [x, a] | true]\n"));
    }

    @Test
    public void test_optimize_for_in_subquery_only_operates_on_primitive_types() {
        LogicalPlan plan = plan("select array(select {a = x} from t1)");
        assertThat(plan.dependencies().entrySet().size(), is(1));
        LogicalPlan subPlan = plan.dependencies().keySet().iterator().next();
        assertThat(subPlan, isPlan("RootBoundary[_map('a', x)]\n" +
                                   "Collect[doc.t1 | [_map('a', x)] | true]\n"));
    }

    @Test
    public void testParentQueryIsPushedDownAndMergedIntoSubRelationWhereClause() {
        LogicalPlan plan = plan("select * from " +
                                " (select a, i from t1 order by a limit 5) t1 " +
                                "inner join" +
                                " (select b, i from t2 where b > 10) t2 " +
                                "on t1.i = t2.i where t1.a > 50 and t2.b > 100 " +
                                "limit 10");
        assertThat(plan, isPlan("Limit[10;0]\n" +
                                "HashJoin[\n" +
                                "    Rename[a, i] AS t1\n" +    // Aliased relation boundary
                                "    Filter[(a > '50')]\n" +
                                "    Limit[5;0]\n" +
                                "    OrderBy[a ASC]\n" +
                                "    Collect[doc.t1 | [a, i] | true]\n" +
                                "    --- INNER ---\n" +
                                "    Rename[b, i] AS t2\n" +    // Aliased relation boundary
                                "    Collect[doc.t2 | [b, i] | ((b > '100') AND (b > '10'))]\n" +
                                "]\n"));
    }

    @Test
    public void testPlanOfJoinedViewsHasBoundaryWithViewOutputs() {
        LogicalPlan plan = plan("SELECT v2.x, v2.a, v3.x, v3.a " +
                              "FROM v2 " +
                              "  INNER JOIN v3 " +
                              "  ON v2.x= v3.x");
        assertThat(plan, isPlan("Eval[x, a, x, a]\n" +
                                "HashJoin[\n" +
                                "    Rename[a, x] AS doc.v2\n" +
                                "    Collect[doc.t1 | [a, x] | true]\n" +
                                "    --- INNER ---\n" +
                                "    Rename[a, x] AS doc.v3\n" +
                                "    Collect[doc.t1 | [a, x] | true]\n" +
                                "]\n"));
    }

    @Test
    public void testAliasedPrimaryKeyLookupHasGetPlan() {
        LogicalPlan plan = plan("select name from users u where id = 1");
        assertThat(plan, isPlan("Rename[name] AS u\n" +
                                "Get[doc.users | name | DocKeys{1}"));
    }

    public static LogicalPlan plan(String statement,
                                   SQLExecutor sqlExecutor,
                                   ClusterService clusterService,
                                   TableStats tableStats) {
        PlannerContext context = sqlExecutor.getPlannerContext(clusterService.state());
        AnalyzedRelation relation = sqlExecutor.analyze(statement);
        LogicalPlanner logicalPlanner = new LogicalPlanner(
            getFunctions(),
            tableStats,
            () -> clusterService.state().nodes().getMinNodeVersion()
        );
        SubqueryPlanner subqueryPlanner = new SubqueryPlanner((s) -> logicalPlanner.planSubSelect(s, context));

        return logicalPlanner.normalizeAndPlan(relation, context, subqueryPlanner, Set.of());
    }

    public static Matcher<LogicalPlan> isPlan(Functions functions, String expectedPlan) {
        return new FeatureMatcher<>(equalTo(expectedPlan), "same output", "output ") {

            @Override
            protected String featureValueOf(LogicalPlan actual) {
                Printer printer = new Printer();
                return printer.printPlan(actual);
            }
        };
    }

    public String printPlan(LogicalPlan plan) {
        Printer printer = new Printer();
        return printer.printPlan(plan);
    }

    private Matcher<LogicalPlan> isPlan(String expectedPlan) {
        return isPlan(sqlExecutor.functions(), expectedPlan);
    }

    private static class Printer {

        private final StringBuilder sb;

        private int indentation = 0;

        Printer() {
            this.sb = new StringBuilder();
        }

        private void startLine(String start) {
            assert indentation >= 0 : "indentation must not get negative";
            sb.append(" ".repeat(indentation));
            sb.append(start);
        }

        private String printPlan(LogicalPlan plan) {
            if (plan instanceof RootRelationBoundary) {
                RootRelationBoundary boundary = (RootRelationBoundary) plan;
                startLine("RootBoundary[");
                addSymbolsList(boundary.outputs());
                sb.append("]\n");
                plan = boundary.source;
            }
            if (plan instanceof Rename) {
                Rename rename = (Rename) plan;
                startLine("Rename[");
                addSymbolsList(rename.outputs());
                sb.append("] AS ");
                sb.append(rename.name);
                sb.append("\n");
                plan = rename.source;
            }
            if (plan instanceof GroupHashAggregate) {
                GroupHashAggregate groupHashAggregate = (GroupHashAggregate) plan;
                startLine("GroupBy[");
                addSymbolsList(groupHashAggregate.groupKeys);
                sb.append(" | ");
                addSymbolsList(groupHashAggregate.aggregates);
                sb.append("]\n");
                plan = groupHashAggregate.source;
            }
            if (plan instanceof MultiPhase) {
                MultiPhase multiPhase = (MultiPhase) plan;
                startLine("MultiPhase[\n");
                indentation += 4;
                startLine("subQueries[\n");
                indentation += 4;
                for (Map.Entry<LogicalPlan, SelectSymbol> entry : multiPhase.dependencies().entrySet()) {
                    printPlan(entry.getKey());
                }
                indentation -= 4;
                startLine("]\n");
                printPlan(multiPhase.source);
                indentation -= 4;
                startLine("]\n");
                return sb.toString();
            }
            if (plan instanceof Eval) {
                Eval eval = (Eval) plan;
                startLine("Eval[");
                addSymbolsList(eval.outputs());
                sb.append("]\n");
                plan = eval.source;
            }
            if (plan instanceof Limit) {
                Limit limit = (Limit) plan;
                startLine("Limit[");
                sb.append(SymbolPrinter.printUnqualified(limit.limit));
                sb.append(';');
                sb.append(SymbolPrinter.printUnqualified(limit.offset));
                sb.append("]\n");
                plan = limit.source;
            }
            if (plan instanceof Order) {
                Order order = (Order) plan;
                startLine("OrderBy[");
                OrderBy.explainRepresentation(
                    sb,
                    order.orderBy.orderBySymbols()
                        .stream().map(s -> Literal.of(SymbolPrinter.printUnqualified(s))).collect(Collectors.toList()),
                    order.orderBy.reverseFlags(),
                    order.orderBy.nullsFirst());
                sb.append("]\n");
                plan = order.source;
            }
            if (plan instanceof Filter) {
                Filter filter = (Filter) plan;
                startLine("Filter[");
                sb.append(SymbolPrinter.printUnqualified(filter.query));
                sb.append("]\n");
                plan = filter.source;
            }
            if (plan instanceof HashAggregate) {
                HashAggregate aggregate = (HashAggregate) plan;
                startLine("Aggregate[");
                addSymbolsList(aggregate.aggregates);
                sb.append("]\n");
                plan = aggregate.source;
            }
            if (plan instanceof TopNDistinct) {
                var topNDistinct = (TopNDistinct) plan;
                startLine("TopNDistinct[");
                sb.append(SymbolPrinter.printUnqualified(topNDistinct.limit()));
                sb.append(" | [");
                addSymbolsList(topNDistinct.outputs());
                sb.append("]\n");
                plan = topNDistinct.source();
            }
            if (plan instanceof NestedLoopJoin) {
                NestedLoopJoin nestedLoopJoin = (NestedLoopJoin) plan;
                startLine("NestedLoopJoin[\n");
                indentation += 4;
                printPlan(nestedLoopJoin.lhs);
                startLine("--- ");
                sb.append(nestedLoopJoin.joinType());
                sb.append(" ---\n");
                printPlan(nestedLoopJoin.rhs);
                indentation -= 4;
                sb.append("]\n");
                return sb.toString();
            }
            if (plan instanceof HashJoin) {
                HashJoin hashJoin = (HashJoin) plan;
                startLine("HashJoin[\n");
                indentation += 4;
                printPlan(hashJoin.lhs);
                startLine("--- ");
                sb.append(hashJoin.joinType());
                sb.append(" ---\n");
                printPlan(hashJoin.rhs);
                indentation -= 4;
                sb.append("]\n");
                return sb.toString();
            }
            if (plan instanceof Collect) {
                Collect collect = (Collect) plan;
                startLine("Collect[");
                sb.append(collect.tableInfo.ident());
                sb.append(" | [");
                addSymbolsList(collect.outputs());
                sb.append("] | ");
                sb.append(SymbolPrinter.printUnqualified(collect.where.queryOrFallback()));
                sb.append("]\n");
                return sb.toString();
            }
            if (plan instanceof Count) {
                Count count = (Count) plan;
                startLine("Count[");
                sb.append(count.tableRelation.tableInfo().ident());
                sb.append(" | ");
                sb.append(SymbolPrinter.printUnqualified(count.where.queryOrFallback()));
                sb.append("]\n");
                return sb.toString();
            }
            if (plan instanceof Union) {
                Union union = (Union) plan;
                startLine("Union[\n");
                printPlan(union.lhs);
                startLine("---\n");
                printPlan(union.rhs);
                sb.append("]\n");
                return sb.toString();
            }
            if (plan instanceof Get) {
                Get get = (Get) plan;
                startLine("Get[");
                sb.append(get.tableRelation.tableInfo().ident());
                sb.append(" | ");
                addSymbolsList(get.outputs());
                sb.append(" | ");
                sb.append(get.docKeys);
                return sb.toString();
            }
            if (plan instanceof TableFunction) {
                TableFunction tableFunction = (TableFunction) plan;
                startLine("TableFunction[");
                sb.append(tableFunction.relation().function().info().ident().name());
                sb.append(" | [");
                addSymbolsList(tableFunction.outputs());
                sb.append("] | ");
                sb.append(SymbolPrinter.printUnqualified(tableFunction.where.queryOrFallback()));
                sb.append("]\n");
                return sb.toString();
            }
            if (plan instanceof ProjectSet) {
                ProjectSet projectSet = (ProjectSet) plan;
                startLine("ProjectSet[");
                addSymbolsList(projectSet.tableFunctions);
                if (!projectSet.standalone.isEmpty()) {
                    sb.append(" | ");
                    addSymbolsList(projectSet.standalone);
                }
                sb.append("]\n");
                plan = projectSet.source;
            }
            if (plan instanceof WindowAgg) {
                WindowAgg windowAgg = (WindowAgg) plan;
                startLine("WindowAgg[");
                addSymbolsList(windowAgg.windowFunctions());
                if (!windowAgg.windowDefinition.partitions().isEmpty()) {
                    sb.append(" | PARTITION BY ");
                    addSymbolsList(windowAgg.windowDefinition.partitions());
                }
                OrderBy orderBy = windowAgg.windowDefinition.orderBy();
                if (orderBy != null) {
                    sb.append(" | ORDER BY ");
                    OrderBy.explainRepresentation(sb, orderBy.orderBySymbols(), orderBy.reverseFlags(), orderBy.nullsFirst());
                }
                sb.append("]\n");
                plan = windowAgg.source;
            }
            return printPlan(plan);
        }

        private void addSymbolsList(Iterable<? extends Symbol> symbols) {
            StringJoiner commaJoiner = new StringJoiner(", ");
            for (Symbol symbol : symbols) {
                commaJoiner.add(SymbolPrinter.printUnqualified(symbol));
            }
            sb.append(commaJoiner.toString());
        }
    }
}
