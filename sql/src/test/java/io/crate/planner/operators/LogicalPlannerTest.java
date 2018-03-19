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
import io.crate.analyze.QueryClause;
import io.crate.analyze.relations.QueriedRelation;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.format.SymbolPrinter;
import io.crate.metadata.Functions;
import io.crate.planner.PlannerContext;
import io.crate.planner.SubqueryPlanner;
import io.crate.planner.TableStats;
import io.crate.planner.consumer.FetchMode;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import org.elasticsearch.cluster.service.ClusterService;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.StringJoiner;
import java.util.stream.Collectors;

import static io.crate.testing.TestingHelpers.getFunctions;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class LogicalPlannerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor sqlExecutor;
    private TableStats tableStats;

    @Before
    public void prepare() {
        sqlExecutor = SQLExecutor.builder(clusterService)
            .enableDefaultTables()
            .build();
        tableStats = new TableStats();
    }

    private LogicalPlan plan(String statement) {
        return plan(statement, sqlExecutor, clusterService, tableStats);
    }

    @Test
    public void testAggregationOnTableFunction() throws Exception {
        LogicalPlan plan = plan("select max(col1) from unnest([1, 2, 3])");
        assertThat(plan, isPlan("Aggregate[max(col1)]\n" +
                                "Collect[.unnest | [col1] | All]\n"));
    }

    @Test
    public void testQTFWithOrderBy() throws Exception {
        LogicalPlan plan = plan("select a, x from t1 order by a");
        assertThat(plan, isPlan("FetchOrEval[a, x]\n" +
                                "OrderBy['a' ASC]\n" +
                                "Collect[doc.t1 | [_fetchid, a] | All]\n"));
    }

    @Test
    public void testQTFWithoutOrderBy() throws Exception {
        LogicalPlan plan = plan("select a, x from t1");
        assertThat(plan, isPlan("FetchOrEval[a, x]\n" +
                                "Collect[doc.t1 | [_fetchid] | All]\n"));
    }

    @Test
    public void testSimpleSelectQAFAndLimit() throws Exception {
        LogicalPlan plan = plan("select a from t1 order by a limit 10 offset 5");
        assertThat(plan, isPlan("Limit[10;5]\n" +
                                "OrderBy['a' ASC]\n" +
                                "Collect[doc.t1 | [a] | All]\n"));
    }

    @Test
    public void testSelectOnVirtualTableWithOrderBy() throws Exception {
        LogicalPlan plan = plan("select a, x from (" +
                                "   select a, x from t1 order by a limit 3) tt " +
                                "order by x desc limit 1");
        assertThat(plan, isPlan("Limit[1;0]\n" +
                                "OrderBy['x' DESC]\n" +
                                "Boundary[a, x]\n" +
                                "Limit[3;0]\n" +
                                "OrderBy['a' ASC]\n" +
                                "Collect[doc.t1 | [a, x] | All]\n"));
    }

    @Test
    public void testIntermediateFetch() throws Exception {
        LogicalPlan plan = plan("select sum(x) from (select x from t1 limit 10) tt");
        assertThat(plan, isPlan("Aggregate[sum(x)]\n" +
                                "Boundary[x]\n" +
                                "FetchOrEval[x]\n" +
                                "Limit[10;0]\n" +
                                "Collect[doc.t1 | [_fetchid] | All]\n"));
    }

    @Test
    public void testHavingGlobalAggregation() throws Exception {
        LogicalPlan plan = plan("select min(a), min(x) from t1 having min(x) < 33 and max(x) > 100");
        assertThat(plan, isPlan("FetchOrEval[min(a), min(x)]\n" +
                                "Filter[((cast(min(x) AS long) < 33) AND (cast(max(x) AS long) > 100))]\n" +
                                "Aggregate[min(a), min(x), max(x)]\n" +
                                "Collect[doc.t1 | [a, x] | All]\n"));
    }

    @Test
    public void testSelectCountStarIsOptimized() throws Exception {
        LogicalPlan plan = plan("select count(*) from t1 where x > 10");
        assertThat(plan, isPlan("Count[doc.t1 | (x > 10)]\n"));
    }

    @Test
    public void testSelectCountStarIsOptimizedOnNestedSubqueries() throws Exception {
        LogicalPlan plan = plan("select * from t1 where x > (select 1 from t1 where x > (select count(*) from t2 limit 1)::integer)");
        // instead of a Collect plan, this must result in a CountPlan through optimization
        assertThat(plan, isPlan("MultiPhase[\n" +
                                "    subQueries[\n" +
                                "        RootBoundary[1]\n" +
                                "        MultiPhase[\n" +
                                "            subQueries[\n" +
                                "                RootBoundary[count(*)]\n" +
                                "                Limit[1;0]\n" +
                                "                Count[doc.t2 | All]\n" +
                                "            ]\n" +
                                "            Limit[2;0]\n" +
                                "            Collect[doc.t1 | [1] | (x > cast(SelectSymbol{long_table} AS integer))]\n" +
                                "        ]\n" +
                                "    ]\n" +
                                "    FetchOrEval[a, x, i]\n" +
                                "    Collect[doc.t1 | [_fetchid] | (x > cast(SelectSymbol{long_table} AS integer))]\n" +
                                "]\n"));
    }

    @Test
    public void testSelectCountStarIsOptimizedInsideRelations() {
        LogicalPlan plan = plan("select t2.i, cnt from " +
                               " (select count(*) as cnt from t1) t1 " +
                               "join" +
                               " (select i from t2 limit 1) t2 " +
                               "on t1.cnt = t2.i::long ");
        assertThat(plan, isPlan("FetchOrEval[i, cnt]\n" +
                                "HashJoin[\n" +
                                "    Boundary[cnt]\n" +
                                "    Count[doc.t1 | All]\n" +
                                "    --- INNER ---\n" +
                                "    Boundary[i]\n" +
                                "    Limit[1;0]\n" +
                                "    Collect[doc.t2 | [i] | All]\n" +
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
        assertThat(plan, isPlan("FetchOrEval[x, a, y]\n" +
                                "Limit[10;0]\n" +
                                "OrderBy['x' ASC]\n" +
                                "HashJoin[\n" +
                                "    Boundary[_fetchid, x]\n" +
                                "    FetchOrEval[_fetchid, x]\n" +
                                "    Collect[doc.t1 | [_fetchid, x] | All]\n" +
                                "    --- INNER ---\n" +
                                "    Boundary[y]\n" +
                                "    Collect[doc.t2 | [y] | All]\n" +
                                "]\n"));
    }

    @Test
    public void testScoreColumnIsCollectedNotFetched() throws Exception {
        LogicalPlan plan = plan("select x, _score from t1");
        assertThat(plan, isPlan("FetchOrEval[x, _score]\n" +
                                "Collect[doc.t1 | [_fetchid, _score] | All]\n"));
    }

    @Test
    public void testInWithSubqueryOrderImplicitlyApplied() {
        LogicalPlan plan = plan("select x from t1 where x in (select x from t1)");
        assertThat(plan.dependencies().entrySet().size(), is(1));
        LogicalPlan subPlan = plan.dependencies().keySet().iterator().next();
        assertThat(subPlan, isPlan("RootBoundary[x]\n" +
                                   "OrderBy['x' ASC NULLS LAST]\n" +
                                   "Collect[doc.t1 | [x] | All]\n"));
    }

    @Test
    public void testInWithSubqueryOrderImplicitlyAppliedWithExistingOrderBy() {
        LogicalPlan plan = plan("select x from t1 where x in (select x from t1 order by 1 desc limit 10)");
        assertThat(plan.dependencies().entrySet().size(), is(1));
        LogicalPlan subPlan = plan.dependencies().keySet().iterator().next();
        assertThat(subPlan, isPlan("RootBoundary[x]\n" +
                                   "Limit[10;0]\n" +
                                   "OrderBy['x' DESC]\n" +
                                   "Collect[doc.t1 | [x] | All]\n"));
    }

    @Test
    public void testInWithSubqueryOrderImplicitlyAppliedWithExistingOrderByOnDifferentField() {
        LogicalPlan plan = plan("select x from t1 where x in (select x from t1 order by a desc limit 10)");
        assertThat(plan.dependencies().entrySet().size(), is(1));
        LogicalPlan subPlan = plan.dependencies().keySet().iterator().next();
        assertThat(subPlan, isPlan("RootBoundary[x]\n" +
                                   "OrderBy['x' ASC NULLS LAST]\n" +
                                   "FetchOrEval[x]\n" +
                                   "Limit[10;0]\n" +
                                   "OrderBy['a' DESC]\n" +
                                   "Collect[doc.t1 | [x, a] | All]\n"));
    }

    public static LogicalPlan plan(String statement,
                                   SQLExecutor sqlExecutor,
                                   ClusterService clusterService,
                                   TableStats tableStats) {
        QueriedRelation relation = sqlExecutor.analyze(statement);
        PlannerContext context = sqlExecutor.getPlannerContext(clusterService.state());
        LogicalPlanner logicalPlanner = new LogicalPlanner(getFunctions(), tableStats);
        SubqueryPlanner subqueryPlanner = new SubqueryPlanner((s) -> logicalPlanner.planSubSelect(s, context));

        return logicalPlanner.plan(relation, context, subqueryPlanner, FetchMode.MAYBE_CLEAR);
    }

    public static Matcher<LogicalPlan> isPlan(Functions functions, String expectedPlan) {
        return new FeatureMatcher<LogicalPlan, String>(equalTo(expectedPlan), "same output", "output ") {


            @Override
            protected String featureValueOf(LogicalPlan actual) {
                Printer printer = new Printer(new SymbolPrinter(functions));
                return printer.printPlan(actual);
            }
        };
    }

    private Matcher<LogicalPlan> isPlan(String expectedPlan) {
        return isPlan(sqlExecutor.functions(), expectedPlan);
    }

    private static class Printer {

        private final StringBuilder sb;
        private final SymbolPrinter symbolPrinter;

        private int indentation = 0;

        Printer(SymbolPrinter symbolPrinter) {
            this.sb = new StringBuilder();
            this.symbolPrinter = symbolPrinter;
        }

        private void startLine(String start) {
            for (int i = 0; i < indentation; i++) {
                sb.append(' ');
            }
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
            if (plan instanceof RelationBoundary) {
                RelationBoundary boundary = (RelationBoundary) plan;
                startLine("Boundary[");
                addSymbolsList(boundary.outputs());
                sb.append("]\n");
                plan = boundary.source;
            }
            if (plan instanceof MultiPhase) {
                MultiPhase multiPhase = (MultiPhase) plan;
                startLine("MultiPhase[\n");
                indentation += 4;
                startLine("subQueries[\n");
                indentation += 4;
                for (Map.Entry<LogicalPlan, SelectSymbol> entry : multiPhase.dependencies.entrySet()) {
                    printPlan(entry.getKey());
                }
                indentation -= 4;
                startLine("]\n");
                printPlan(multiPhase.source);
                indentation -= 4;
                startLine("]\n");
                return sb.toString();
            }
            if (plan instanceof FetchOrEval) {
                FetchOrEval fetchOrEval = (FetchOrEval) plan;
                startLine("FetchOrEval[");
                addSymbolsList(fetchOrEval.outputs);
                sb.append("]\n");
                plan = fetchOrEval.source;
            }
            if (plan instanceof Limit) {
                Limit limit = (Limit) plan;
                startLine("Limit[");
                sb.append(symbolPrinter.printUnqualified(limit.limit));
                sb.append(';');
                sb.append(symbolPrinter.printUnqualified(limit.offset));
                sb.append("]\n");
                plan = limit.source;
            }
            if (plan instanceof Order) {
                Order order = (Order) plan;
                startLine("OrderBy[");
                OrderBy.explainRepresentation(
                    sb,
                    order.orderBy.orderBySymbols()
                        .stream().map(s -> Literal.of(symbolPrinter.printUnqualified(s))).collect(Collectors.toList()),
                    order.orderBy.reverseFlags(),
                    order.orderBy.nullsFirst());
                sb.append("]\n");
                plan = order.source;
            }
            if (plan instanceof Filter) {
                Filter filter = (Filter) plan;
                startLine("Filter[");
                sb.append(printQueryClause(symbolPrinter, filter.queryClause));
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
                addSymbolsList(collect.outputs);
                sb.append("] | ");
                sb.append(printQueryClause(symbolPrinter, collect.where));
                sb.append("]\n");
                return sb.toString();
            }
            if (plan instanceof Count) {
                Count count = (Count) plan;
                startLine("Count[");
                sb.append(count.tableRelation.tableInfo().ident());
                sb.append(" | ");
                sb.append(printQueryClause(symbolPrinter, count.where));
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
            return printPlan(plan);
        }

        private void addSymbolsList(Iterable<? extends Symbol> symbols) {
            StringJoiner commaJoiner = new StringJoiner(", ");
            for (Symbol symbol : symbols) {
                commaJoiner.add(symbolPrinter.printUnqualified(symbol));
            }
            sb.append(commaJoiner.toString());
        }
    }

    private static String printQueryClause(SymbolPrinter printer, QueryClause queryClause) {
        if (queryClause.hasQuery()) {
            return printer.printUnqualified(queryClause.query());
        } else if (queryClause.noMatch()) {
            return "None";
        } else {
            return "All";
        }
    }
}
