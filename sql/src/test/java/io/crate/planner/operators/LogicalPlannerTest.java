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

import io.crate.analyze.QueryClause;
import io.crate.analyze.SelectAnalyzedStatement;
import io.crate.analyze.relations.QueriedRelation;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.format.SymbolPrinter;
import io.crate.planner.consumer.FetchMode;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.junit.Before;
import org.junit.Test;

import java.util.StringJoiner;

import static org.hamcrest.Matchers.equalTo;

public class LogicalPlannerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor sqlExecutor;

    @Before
    public void prepare() {
        sqlExecutor = SQLExecutor.builder(clusterService)
            .enableDefaultTables()
            .build();
    }

    private LogicalPlan plan(String statement) {
        SelectAnalyzedStatement analyzedStatement = sqlExecutor.analyze(statement);
        QueriedRelation relation = analyzedStatement.relation();
        return LogicalPlanner.plan(relation, FetchMode.WITH_PROPAGATION)
            .build(LogicalPlanner.extractColumns(relation.querySpec().outputs()))
            .tryCollapse();
    }

    @Test
    public void testAggregationOnTableFunction() throws Exception {
        LogicalPlan plan = plan("select max(col1) from unnest([1, 2, 3])");
        assertThat(plan, isPlan("Aggregate[max(col1)] -> \n" +
                                "Collect[.unnest | [col1] | All]\n"));
    }

    @Test
    public void testQTFWithOrderBy() throws Exception {
        LogicalPlan plan = plan("select a, x from t1 order by a");
        assertThat(plan, isPlan("FetchOrEval[a, x] -> \n" +
                                "OrderBy[a] -> \n" +
                                "Collect[doc.t1 | [_fetchid, a] | All]\n"));
    }

    @Test
    public void testQTFWithoutOrderBy() throws Exception {
        LogicalPlan plan = plan("select a, x from t1");
        assertThat(plan, isPlan("FetchOrEval[a, x] -> \n" +
                                "Collect[doc.t1 | [_fetchid] | All]\n"));
    }

    @Test
    public void testSimpleSelectQAFAndLimit() throws Exception {
        LogicalPlan plan = plan("select a from t1 order by a limit 10 offset 5");
        assertThat(plan, isPlan("Limit[10;5] -> \n" +
                                "OrderBy[a] -> \n" +
                                "Collect[doc.t1 | [a] | All]\n"));
    }

    @Test
    public void testHavingGlobalAggregation() throws Exception {
        LogicalPlan plan = plan("select min(a), min(x) from t1 having min(x) < 33 and max(x) > 100");
        assertThat(plan, isPlan("FetchOrEval[min(a), min(x)] -> \n" +
                                "Filter[((min(x) < 33) AND (max(x) > 100))] -> \n" +
                                "Aggregate[min(a), min(x), max(x)] -> \n" +
                                "Collect[doc.t1 | [a, x] | All]\n"));
    }

    @Test
    public void testSelectCountStarIsOptimized() throws Exception {
        LogicalPlan plan = plan("select count(*) from t1 where x > 10");
        assertThat(plan, isPlan("Count[doc.t1 | (x > 10)]\n"));
    }

    private Matcher<LogicalPlan> isPlan(String expectedPlan) {
        return new FeatureMatcher<LogicalPlan, String>(equalTo(expectedPlan), "same output", "output ") {


            @Override
            protected String featureValueOf(LogicalPlan actual) {
                StringBuilder sb = new StringBuilder();
                SymbolPrinter printer = new SymbolPrinter(sqlExecutor.functions());
                return printedPlan(printer, actual, sb);
            }
        };
    }

    private static String printedPlan(SymbolPrinter printer, LogicalPlan plan, StringBuilder sb) {
        if (plan instanceof FetchOrEval) {
            FetchOrEval fetchOrEval = (FetchOrEval) plan;
            sb.append("FetchOrEval[");
            addSymbolsList(sb, printer, fetchOrEval.outputs);
            sb.append("] -> \n");
            plan = fetchOrEval.source;
        }
        if (plan instanceof Limit) {
            Limit limit = (Limit) plan;
            sb.append("Limit[");
            sb.append(printer.printSimple(limit.limit));
            sb.append(';');
            sb.append(printer.printSimple(limit.offset));
            sb.append("] -> \n");
            plan = limit.source;
        }
        if (plan instanceof Order) {
            Order order = (Order) plan;
            sb.append("OrderBy[");
            addSymbolsList(sb, printer, order.orderBy.orderBySymbols());
            sb.append("] -> \n");
            plan = order.source;
        }
        if (plan instanceof Filter) {
            Filter filter = (Filter) plan;
            sb.append("Filter[");
            sb.append(printQueryClause(printer, filter.queryClause));
            sb.append("] -> \n");
            plan = filter.source;
        }
        if (plan instanceof HashAggregate) {
            HashAggregate aggregate = (HashAggregate) plan;
            sb.append("Aggregate[");
            addSymbolsList(sb, printer, aggregate.aggregates);
            sb.append("] -> \n");
            plan = aggregate.source;
        }
        if (plan instanceof Collect) {
            Collect collect = (Collect) plan;
            sb.append("Collect[");
            sb.append(collect.tableInfo.ident());
            sb.append(" | [");
            addSymbolsList(sb, printer, collect.toCollect);
            sb.append("] | ");
            sb.append(printQueryClause(printer, collect.where));
            sb.append("]\n");
        }
        if (plan instanceof Count) {
            Count count = (Count) plan;
            sb.append("Count[");
            sb.append(count.tableInfo.ident());
            sb.append(" | ");
            sb.append(printQueryClause(printer, count.where));
            sb.append("]\n");
        }
        return sb.toString();
    }

    private static String printQueryClause(SymbolPrinter printer, QueryClause queryClause) {
        if (queryClause.hasQuery()) {
            return printer.printSimple(queryClause.query());
        } else if (queryClause.noMatch()) {
            return "None";
        } else {
            return "All";
        }
    }

    private static void addSymbolsList(StringBuilder sb, SymbolPrinter printer, Iterable<? extends Symbol> symbols) {
        StringJoiner commaJoiner = new StringJoiner(", ");
        for (Symbol symbol : symbols) {
            commaJoiner.add(printer.printSimple(symbol));
        }
        sb.append(commaJoiner.toString());
    }
}
