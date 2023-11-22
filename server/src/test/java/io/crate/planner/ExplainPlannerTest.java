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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import org.jetbrains.annotations.Nullable;
import org.junit.Before;
import org.junit.Test;

import io.crate.analyze.TableDefinitions;
import io.crate.data.BatchIterator;
import io.crate.data.Row;
import io.crate.data.RowConsumer;
import io.crate.data.testing.TestingRowConsumer;
import io.crate.metadata.RelationName;
import io.crate.planner.node.management.ExplainPlan;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.operators.SubQueryResults;
import io.crate.statistics.Stats;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class ExplainPlannerTest extends CrateDummyClusterServiceUnitTest {

    private static final List<String> EXPLAIN_TEST_STATEMENTS = List.of(
        "select 1 as connected",
        "select id from sys.cluster",
        "select id from users order by id",
        "select * from users",
        "select count(*) from users",
        "select name, count(distinct id) from users group by name",
        "select avg(id) from users",
        "select * from users where name = (select 'name')"
    );

    private SQLExecutor e;

    @Before
    public void prepare() throws IOException {
        e = SQLExecutor.builder(clusterService)
            .addTable(TableDefinitions.USER_TABLE_DEFINITION)
            .build();
    }

    @Test
    public void testExplain() {
        for (String statement : EXPLAIN_TEST_STATEMENTS) {
            ExplainPlan plan = e.plan("EXPLAIN " + statement);
            assertThat(plan).isNotNull();
            assertThat(plan.subPlan()).isNotNull();
            assertThat(plan.doAnalyze()).isFalse();
            assertThat(plan.showCosts()).isTrue();
        }
    }

    @Test
    public void testExplainCostsActivated() {
        for (String statement : EXPLAIN_TEST_STATEMENTS) {
            ExplainPlan plan = e.plan("EXPLAIN (COSTS true)" + statement);
            assertThat(plan).isNotNull();
            assertThat(plan.subPlan()).isNotNull();
            assertThat(plan.doAnalyze()).isFalse();
            assertThat(plan.showCosts()).isTrue();
        }
    }

    @Test
    public void testExplainCostsActivated1() {
        for (String statement : EXPLAIN_TEST_STATEMENTS) {
            ExplainPlan plan = e.plan("EXPLAIN (Analyze true, costs false)" + statement);
            assertThat(plan).isNotNull();
            assertThat(plan.subPlan()).isNotNull();
            assertThat(plan.doAnalyze()).isTrue();
            assertThat(plan.showCosts()).isFalse();
        }
    }

    @Test
    public void testExplainCostsDeactivated() {
        for (String statement : EXPLAIN_TEST_STATEMENTS) {
            ExplainPlan plan = e.plan("EXPLAIN (COSTS false)" + statement);
            assertThat(plan).isNotNull();
            assertThat(plan.subPlan()).isNotNull();
            assertThat(plan.doAnalyze()).isFalse();
            assertThat(plan.showCosts()).isFalse();
        }
    }

    @Test
    public void testExplainAnalyze() {
        for (String statement : EXPLAIN_TEST_STATEMENTS) {
            ExplainPlan plan = e.plan("EXPLAIN ANALYZE " + statement);
            assertThat(plan).isNotNull();
            assertThat(plan.subPlan()).isNotNull();
            assertThat(plan.doAnalyze()).isTrue();
        }
    }

    @Test
    public void testExplainAnalyzeAsOptionActivated() {
        for (String statement : EXPLAIN_TEST_STATEMENTS) {
            ExplainPlan plan = e.plan("EXPLAIN (ANALYZE true) " + statement);
            assertThat(plan).isNotNull();
            assertThat(plan.subPlan()).isNotNull();
            assertThat(plan.doAnalyze()).isTrue();
        }
    }

    @Test
    public void testExplainAnalyzeAsOptionDeactivated() {
        for (String statement : EXPLAIN_TEST_STATEMENTS) {
            ExplainPlan plan = e.plan("EXPLAIN (ANALYZE false) " + statement);
            assertThat(plan).isNotNull();
            assertThat(plan.subPlan()).isNotNull();
            assertThat(plan.doAnalyze()).isFalse();
        }
    }

    @Test
    public void test_explain_verbose() {
        for (String statement : EXPLAIN_TEST_STATEMENTS) {
            ExplainPlan plan = e.plan("EXPLAIN VERBOSE " + statement);
            assertThat(plan).isNotNull();
            assertThat(plan.subPlan()).isNotNull();
            assertThat(plan.doAnalyze()).isFalse();
            assertThat(plan.showCosts()).isTrue();
            assertThat(plan.verbose()).isTrue();
        }
    }

    @Test
    public void test_explain_verbose_as_option_activated() {
        for (String statement : EXPLAIN_TEST_STATEMENTS) {
            ExplainPlan plan = e.plan("EXPLAIN (VERBOSE true) " + statement);
            assertThat(plan).isNotNull();
            assertThat(plan.subPlan()).isNotNull();
            assertThat(plan.doAnalyze()).isFalse();
            assertThat(plan.showCosts()).isTrue();
            assertThat(plan.verbose()).isTrue();
        }
    }

    @Test
    public void test_explain_verbose_as_option_deactivated() {
        for (String statement : EXPLAIN_TEST_STATEMENTS) {
            ExplainPlan plan = e.plan("EXPLAIN (VERBOSE false)" + statement);
            assertThat(plan).isNotNull();
            assertThat(plan.subPlan()).isNotNull();
            assertThat(plan.doAnalyze()).isFalse();
            assertThat(plan.showCosts()).isTrue();
            assertThat(plan.verbose()).isFalse();
        }
    }

    @Test
    public void testExplainAnalyzeMultiPhasePlanNotSupported() {
        ExplainPlan plan = e.plan("EXPLAIN ANALYZE SELECT * FROM users WHERE name = (SELECT 'crate') or id = (SELECT 1)");
        PlannerContext plannerContext = e.getPlannerContext(clusterService.state());
        CountDownLatch counter = new CountDownLatch(1);

        AtomicReference<BatchIterator<Row>> itRef = new AtomicReference<>();
        AtomicReference<Throwable> failureRef = new AtomicReference<>();

        plan.execute(null, plannerContext, new RowConsumer() {
            @Override
            public void accept(BatchIterator<Row> iterator, @Nullable Throwable failure) {
                itRef.set(iterator);
                failureRef.set(failure);
                counter.countDown();
            }

            @Override
            public CompletableFuture<?> completionFuture() {
                return null;
            }
        }, Row.EMPTY, SubQueryResults.EMPTY);

        assertThat(itRef.get()).isNull();
        assertThat(failureRef.get()).isNotNull();
        assertThat(failureRef.get().getMessage()).isEqualTo("EXPLAIN ANALYZE does not support profiling multi-phase plans, such as queries with scalar subselects.");
    }

    @Test
    public void test_explain_on_collect_uses_cast_optimizer_for_query_symbol() throws Exception {
        var e = SQLExecutor.builder(clusterService)
            .addTable("CREATE TABLE ts1 (ts TIMESTAMP NOT NULL)")
            .build();

        ExplainPlan plan = e.plan("EXPLAIN (COSTS FALSE) SELECT * FROM ts1 WHERE ts = ts");
        var printedPlan = ExplainPlan.printLogicalPlan((LogicalPlan) plan.subPlan(), e.getPlannerContext(clusterService.state()), plan.showCosts());
        assertThat(printedPlan).isEqualTo(
            "Collect[doc.ts1 | [ts] | true]"
        );
    }

    @Test
    public void test_explain_verbose_on_collect_uses_cast_optimizer_for_query_symbol() throws Exception {
        var e = SQLExecutor.builder(clusterService)
            .addTable("CREATE TABLE ts1 (ts TIMESTAMP NOT NULL)")
            .build();
        PlannerContext plannerContext = e.getPlannerContext(clusterService.state());

        ExplainPlan plan = e.plan("EXPLAIN (VERBOSE TRUE, COSTS FALSE) SELECT * FROM ts1 WHERE ts = ts");
        List<Object[]> rows = execute(plan, plannerContext);
        assertThat(rows).containsExactly(
            new Object[]{
                "Initial logical plan",
                """
                Filter[(ts = ts)]
                  └ Collect[doc.ts1 | [ts] | true]"""},
            new Object[]{
                "optimizer_merge_filter_and_collect",
                "Collect[doc.ts1 | [ts] | (ts = ts)]"},
            new Object[]{
                "Final logical plan",
                "Collect[doc.ts1 | [ts] | true]"}
        );
    }

    @Test
    public void test_explain_costs_adds_estimated_rows_to_output() throws Exception {
        var e = SQLExecutor.builder(clusterService)
            .addTable("CREATE TABLE doc.a (x int)")
            .addTable("CREATE TABLE doc.b (x int)")
            .build();

        e.updateTableStats(Map.of(
            new RelationName("doc", "a"), new Stats(100, 100, Map.of()),
            new RelationName("doc", "b"), new Stats(100, 100, Map.of())
        ));

        ExplainPlan plan = e.plan("EXPLAIN SELECT COUNT(a.x) FROM a join b on a.x = b.x");
        var printedPlan = ExplainPlan.printLogicalPlan((LogicalPlan) plan.subPlan(), e.getPlannerContext(clusterService.state()), plan.showCosts());
        assertThat(printedPlan).isEqualTo(
            "HashAggregate[count(x)] (rows=1)\n" +
            "  └ HashJoin[(x = x)] (rows=0)\n" +
            "    ├ Collect[doc.a | [x] | true] (rows=100)\n" +
            "    └ Collect[doc.b | [x] | true] (rows=100)"
        );
        plan = e.plan("EXPLAIN (COSTS TRUE) SELECT COUNT(a.x) FROM a join b on a.x = b.x");
        printedPlan = ExplainPlan.printLogicalPlan((LogicalPlan) plan.subPlan(), e.getPlannerContext(clusterService.state()), plan.showCosts());
        assertThat(printedPlan).isEqualTo(
            "HashAggregate[count(x)] (rows=1)\n" +
            "  └ HashJoin[(x = x)] (rows=0)\n" +
            "    ├ Collect[doc.a | [x] | true] (rows=100)\n" +
            "    └ Collect[doc.b | [x] | true] (rows=100)"
        );
        plan = e.plan("EXPLAIN (COSTS FALSE) SELECT COUNT(a.x) FROM a join b on a.x = b.x");
        printedPlan = ExplainPlan.printLogicalPlan((LogicalPlan) plan.subPlan(), e.getPlannerContext(clusterService.state()), plan.showCosts());
        assertThat(printedPlan).isEqualTo(
            "HashAggregate[count(x)]\n" +
            "  └ HashJoin[(x = x)]\n" +
            "    ├ Collect[doc.a | [x] | true]\n" +
            "    └ Collect[doc.b | [x] | true]"
        );
    }

    @Test
    public void test_explain_verbose_costs_adds_estimated_rows_to_output() throws Exception {
        var e = SQLExecutor.builder(clusterService)
            .addTable("CREATE TABLE doc.a (x int)")
            .addTable("CREATE TABLE doc.b (x int)")
            .build();
        PlannerContext plannerContext = e.getPlannerContext(clusterService.state());

        e.updateTableStats(Map.of(
            new RelationName("doc", "a"), new Stats(100, 100, Map.of()),
            new RelationName("doc", "b"), new Stats(100, 100, Map.of())
        ));

        ExplainPlan plan = e.plan("EXPLAIN VERBOSE SELECT COUNT(a.x) FROM a join b on a.x = b.x");
        List<Object[]> rows = execute(plan, plannerContext);
        assertThat(rows).containsExactly(
            new Object[]{
                "Initial logical plan",
                """
                HashAggregate[count(x)] (rows=1)
                  └ Join[INNER | (x = x)] (rows=0)
                    ├ Collect[doc.a | [x] | true] (rows=100)
                    └ Collect[doc.b | [x] | true] (rows=100)"""},
            new Object[]{
                "optimizer_rewrite_join_plan",
                """
                HashAggregate[count(x)] (rows=1)
                  └ HashJoin[(x = x)] (rows=0)
                    ├ Collect[doc.a | [x] | true] (rows=100)
                    └ Collect[doc.b | [x] | true] (rows=100)"""},
            new Object[]{
                "Final logical plan",
                """
                HashAggregate[count(x)] (rows=1)
                  └ HashJoin[(x = x)] (rows=0)
                    ├ Collect[doc.a | [x] | true] (rows=100)
                    └ Collect[doc.b | [x] | true] (rows=100)"""}
        );

        plan = e.plan("EXPLAIN (VERBOSE TRUE, COSTS TRUE) SELECT COUNT(a.x) FROM a join b on a.x = b.x");
        rows = execute(plan, plannerContext);
        assertThat(rows).containsExactly(
            new Object[]{
                "Initial logical plan",
                """
                HashAggregate[count(x)] (rows=1)
                  └ Join[INNER | (x = x)] (rows=0)
                    ├ Collect[doc.a | [x] | true] (rows=100)
                    └ Collect[doc.b | [x] | true] (rows=100)"""},
            new Object[]{
                "optimizer_rewrite_join_plan",
                """
                HashAggregate[count(x)] (rows=1)
                  └ HashJoin[(x = x)] (rows=0)
                    ├ Collect[doc.a | [x] | true] (rows=100)
                    └ Collect[doc.b | [x] | true] (rows=100)"""},
            new Object[]{
                "Final logical plan",
                """
                HashAggregate[count(x)] (rows=1)
                  └ HashJoin[(x = x)] (rows=0)
                    ├ Collect[doc.a | [x] | true] (rows=100)
                    └ Collect[doc.b | [x] | true] (rows=100)"""}
        );

        plan = e.plan("EXPLAIN (VERBOSE TRUE, COSTS FALSE) SELECT COUNT(a.x) FROM a join b on a.x = b.x");
        rows = execute(plan, plannerContext);
        assertThat(rows).containsExactly(
            new Object[]{
                "Initial logical plan",
                """
                HashAggregate[count(x)]
                  └ Join[INNER | (x = x)]
                    ├ Collect[doc.a | [x] | true]
                    └ Collect[doc.b | [x] | true]"""},
            new Object[]{
                "optimizer_rewrite_join_plan",
                """
                HashAggregate[count(x)]
                  └ HashJoin[(x = x)]
                    ├ Collect[doc.a | [x] | true]
                    └ Collect[doc.b | [x] | true]"""},
            new Object[]{
                "Final logical plan",
                """
                HashAggregate[count(x)]
                  └ HashJoin[(x = x)]
                    ├ Collect[doc.a | [x] | true]
                    └ Collect[doc.b | [x] | true]"""}
        );
    }

    private static List<Object[]> execute(ExplainPlan plan, PlannerContext plannerContext) throws Exception {
        AtomicReference<BatchIterator<Row>> itRef = new AtomicReference<>();

        plan.execute(null, plannerContext, new RowConsumer() {
            @Override
            public void accept(BatchIterator<Row> iterator, @Nullable Throwable failure) {
                itRef.set(iterator);
            }

            @Override
            public CompletableFuture<?> completionFuture() {
                return null;
            }
        }, Row.EMPTY, SubQueryResults.EMPTY);

        BatchIterator<Row> batchIterator = itRef.get();
        TestingRowConsumer testingBatchConsumer = new TestingRowConsumer();
        testingBatchConsumer.accept(batchIterator, null);
        return testingBatchConsumer.getResult();
    }
}
