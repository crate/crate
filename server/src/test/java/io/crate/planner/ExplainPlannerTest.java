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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nullable;

import org.junit.Before;
import org.junit.Test;

import io.crate.data.BatchIterator;
import io.crate.data.Row;
import io.crate.data.RowConsumer;
import io.crate.planner.node.management.ExplainPlan;
import io.crate.planner.operators.SubQueryResults;
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
        e = SQLExecutor.builder(clusterService).enableDefaultTables().build();
    }

    @Test
    public void testExplain() {
        for (String statement : EXPLAIN_TEST_STATEMENTS) {
            ExplainPlan plan = e.plan("EXPLAIN " + statement);
            assertThat(plan).isNotNull();
            assertThat(plan.subPlan()).isNotNull();
            assertThat(plan.doAnalyze()).isFalse();
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
}
