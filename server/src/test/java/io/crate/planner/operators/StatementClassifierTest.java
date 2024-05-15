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

import static io.crate.analyze.TableDefinitions.USER_TABLE_DEFINITION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertThat;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

import io.crate.planner.Plan;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class StatementClassifierTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;

    @Before
    public void prepare() throws IOException {
        e = SQLExecutor.of(clusterService)
            .addTable(USER_TABLE_DEFINITION);
    }

    @Test
    public void test_classify_qtf_statement_contains_fetch_limit_and_collect() throws Exception {
        LogicalPlan plan = e.logicalPlan("SELECT * FROM users LIMIT 10");
        StatementClassifier.Classification classification = StatementClassifier.classify(plan);
        assertThat(classification.type()).isEqualTo(Plan.StatementType.SELECT);
        assertThat(classification.labels(), contains("Collect", "Fetch", "Limit"));
    }

    @Test
    public void testClassifySelectStatements() {
        LogicalPlan plan = e.logicalPlan("SELECT 1");
        StatementClassifier.Classification classification = StatementClassifier.classify(plan);
        assertThat(classification.type()).isEqualTo(Plan.StatementType.SELECT);
        assertThat(classification.labels(), contains("Eval", "TableFunction"));

        plan = e.logicalPlan("SELECT * FROM users WHERE id = 1");
        classification = StatementClassifier.classify(plan);
        assertThat(classification.type()).isEqualTo(Plan.StatementType.SELECT);
        assertThat(classification.labels(), contains("Get"));

        plan = e.logicalPlan("SELECT * FROM users ORDER BY id");
        classification = StatementClassifier.classify(plan);
        assertThat(classification.type()).isEqualTo(Plan.StatementType.SELECT);
        assertThat(classification.labels(), contains("Collect", "Fetch", "Order"));

        plan = e.logicalPlan("SELECT a.id, b.id FROM users a, users b WHERE a.id = b.id");
        classification = StatementClassifier.classify(plan);
        assertThat(classification.type()).isEqualTo(Plan.StatementType.SELECT);
        assertThat(classification.labels(), contains("Collect", "HashJoin"));

        plan = e.logicalPlan("SELECT a.id, b.id FROM users a, users b WHERE a.id > b.id");
        classification = StatementClassifier.classify(plan);
        assertThat(classification.type()).isEqualTo(Plan.StatementType.SELECT);
        assertThat(classification.labels(), contains("Collect", "NestedLoopJoin"));

        plan = e.logicalPlan("SELECT id FROM users UNION ALL SELECT id FROM users");
        classification = StatementClassifier.classify(plan);
        assertThat(classification.type()).isEqualTo(Plan.StatementType.SELECT);
        assertThat(classification.labels(), contains("Collect", "Union"));

        plan = e.logicalPlan("SELECT count(*) FROM users");
        classification = StatementClassifier.classify(plan);
        assertThat(classification.type()).isEqualTo(Plan.StatementType.SELECT);
        assertThat(classification.labels(), contains("Count"));

        plan = e.logicalPlan("SELECT count(*), name FROM users GROUP BY 2");
        classification = StatementClassifier.classify(plan);
        assertThat(classification.type()).isEqualTo(Plan.StatementType.SELECT);
        assertThat(classification.labels(), contains("Collect", "Eval", "GroupHashAggregate"));

        plan = e.logicalPlan("SELECT * FROM users WHERE id = (SELECT 1) OR name = (SELECT 'Arthur')");
        classification = StatementClassifier.classify(plan);
        assertThat(classification.type()).isEqualTo(Plan.StatementType.SELECT);
        assertThat(classification.labels(), contains("Collect", "Eval", "Limit", "MultiPhase", "TableFunction"));
    }


    @Test
    public void test_select_with_window_function_and_table_function_contains_project_set_and_window_agg_labels() {
        var plan = e.logicalPlan(
            "SELECT" +
            "  x," +
            "  avg(x) OVER (ORDER BY y) " +
            "FROM " +
            "  (SELECT x, generate_series(0, 3) as y FROM unnest([1, 2]) as t (x)) as tt " +
            "WHERE y >= 2");
        var classification = StatementClassifier.classify(plan);
        assertThat(classification.type()).isEqualTo(Plan.StatementType.SELECT);
        assertThat(classification.labels(), contains("Eval", "Filter", "ProjectSet", "TableFunction", "WindowAgg"));
    }

    @Test
    public void testClassifyInsertStatements() {
        Plan plan = e.logicalPlan("INSERT INTO users (id, name) VALUES (1, 'foo')");
        StatementClassifier.Classification classification = StatementClassifier.classify(plan);
        assertThat(classification.type()).isEqualTo(Plan.StatementType.INSERT);
        assertThat(classification.labels(), contains("InsertFromValues"));

        plan = e.logicalPlan("INSERT INTO users (id, name) (SELECT id, name FROM users)");
        classification = StatementClassifier.classify(plan);
        assertThat(classification.type()).isEqualTo(Plan.StatementType.INSERT);
        assertThat(classification.labels(), contains("Collect"));

        plan = e.logicalPlan("INSERT INTO users (id, name) (SELECT * FROM unnest([1], ['foo']))");
        classification = StatementClassifier.classify(plan);
        assertThat(classification.type()).isEqualTo(Plan.StatementType.INSERT);
        assertThat(classification.labels(), contains("TableFunction"));
    }

    @Test
    public void test_classify_multiphase_delete_statement() {
        Plan plan = e.plan("DELETE FROM users WHERE id in (SELECT id from users)");
        StatementClassifier.Classification classification = StatementClassifier.classify(plan);
        assertThat(classification.type()).isEqualTo(Plan.StatementType.DELETE);
        assertThat(classification.labels()).isEmpty();
    }

    @Test
    public void test_classify_multiphase_update_statement() {
        Plan plan = e.plan("UPDATE users set name = 'a' WHERE id in (SELECT id from users)");
        StatementClassifier.Classification classification = StatementClassifier.classify(plan);
        assertThat(classification.type()).isEqualTo(Plan.StatementType.UPDATE);
        assertThat(classification.labels()).isEmpty();
    }
}
