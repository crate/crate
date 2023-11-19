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

package io.crate.analyze;

import static io.crate.testing.Asserts.isField;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class ExplainAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;

    @Before
    public void prepare() throws IOException {
        e = SQLExecutor.builder(clusterService)
            .addTable(TableDefinitions.USER_TABLE_DEFINITION)
            .build();
    }

    @Test
    public void testExplain() {
        ExplainAnalyzedStatement stmt = e.analyze("explain select id from sys.cluster");
        assertThat(stmt.statement()).isNotNull();
        assertThat(stmt.statement()).isExactlyInstanceOf(QueriedSelectRelation.class);
        assertThat(stmt.context()).isNull();
    }

    @Test
    public void test_cost_and_analyze_raises_errors() {
        assertThatThrownBy(() -> e.analyze("explain (costs, analyze) select id from sys.cluster"))
            .hasMessage("The ANALYZE and COSTS options are not allowed together");
    }

    @Test
    public void test_analyze_and_verbose_raises_errors() {
        assertThatThrownBy(() -> e.analyze("explain (analyze, verbose) select id from sys.cluster"))
            .hasMessage("The ANALYZE and VERBOSE options are not allowed together");
    }

    @Test
    public void testAnalyzePropertyIsSetOnExplainAnalyze() {
        ExplainAnalyzedStatement stmt = e.analyze("explain analyze select id from sys.cluster");
        assertThat(stmt.context()).isNotNull();
    }

    @Test
    public void testAnalyzePropertyIsReflectedInColumnName() {
        ExplainAnalyzedStatement stmt = e.analyze("explain analyze select 1");
        assertThat(stmt.outputs()).satisfiesExactly(isField("QUERY PLAN"));
    }

    @Test
    public void testExplainArrayComparison() {
        ExplainAnalyzedStatement stmt = e.analyze("explain SELECT id from sys.cluster where id = any([1,2,3])");
        assertThat(stmt.statement()).isNotNull();
        assertThat(stmt.statement()).isExactlyInstanceOf(QueriedSelectRelation.class);
        assertThat(stmt.outputs()).satisfiesExactly(isField("QUERY PLAN"));
    }

    @Test
    public void testExplainCopyFrom() {
        ExplainAnalyzedStatement stmt = e.analyze("explain copy users from '/tmp/*' WITH (shared=True)");
        assertThat(stmt.statement()).isExactlyInstanceOf(AnalyzedCopyFrom.class);
        assertThat(stmt.outputs()).satisfiesExactly(isField("QUERY PLAN"));
    }

    @Test
    public void testExplainRefreshUnsupported() {
        assertThatThrownBy(() -> e.analyze("explain refresh table parted"))
            .isExactlyInstanceOf(UnsupportedFeatureException.class)
            .hasMessageStartingWith("EXPLAIN is not supported for RefreshStatement");

    }

    @Test
    public void testExplainOptimizeUnsupported() {
        assertThatThrownBy(() -> e.analyze("explain optimize table parted"))
            .isExactlyInstanceOf(UnsupportedFeatureException.class)
            .hasMessageStartingWith("EXPLAIN is not supported for OptimizeStatement");
    }

    @Test
    public void test_explain_verbose() {
        ExplainAnalyzedStatement stmt = e.analyze("explain verbose select id from sys.cluster");
        assertThat(stmt.statement()).isExactlyInstanceOf(QueriedSelectRelation.class);
        assertThat(stmt.outputs()).satisfiesExactly(isField("STEP"), isField("QUERY PLAN"));
    }

    @Test
    public void test_explain_verbose_copy_from_unsupported() {
        assertThatThrownBy(() -> e.analyze("explain verbose copy users from '/tmp/*' WITH (shared=True)"))
            .isExactlyInstanceOf(UnsupportedFeatureException.class)
            .hasMessageStartingWith("EXPLAIN VERBOSE is not supported for CopyFrom");
    }
}
