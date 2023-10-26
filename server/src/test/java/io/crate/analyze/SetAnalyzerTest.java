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

import static io.crate.testing.Asserts.assertThat;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import io.crate.expression.symbol.Literal;
import io.crate.sql.tree.Assignment;
import io.crate.sql.tree.SetStatement;
import io.crate.sql.tree.SetTransactionStatement;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class SetAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor executor;

    @Before
    public void prepare() {
        executor = SQLExecutor.builder(clusterService).build();
    }

    private <T extends AnalyzedStatement> T analyze(String stmt) {
        return executor.analyze(stmt);
    }

    @Test
    public void testSetGlobal() throws Exception {
        AnalyzedSetStatement analysis = analyze("SET GLOBAL PERSISTENT stats.operations_log_size=1");
        assertThat(analysis.isPersistent()).isEqualTo(true);
        assertThat(analysis.scope()).isEqualTo(SetStatement.Scope.GLOBAL);
        assertThat(analysis.settings().get(0)).isEqualTo(
            new Assignment<>(Literal.of("stats.operations_log_size"), List.of(Literal.of(1))));

        analysis = analyze("SET GLOBAL TRANSIENT stats.jobs_log_size=2");
        assertThat(analysis.isPersistent()).isEqualTo(false);
        assertThat(analysis.scope()).isEqualTo(SetStatement.Scope.GLOBAL);
        assertThat(analysis.settings().get(0)).isEqualTo(
            new Assignment<>(Literal.of("stats.jobs_log_size"), List.of(Literal.of(2))));


        analysis = analyze("SET GLOBAL TRANSIENT stats.enabled=false, stats.operations_log_size=0, stats.jobs_log_size=0");
        assertThat(analysis.scope()).isEqualTo(SetStatement.Scope.GLOBAL);
        assertThat(analysis.isPersistent()).isEqualTo(false);

        assertThat(analysis.settings().get(0)).isEqualTo(
            new Assignment<>(Literal.of("stats.enabled"), List.of(Literal.of(false))));

        assertThat(analysis.settings().get(1)).isEqualTo(
            new Assignment<>(Literal.of("stats.operations_log_size"), List.of(Literal.of(0))));

        assertThat(analysis.settings().get(2)).isEqualTo(
            new Assignment<>(Literal.of("stats.jobs_log_size"), List.of(Literal.of(0))));
    }

    @Test
    public void testSetLocal() throws Exception {
        AnalyzedSetStatement analysis = analyze("SET LOCAL something TO 2");
        assertThat(analysis.scope()).isEqualTo(SetStatement.Scope.LOCAL);

        assertThat(analysis.settings().get(0)).isEqualTo(
            new Assignment<>(Literal.of("something"), List.of(Literal.of(2))));


        analysis = analyze("SET LOCAL something TO DEFAULT");
        assertThat(analysis.scope()).isEqualTo(SetStatement.Scope.LOCAL);

        assertThat(analysis.settings().get(0)).isEqualTo(
            new Assignment<>(Literal.of("something"), List.of()));
    }

    @Test
    public void testSetSessionTransactionMode() throws Exception {
        AnalyzedSetTransaction analysis = analyze(
            "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ UNCOMMITTED");
        assertThat(analysis.transactionModes()).containsExactly(
            SetTransactionStatement.IsolationLevel.READ_UNCOMMITTED);
    }

    @Test
    public void testSetSession() throws Exception {
        AnalyzedSetStatement analysis = analyze("SET SESSION something TO 2");
        assertThat(analysis.scope()).isEqualTo(SetStatement.Scope.SESSION);

        assertThat(analysis.settings().get(0)).isEqualTo(
            new Assignment<>(Literal.of("something"), List.of(Literal.of(2))));

        analysis = analyze("SET SESSION something = 1,2,3");
        assertThat(analysis.scope()).isEqualTo(SetStatement.Scope.SESSION);

        assertThat(analysis.settings().get(0)).isEqualTo(
            new Assignment<>(Literal.of("something"), List.of(Literal.of(1), Literal.of(2), Literal.of(3))));
    }

    @Test
    public void testSet() throws Exception {
        AnalyzedSetStatement analysis = analyze("SET something TO 2");
        assertThat(analysis.scope()).isEqualTo(SetStatement.Scope.SESSION);

        assertThat(analysis.settings().get(0)).isEqualTo(
            new Assignment<>(Literal.of("something"), List.of(Literal.of(2))));

        analysis = analyze("SET something = DEFAULT");
        assertThat(analysis.scope()).isEqualTo(SetStatement.Scope.SESSION);

        assertThat(analysis.settings().get(0)).isEqualTo(
            new Assignment<>(Literal.of("something"), List.of()));

        analysis = analyze("SET something = default");
        assertThat(analysis.scope()).isEqualTo(SetStatement.Scope.SESSION);

        assertThat(analysis.settings().get(0)).isEqualTo(
            new Assignment<>(Literal.of("something"), List.of()));
    }

    @Test
    public void testSetFullQualified() throws Exception {
        AnalyzedSetStatement analysis = analyze("SET GLOBAL PERSISTENT stats['operations_log_size']=1");
        assertThat(analysis.isPersistent()).isEqualTo(true);

        assertThat(analysis.settings().get(0)).isEqualTo(
            new Assignment<>(Literal.of("stats.operations_log_size"), List.of(Literal.of(1))));
    }

    @Test
    public void testObjectValue() throws Exception {
        AnalyzedSetStatement analysis = analyze("SET GLOBAL PERSISTENT cluster.graceful_stop = {timeout='1h'}");
        HashMap<String, Object> expected = new HashMap<>();
        expected.put("timeout", "1h");

        assertThat(analysis.settings().get(0)).isEqualTo(
            new Assignment<>(Literal.of("cluster.graceful_stop"), List.of(Literal.of(expected))));
    }

    @Test
    public void testSetRuntimeSettingSubscript() {
        AnalyzedSetStatement analysis =
            analyze("SET GLOBAL TRANSIENT cluster['routing']['allocation']['include'] = {_host = 'host1.example.com'}");

        assertThat(analysis.settings().get(0)).isEqualTo(
            new Assignment<>(Literal.of("cluster.routing.allocation.include"),
                             List.of(Literal.of(Map.of("_host", "host1.example.com")))));
    }

    @Test
    public void testSetLoggingSetting() {
        AnalyzedSetStatement analysis = analyze("SET GLOBAL TRANSIENT \"logger.action\" = 'INFo'");
        assertThat(analysis.isPersistent()).isEqualTo(false);

        assertThat(analysis.settings().get(0)).isEqualTo(
            new Assignment<>(Literal.of("logger.action"), List.of(Literal.of("INFo"))));
    }
}
