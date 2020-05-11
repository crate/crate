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

package io.crate.analyze;

import io.crate.expression.symbol.Literal;
import io.crate.sql.tree.Assignment;
import io.crate.sql.tree.SetStatement;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.crate.testing.SymbolMatchers.isLiteral;
import static org.hamcrest.Matchers.is;

public class SetAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor executor;

    @Before
    public void prepare() {
        executor = SQLExecutor.builder(clusterService).build();
    }

    private <T> T analyze(String stmt) {
        return executor.analyze(stmt);
    }

    @Test
    public void testSetGlobal() throws Exception {
        AnalyzedSetStatement analysis = analyze("SET GLOBAL PERSISTENT stats.operations_log_size=1");
        assertThat(analysis.isPersistent(), is(true));
        assertThat(analysis.scope(), is(SetStatement.Scope.GLOBAL));
        assertThat(
            analysis.settings().get(0),
            is(new Assignment<>(Literal.of("stats.operations_log_size"), List.of(Literal.of(1L)))));

        analysis = analyze("SET GLOBAL TRANSIENT stats.jobs_log_size=2");
        assertThat(analysis.isPersistent(), is(false));
        assertThat(analysis.scope(), is(SetStatement.Scope.GLOBAL));
        assertThat(
            analysis.settings().get(0),
            is(new Assignment<>(Literal.of("stats.jobs_log_size"), List.of(Literal.of(2L)))));


        analysis = analyze("SET GLOBAL TRANSIENT stats.enabled=false, stats.operations_log_size=0, stats.jobs_log_size=0");
        assertThat(analysis.scope(), is(SetStatement.Scope.GLOBAL));
        assertThat(analysis.isPersistent(), is(false));

        assertThat(
            analysis.settings().get(0),
            is(new Assignment<>(Literal.of("stats.enabled"), List.of(Literal.of(false)))));

        assertThat(
            analysis.settings().get(1),
            is(new Assignment<>(Literal.of("stats.operations_log_size"), List.of(Literal.of(0L)))));

        assertThat(
            analysis.settings().get(2),
            is(new Assignment<>(Literal.of("stats.jobs_log_size"), List.of(Literal.of(0L)))));
    }

    @Test
    public void testSetLocal() throws Exception {
        AnalyzedSetStatement analysis = analyze("SET LOCAL something TO 2");
        assertThat(analysis.scope(), is(SetStatement.Scope.LOCAL));

        assertThat(
            analysis.settings().get(0),
            is(new Assignment<>(Literal.of("something"), List.of(Literal.of(2L)))));


        analysis = analyze("SET LOCAL something TO DEFAULT");
        assertThat(analysis.scope(), is(SetStatement.Scope.LOCAL));

        assertThat(
            analysis.settings().get(0),
            is(new Assignment<>(Literal.of("something"), List.of())));
    }

    @Test
    public void testSetSessionTransactionMode() throws Exception {
        AnalyzedSetStatement analysis = analyze("SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ UNCOMMITTED");
        assertThat(analysis.scope(), is(SetStatement.Scope.SESSION_TRANSACTION_MODE));

        assertThat(
            analysis.settings().get(0).columnName(), is(Literal.of("transaction_mode")));

        assertThat(
            analysis.settings().get(0).expressions().size(), is(4));
    }

    @Test
    public void testSetSession() throws Exception {
        AnalyzedSetStatement analysis = analyze("SET SESSION something TO 2");
        assertThat(analysis.scope(), is(SetStatement.Scope.SESSION));

        assertThat(
            analysis.settings().get(0),
            is(new Assignment<>(Literal.of("something"), List.of(Literal.of(2L)))));

        analysis = analyze("SET SESSION something = 1,2,3");
        assertThat(analysis.scope(), is(SetStatement.Scope.SESSION));

        assertThat(
            analysis.settings().get(0),
            is(new Assignment<>(Literal.of("something"), List.of(Literal.of(1L), Literal.of(2L), Literal.of(3L)))));
    }

    @Test
    public void testSet() throws Exception {
        AnalyzedSetStatement analysis = analyze("SET something TO 2");
        assertThat(analysis.scope(), is(SetStatement.Scope.SESSION));

        assertThat(
            analysis.settings().get(0),
            is(new Assignment<>(Literal.of("something"), List.of(Literal.of(2L)))));

        analysis = analyze("SET something = DEFAULT");
        assertThat(analysis.scope(), is(SetStatement.Scope.SESSION));

        assertThat(
            analysis.settings().get(0),
            is(new Assignment<>(Literal.of("something"), List.of())));

        analysis = analyze("SET something = default");
        assertThat(analysis.scope(), is(SetStatement.Scope.SESSION));

        assertThat(
            analysis.settings().get(0),
            is(new Assignment<>(Literal.of("something"), List.of())));
    }

    @Test
    public void testSetFullQualified() throws Exception {
        AnalyzedSetStatement analysis = analyze("SET GLOBAL PERSISTENT stats['operations_log_size']=1");
        assertThat(analysis.isPersistent(), is(true));

        assertThat(
            analysis.settings().get(0),
            is(new Assignment<>(Literal.of("stats.operations_log_size"), List.of(Literal.of(1L)))));
    }
//
    @Test
    public void testObjectValue() throws Exception {
        AnalyzedSetStatement analysis = analyze("SET GLOBAL PERSISTENT cluster.graceful_stop = {timeout='1h'}");
        HashMap<String, Object> expected = new HashMap<>();
        expected.put("timeout", "1h");

        assertThat(
            analysis.settings().get(0),
            is(new Assignment<>(Literal.of("cluster.graceful_stop"), List.of(Literal.of(expected)))));
    }

    @Test
    public void testSetRuntimeSettingSubscript() {
        AnalyzedSetStatement analysis =
            analyze("SET GLOBAL TRANSIENT cluster['routing']['allocation']['include'] = {_host = 'host1.example.com'}");

        assertThat(
            analysis.settings().get(0),
            is(new Assignment<>(Literal.of("cluster.routing.allocation.include"),
                                List.of(Literal.of(Map.of("_host", "host1.example.com"))))));
    }

    @Test
    public void testSetLoggingSetting() {
        AnalyzedSetStatement analysis = analyze("SET GLOBAL TRANSIENT \"logger.action\" = 'INFo'");
        assertThat(analysis.isPersistent(), is(false));

        assertThat(
            analysis.settings().get(0),
            is(new Assignment<>(Literal.of("logger.action"), List.of(Literal.of("INFo")))));
    }

    @Test
    public void testSetLicense() throws Exception {
        AnalyzedSetLicenseStatement analysis = analyze("SET LICENSE 'ThisShouldBeAnEncryptedLicenseKey'");
        assertThat(analysis.licenseKey(), isLiteral("ThisShouldBeAnEncryptedLicenseKey"));
    }
}
