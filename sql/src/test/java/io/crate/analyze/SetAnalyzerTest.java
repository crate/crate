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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.Literal;
import io.crate.sql.tree.ObjectLiteral;
import io.crate.sql.tree.SetStatement;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import java.util.Set;

import static org.hamcrest.Matchers.*;

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
        SetAnalyzedStatement analysis = analyze("SET GLOBAL PERSISTENT stats.operations_log_size=1");
        assertThat(analysis.isPersistent(), is(true));
        assertThat(analysis.scope(), is(SetStatement.Scope.GLOBAL));
        assertThat(
            analysis.settings().get("stats.operations_log_size").get(0),
            Matchers.<Expression>is(Literal.fromObject(1))
        );

        analysis = analyze("SET GLOBAL TRANSIENT stats.jobs_log_size=2");
        assertThat(analysis.isPersistent(), is(false));
        assertThat(analysis.scope(), is(SetStatement.Scope.GLOBAL));
        assertThat(
            analysis.settings().get("stats.jobs_log_size").get(0),
            Matchers.<Expression>is(Literal.fromObject(2))
        );

        analysis = analyze("SET GLOBAL TRANSIENT stats.enabled=false, stats.operations_log_size=0, stats.jobs_log_size=0");
        assertThat(analysis.scope(), is(SetStatement.Scope.GLOBAL));
        assertThat(analysis.isPersistent(), is(false));
        assertThat(
            analysis.settings().get("stats.operations_log_size").get(0),
            Matchers.<Expression>is(Literal.fromObject(0))
        );
        assertThat(
            analysis.settings().get("stats.jobs_log_size").get(0),
            Matchers.<Expression>is(Literal.fromObject(0))
        );
    }

    @Test
    public void testSetLocal() throws Exception {
        SetAnalyzedStatement analysis = analyze("SET LOCAL something TO 2");
        assertThat(analysis.scope(), is(SetStatement.Scope.LOCAL));
        assertThat(analysis.settings().get("something").get(0), Matchers.<Expression>is(Literal.fromObject(2)));

        analysis = analyze("SET LOCAL something TO DEFAULT");
        assertThat(analysis.scope(), is(SetStatement.Scope.LOCAL));
        assertThat(analysis.settings().get("something").size(), is(0));
    }

    @Test
    public void testSetSessionTransactionMode() throws Exception {
        SetAnalyzedStatement analysis = analyze("SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ UNCOMMITTED");
        assertThat(analysis.scope(), is(SetStatement.Scope.SESSION_TRANSACTION_MODE));
        assertThat(analysis.settings().get("transaction_mode").size(), is(4));
    }

    @Test
    public void testSetSession() throws Exception {
        SetAnalyzedStatement analysis = analyze("SET SESSION something TO 2");
        assertThat(analysis.scope(), is(SetStatement.Scope.SESSION));
        assertThat(analysis.settings().get("something").size(), is(1));
        assertThat(analysis.settings().get("something").get(0), Matchers.<Expression>is(Literal.fromObject(2)));

        analysis = analyze("SET SESSION something = 1,2,3");
        assertThat(analysis.scope(), is(SetStatement.Scope.SESSION));
        assertThat(analysis.settings().get("something").size(), is(3));
        assertThat(analysis.settings().get("something").get(1), Matchers.<Expression>is(Literal.fromObject(2)));
    }

    @Test
    public void testSet() throws Exception {
        SetAnalyzedStatement analysis = analyze("SET something TO 2");
        assertThat(analysis.scope(), is(SetStatement.Scope.SESSION));
        assertThat(analysis.settings().get("something").get(0), Matchers.<Expression>is(Literal.fromObject(2)));

        analysis = analyze("SET something = DEFAULT");
        assertThat(analysis.scope(), is(SetStatement.Scope.SESSION));
        assertThat(analysis.settings().get("something").size(), is(0));

        analysis = analyze("SET something = default");
        assertThat(analysis.scope(), is(SetStatement.Scope.SESSION));
        assertThat(analysis.settings().get("something").size(), is(0));
    }

    @Test
    public void testSetFullQualified() throws Exception {
        SetAnalyzedStatement analysis = analyze("SET GLOBAL PERSISTENT stats['operations_log_size']=1");
        assertThat(analysis.isPersistent(), is(true));
        assertThat(analysis.settings().get("stats.operations_log_size").get(0), Matchers.<Expression>is(Literal.fromObject(1)));
    }

    @Test
    public void testSetSessionInvalidSetting() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("GLOBAL Cluster setting 'stats.operations_log_size' cannot be used with SET SESSION / LOCAL");
        analyze("SET SESSION stats.operations_log_size=1");
    }

    @Test
    public void testObjectValue() throws Exception {
        SetAnalyzedStatement analysis = analyze("SET GLOBAL PERSISTENT cluster.graceful_stop = {timeout='1h'}");
        Multimap<String, Expression> map = LinkedListMultimap.create();
        map.put("timeout", Literal.fromObject("1h"));
        Literal expected = new ObjectLiteral(map);
        assertThat(analysis.settings().get("cluster.graceful_stop").get(0), Matchers.<Expression>is(expected));
    }

    @Test
    public void testSetRuntimeSettingSubscript() {
        SetAnalyzedStatement analysis =
            analyze("SET GLOBAL TRANSIENT cluster['routing']['allocation']['include'] = {_host = 'host1.example.com'}");
        Expression expression = analysis.settings().get("cluster.routing.allocation.include").get(0);
        assertThat(expression.toString(), is("{\"_host\"= 'host1.example.com'}"));
    }

    @Test
    public void testReset() throws Exception {
        ResetAnalyzedStatement analysis = analyze("RESET GLOBAL stats.enabled");
        assertThat(analysis.settingsToRemove(), contains("stats.enabled"));

        analysis = analyze("RESET GLOBAL stats");
        assertThat(
            analysis.settingsToRemove(),
            containsInAnyOrder(
                "stats.breaker.log.operations.limit",
                "stats.breaker.log.operations.overhead",
                "stats.breaker.log.jobs.limit",
                "stats.breaker.log.jobs.overhead",
                "stats.enabled",
                "stats.jobs_log_size",
                "stats.jobs_log_expiration",
                "stats.operations_log_size",
                "stats.operations_log_expiration",
                "stats.service.interval")
        );
    }

    @Test
    public void testSetNonRuntimeSetting() {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Setting 'gateway.recover_after_time' cannot be set/reset at runtime");
        analyze("SET GLOBAL TRANSIENT gateway.recover_after_time = '5m'");
    }

    @Test
    public void testResetNonRuntimeSetting() {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Setting 'gateway.recover_after_nodes' cannot be set/reset at runtime");
        analyze("RESET GLOBAL gateway.recover_after_nodes");
    }

    @Test
    public void testSetNonRuntimeSettingObject() {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Setting 'gateway.recover_after_nodes' cannot be set/reset at runtime");
        analyze("SET GLOBAL TRANSIENT gateway = {recover_after_nodes = 3}");
    }

    @Test
    public void testResetNonRuntimeSettingObject() {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Setting 'gateway.recover_after_nodes' cannot be set/reset at runtime");
        analyze("RESET GLOBAL gateway");
    }

    @Test
    public void testSetLoggingSetting() {
        SetAnalyzedStatement analysis = analyze("SET GLOBAL TRANSIENT \"logger.action\" = 'INFo'");
        assertThat(analysis.isPersistent(), is(false));
        assertThat(
            analysis.settings().get("logger.action").get(0),
            Matchers.<Expression>is(Literal.fromObject("INFo"))
        );
    }

    @Test
    public void testResetLoggingSetting() {
        ResetAnalyzedStatement analysis = analyze("RESET GLOBAL \"logger.action\"");
        assertThat(analysis.settingsToRemove(), Matchers.<Set<String>>is(ImmutableSet.of("logger.action")));
    }
}
