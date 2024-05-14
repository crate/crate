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

package io.crate.planner.node.ddl;

import static io.crate.planner.node.ddl.ResetSettingsPlan.buildSettingsFrom;
import static org.assertj.core.api.Assertions.assertThat;
import static io.crate.testing.TestingHelpers.createNodeContext;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Set;
import java.util.function.Function;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import io.crate.analyze.SymbolEvaluator;
import io.crate.data.Row;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.SearchPath;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.settings.SessionSettings;
import io.crate.planner.operators.SubQueryResults;

public class ResetSettingsPlanTest extends ESTestCase {

    @Test
    public void testResetSimple() throws Exception {
        Set<Symbol> settings = Set.of(Literal.of("stats.enabled"));

        Settings expected = Settings.builder()
            .put("stats.enabled", (String) null)
            .build();

        assertThat(buildSettingsFrom(settings, symbolEvaluator(Row.EMPTY))).isEqualTo(expected);
    }

    @Test
    public void testReset() throws Exception {
        Set<Symbol> settings = Set.of(Literal.of("stats"));

        Settings expected = Settings.builder()
            .put("stats.breaker.log.operations.limit", (String) null)
            .put("stats.breaker.log.jobs.limit", (String) null)
            .put("stats.enabled", (String) null)
            .put("stats.jobs_log_size", (String) null)
            .put("stats.jobs_log_expiration", (String) null)
            .put("stats.jobs_log_filter", (String) null)
            .put("stats.jobs_log_persistent_filter", (String) null)
            .put("stats.operations_log_size", (String) null)
            .put("stats.operations_log_expiration", (String) null)
            .put("stats.service.interval", (String) null)
            .put("stats.service.max_bytes_per_sec", (String) null)
            .build();

        assertThat(buildSettingsFrom(settings, symbolEvaluator(Row.EMPTY))).isEqualTo(expected);

    }

    @Test
    public void testResetNonRuntimeSetting() {
        Set<Symbol> settings = Set.of(Literal.of("gateway"));

        assertThatThrownBy(() -> buildSettingsFrom(settings, symbolEvaluator(Row.EMPTY)))
            .isInstanceOf(UnsupportedOperationException.class)
            .hasMessageContaining("cannot be set/reset at runtime");
    }

    @Test
    public void testResetNonRuntimeSettingObject() {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Setting 'gateway.recover_after_nodes' cannot be set/reset at runtime");

        Set<Symbol> settings = Set.of(Literal.of("gateway.recover_after_nodes"));

        buildSettingsFrom(settings, symbolEvaluator(Row.EMPTY));
    }

    @Test
    public void testResetLoggingSetting() {
        Set<Symbol> settings = Set.of(Literal.of("logger.action"));
        Settings expected = Settings.builder()
            .put("logger.action", (String) null)
            .build();

        assertThat(buildSettingsFrom(settings, symbolEvaluator(Row.EMPTY))).isEqualTo(expected);
    }

    private Function<Symbol, Object> symbolEvaluator(Row row) {
        return x -> SymbolEvaluator.evaluate(
            TransactionContext.of(new SessionSettings("", SearchPath.createSearchPathFrom(""))),
            createNodeContext(),
            x,
            row,
            SubQueryResults.EMPTY);
    }

    @Test
    public void testUpdateSettingsWithStringValue() throws Exception {
        Set<Symbol> settings = Set.of(Literal.of("cluster.graceful_stop.min_availability"));

        Settings expected = Settings.builder()
            .put("cluster.graceful_stop.min_availability", (String) null)
            .build();

        assertThat(buildSettingsFrom(settings, symbolEvaluator(Row.EMPTY))).isEqualTo(expected);
    }

    @Test
    public void testUpdateMultipleSettingsWithParameters() throws Exception {
        Set<Symbol> settings = Set.of(Literal.of("stats.operations_log_size"), Literal.of("stats.jobs_log_size"));

        Settings expected = Settings.builder()
            .put("stats.operations_log_size", (String) null)
            .put("stats.jobs_log_size", (String) null)
            .build();

        assertThat(
            buildSettingsFrom(settings, symbolEvaluator(Row.EMPTY))).isEqualTo(expected);
    }

    @Test
    public void testUnsupportedSetting() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Setting 'unsupported_setting' is not supported");

        Set<Symbol> settings = Set.of(Literal.of("unsupported_setting"));
        buildSettingsFrom(settings, symbolEvaluator(Row.EMPTY));
    }
}
