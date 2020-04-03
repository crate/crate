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

package io.crate.planner.node.ddl;

import io.crate.analyze.SymbolEvaluator;
import io.crate.data.Row;
import io.crate.data.RowN;
import io.crate.expression.scalar.arithmetic.ArrayFunction;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.ParameterSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.SearchPath;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.settings.SessionSettings;
import io.crate.planner.operators.SubQueryResults;
import io.crate.sql.tree.Assignment;
import io.crate.test.integration.CrateUnitTest;
import io.crate.types.DataTypes;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static io.crate.planner.node.ddl.UpdateSettingsPlan.buildSettingsFrom;
import static io.crate.testing.TestingHelpers.getFunctions;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;

public class UpdateSettingsPlanTest extends CrateUnitTest {

    private Function<Symbol, Object> symbolEvaluator(Row row) {
        return x -> SymbolEvaluator.evaluate(
            TransactionContext.of(new SessionSettings("", SearchPath.createSearchPathFrom(""))),
            getFunctions(),
            x,
            row,
            SubQueryResults.EMPTY);
    }

    @Test
    public void testUpdateSettingsWithStringValue() throws Exception {
        List<Assignment<Symbol>> settings = List.of(
            new Assignment<>(Literal.of("cluster.graceful_stop.min_availability"), List.of(Literal.of("full"))));

        Settings expected = Settings.builder()
            .put("cluster.graceful_stop.min_availability", "full")
            .build();

        assertThat(buildSettingsFrom(settings, symbolEvaluator(Row.EMPTY)), is(expected));
    }

    @Test
    public void testUpdateMultipleSettingsWithParameters() throws Exception {
        List<Assignment<Symbol>> settings = List.of(
            new Assignment<>(Literal.of("stats.operations_log_size"), List.of(new ParameterSymbol(0, DataTypes.LONG))),
            new Assignment<>(Literal.of("stats.jobs_log_size"), List.of(new ParameterSymbol(1, DataTypes.LONG)))
        );
        Settings expected = Settings.builder()
            .put("stats.operations_log_size", 10)
            .put("stats.jobs_log_size", 25)
            .build();

        assertThat(
            buildSettingsFrom(settings, symbolEvaluator(new RowN(new Object[]{10, 25}))),
            is(expected)
        );
    }

    @Test
    public void testUpdateObjectWithParameter() throws Exception {
        List<Assignment<Symbol>> settings = List.of(
            new Assignment<>(Literal.of("stats"), List.of(new ParameterSymbol(0, DataTypes.LONG))));

        Map<String, Object> param = MapBuilder.<String, Object>newMapBuilder()
            .put("enabled", true)
            .put("breaker",
                 MapBuilder.newMapBuilder()
                     .put("log", MapBuilder.newMapBuilder()
                         .put("jobs", MapBuilder.newMapBuilder()
                             .put("overhead", 1.05d).map()
                         ).map()
                     ).map()
            ).map();

        Settings expected = Settings.builder()
            .put("stats.enabled", true)
            .put("stats.breaker.log.jobs.overhead", 1.05d)
            .build();

        assertThat(buildSettingsFrom(settings, symbolEvaluator(new RowN(new Object[]{param}))), is(expected));
    }

    @Test
    public void testSetNonRuntimeSettingObject() {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Setting 'gateway.recover_after_nodes' cannot be set/reset at runtime");

        List<Assignment<Symbol>> settings = List.of(
            new Assignment<>(Literal.of("gateway"), List.of(Literal.of(Map.of("recover_after_nodes", 3))))
        );


        buildSettingsFrom(settings, symbolEvaluator(Row.EMPTY));
    }

    @Test
    public void testSetNonRuntimeSetting() {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Setting 'gateway.recover_after_time' cannot be set/reset at runtime");

        List<Assignment<Symbol>> settings = List.of(
            new Assignment<>(Literal.of("gateway.recover_after_time"), List.of(Literal.of("5m"))
            ));

        buildSettingsFrom(settings, symbolEvaluator(Row.EMPTY));
    }

    @Test
    public void testUnsupportedSetting() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Setting 'unsupported_setting' is not supported");

        List<Assignment<Symbol>> settings = List.of(
            new Assignment<>(Literal.of("unsupported_setting"), List.of(Literal.of("foo"))
            ));

        buildSettingsFrom(settings, symbolEvaluator(Row.EMPTY));
    }

    @Test
    public void test_build_settings_with_null_value() {
        List<Assignment<Symbol>> settings = List.of(new Assignment<>(Literal.of("cluster.routing.allocation.exclude._id"), Literal.NULL));

        expectedException.expectMessage("Cannot set \"cluster.routing.allocation.exclude._id\" to `null`. Use `RESET [GLOBAL] \"cluster.routing.allocation.exclude._id\"` to reset a setting to its default value");
        buildSettingsFrom(settings, symbolEvaluator(Row.EMPTY));
    }

    @Test
    public void test_array_is_implicitly_converted_to_comma_separated_string() {
        var idsArray = new io.crate.expression.symbol.Function(
            ArrayFunction.createInfo(List.of(DataTypes.STRING, DataTypes.STRING)),
            ArrayFunction.SIGNATURE,
            List.of(Literal.of("id1"), Literal.of("id2"))
        );
        Assignment<Symbol> assignment = new Assignment<>(Literal.of("cluster.routing.allocation.exclude._id"), idsArray);
        Settings settings = buildSettingsFrom(List.of(assignment), symbolEvaluator(Row.EMPTY));
        assertThat(settings.get("cluster.routing.allocation.exclude._id"), is("id1,id2"));
        assertThat(settings.getAsList("cluster.routing.allocation.exclude._id"), contains("id1", "id2"));
    }
}
