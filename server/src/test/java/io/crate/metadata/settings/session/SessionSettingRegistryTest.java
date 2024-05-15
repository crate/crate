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

package io.crate.metadata.settings.session;

import static io.crate.testing.TestingHelpers.createNodeContext;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.fail;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import io.crate.analyze.SymbolEvaluator;
import io.crate.common.unit.TimeValue;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.NodeContext;
import io.crate.metadata.settings.CoordinatorSessionSettings;
import io.crate.planner.optimizer.LoadedRules;

public class SessionSettingRegistryTest extends ESTestCase {

    private static final CoordinatorSessionSettings SESSION_SETTINGS = CoordinatorSessionSettings.systemDefaults();
    private static final NodeContext NODE_CTX = createNodeContext();
    private static final Function<Symbol, Object> EVAL = s -> SymbolEvaluator.evaluateWithoutParams(
        CoordinatorTxnCtx.systemTransactionContext(),
        NODE_CTX,
        s
    );

    @Test
    public void test_max_index_keys_session_setting_cannot_be_changed() {
        SessionSetting<?> setting = new SessionSettingRegistry(Set.of(LoadedRules.INSTANCE)).settings().get(SessionSettingRegistry.MAX_INDEX_KEYS);
        assertThatThrownBy(() -> setting.apply(SESSION_SETTINGS, generateInput("32"), EVAL))
            .isExactlyInstanceOf(UnsupportedOperationException.class)
            .hasMessage("\"max_index_keys\" cannot be changed.");
    }

    @Test
    public void test_max_identifier_length_session_setting_cannot_be_changed() {
        SessionSetting<?> setting = new SessionSettingRegistry(Set.of(LoadedRules.INSTANCE)).settings().get(SessionSettingRegistry.MAX_IDENTIFIER_LENGTH);
        assertThatThrownBy(() -> setting.apply(SESSION_SETTINGS, generateInput("255"), EVAL))
            .isExactlyInstanceOf(UnsupportedOperationException.class)
            .hasMessage("\"max_identifier_length\" cannot be changed.");
    }

    @Test
    public void test_server_version_num_session_setting_cannot_be_changed() {
        SessionSetting<?> setting = new SessionSettingRegistry(Set.of(LoadedRules.INSTANCE)).settings().get(SessionSettingRegistry.SERVER_VERSION_NUM);
        assertThatThrownBy(() -> setting.apply(SESSION_SETTINGS, generateInput("100000"), EVAL))
            .isExactlyInstanceOf(UnsupportedOperationException.class)
            .hasMessage("\"server_version_num\" cannot be changed.");
    }

    @Test
    public void test_server_version_session_setting_cannot_be_changed() {
        SessionSetting<?> setting = new SessionSettingRegistry(Set.of(LoadedRules.INSTANCE)).settings().get(SessionSettingRegistry.SERVER_VERSION);
        assertThatThrownBy(() -> setting.apply(SESSION_SETTINGS, generateInput("10.0"), EVAL))
            .isExactlyInstanceOf(UnsupportedOperationException.class)
            .hasMessage("\"server_version\" cannot be changed.");
    }

    @Test
    public void test_standard_confirming_strings_session_setting_cannot_be_set_to_false() {
        SessionSetting<?> setting = new SessionSettingRegistry(Set.of(LoadedRules.INSTANCE)).settings().get(SessionSettingRegistry.STANDARD_CONFORMING_STRINGS);
        var value = generateInput(randomFrom("no", "false", "0", "no"));
        assertThatThrownBy(() -> setting.apply(SESSION_SETTINGS, value, EVAL))
            .isExactlyInstanceOf(UnsupportedOperationException.class)
            .hasMessage("\"standard_conforming_strings\" cannot be changed.");
    }

    @Test
    public void test_standard_confirming_strings_session_setting_invalid_values() {
        SessionSetting<?> setting = new SessionSettingRegistry(Set.of(LoadedRules.INSTANCE)).settings().get(SessionSettingRegistry.STANDARD_CONFORMING_STRINGS);
        var value = generateInput("invalid");
        assertThatThrownBy(() -> setting.apply(SESSION_SETTINGS, value, EVAL))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Can't convert \"invalid\" to boolean");
    }

    @Test
    public void testHashJoinSessionSetting() {
        SessionSetting<?> setting = new SessionSettingRegistry(Set.of(LoadedRules.INSTANCE)).settings().get(SessionSettingRegistry.HASH_JOIN_KEY);
        assertBooleanNonEmptySetting(SESSION_SETTINGS::hashJoinsEnabled, setting, true);
    }

    @Test
    public void testSettingErrorOnUnknownObjectKey() {
        SessionSetting<?> setting = new SessionSettingRegistry(Set.of(LoadedRules.INSTANCE)).settings().get(SessionSettingRegistry.ERROR_ON_UNKNOWN_OBJECT_KEY);
        assertBooleanNonEmptySetting(SESSION_SETTINGS::errorOnUnknownObjectKey, setting, true);
    }

    @Test
    public void test_search_path_session_setting() {
        SessionSetting<?> setting = new SessionSettingRegistry(Set.of(LoadedRules.INSTANCE)).settings().get("search_path");
        assertThat(setting.defaultValue(),is("doc"));
        setting.apply(SESSION_SETTINGS, generateInput("a_schema"), EVAL);
        assertThat(setting.getValue(SESSION_SETTINGS)).isEqualTo("a_schema");
        setting.apply(SESSION_SETTINGS, generateInput("a_schema,  pg_catalog ,b_schema", " c_schema "), EVAL);
        assertThat(setting.getValue(SESSION_SETTINGS)).isEqualTo("a_schema, pg_catalog, b_schema, c_schema");
    }

    @Test
    public void test_date_style_session_setting() {
        SessionSetting<?> setting = new SessionSettingRegistry(Set.of(LoadedRules.INSTANCE)).settings().get(SessionSettingRegistry.DATE_STYLE.name());
        assertThat(setting.defaultValue(),is("ISO"));
        setting.apply(SESSION_SETTINGS, generateInput("iso"), EVAL);
        assertThat(SESSION_SETTINGS.dateStyle()).isEqualTo("ISO");
        setting.apply(SESSION_SETTINGS, generateInput("MDY"), EVAL);
        assertThat(SESSION_SETTINGS.dateStyle()).isEqualTo("ISO");
        setting.apply(SESSION_SETTINGS, generateInput("ISO, MDY"), EVAL);
        assertThat(SESSION_SETTINGS.dateStyle()).isEqualTo("ISO");
        assertThrows(IllegalArgumentException.class,
                     () -> setting.apply(SESSION_SETTINGS, generateInput("ISO, YDM"), EVAL),
                     "Invalid value for parameter \"datestyle\": \"YDM\". Valid values include: [\"ISO\"].");
        assertThrows(IllegalArgumentException.class,
                     () -> setting.apply(SESSION_SETTINGS, generateInput("German,ISO"), EVAL),
                     "Invalid value for parameter \"datestyle\": \"GERMAN\". Valid values include: [\"ISO\"].");
        assertThrows(IllegalArgumentException.class,
                     () -> setting.apply(SESSION_SETTINGS, generateInput("SQL, MDY"), EVAL),
                     "Invalid value for parameter \"datestyle\": \"SQL\". Valid values include: [\"ISO\"].");
    }

    @Test
    public void test_statement_timeout_max_value() throws Exception {
        var statementTimeout = SessionSettingRegistry.STATEMENT_TIMEOUT;
        statementTimeout.apply(SESSION_SETTINGS, generateInput("24 days"), EVAL);
        assertThat(SESSION_SETTINGS.statementTimeout()).isEqualTo(TimeValue.timeValueMillis(2073600000L));

        assertThatThrownBy(() -> statementTimeout.apply(SESSION_SETTINGS, generateInput("25 days"), EVAL))
            .isExactlyInstanceOf(ArithmeticException.class)
            .hasMessage("Value cannot fit in an int: 2160000000");
    }

    @Test
    public void test_statement_timeout_accepts_int() throws Exception {
        var statementTimeout = SessionSettingRegistry.STATEMENT_TIMEOUT;
        statementTimeout.apply(SESSION_SETTINGS, List.of(Literal.of(200)), EVAL);
    }

    private void assertBooleanNonEmptySetting(Supplier<Boolean> contextBooleanSupplier,
                                              SessionSetting<?> sessionSetting,
                                              boolean defaultValue) {
        assertThat(contextBooleanSupplier.get()).isEqualTo(defaultValue);
        sessionSetting.apply(SESSION_SETTINGS, generateInput("true"), EVAL);
        assertThat(contextBooleanSupplier.get()).isTrue();
        sessionSetting.apply(SESSION_SETTINGS, generateInput("false"), EVAL);
        assertThat(contextBooleanSupplier.get()).isFalse();
        sessionSetting.apply(SESSION_SETTINGS, generateInput("TrUe"), EVAL);
        assertThat(contextBooleanSupplier.get()).isTrue();
        try {
            sessionSetting.apply(SESSION_SETTINGS, generateInput(""), EVAL);
            fail("Should have failed to apply setting.");
        } catch (IllegalArgumentException e) {
            assertThat(contextBooleanSupplier.get()).isTrue();
        }
        try {
            sessionSetting.apply(SESSION_SETTINGS, generateInput("invalid", "input"), EVAL);
            fail("Should have failed to apply setting.");
        } catch (IllegalArgumentException e) {
            assertThat(contextBooleanSupplier.get()).isTrue();
        }
    }

    private static List<Symbol> generateInput(String... inputs) {
        ArrayList<Symbol> symbols = new ArrayList<>(inputs.length);
        for (String input : inputs) {
            symbols.add(Literal.of(input));
        }
        return symbols;
    }
}
