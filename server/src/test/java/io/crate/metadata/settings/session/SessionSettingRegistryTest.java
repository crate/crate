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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.fail;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

import org.junit.Test;

import io.crate.analyze.SymbolEvaluator;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.NodeContext;
import io.crate.metadata.settings.CoordinatorSessionSettings;
import io.crate.planner.optimizer.LoadedRules;

public class SessionSettingRegistryTest {

    private CoordinatorSessionSettings sessionSettings = CoordinatorSessionSettings.systemDefaults();
    private NodeContext nodeCtx = createNodeContext();
    private Function<Symbol, Object> eval = s -> SymbolEvaluator.evaluateWithoutParams(
        CoordinatorTxnCtx.systemTransactionContext(),
        nodeCtx,
        s
    );

    @Test
    public void testMaxIndexKeysSessionSettingCannotBeChanged() {
        SessionSetting<?> setting = new SessionSettingRegistry(Set.of(new LoadedRules())).settings().get(SessionSettingRegistry.MAX_INDEX_KEYS);
        assertThrows(UnsupportedOperationException.class,
                     () -> setting.apply(sessionSettings, generateInput("32"), eval),
                     "\"max_index_keys\" cannot be changed.");
    }

    @Test
    public void testHashJoinSessionSetting() {
        SessionSetting<?> setting = new SessionSettingRegistry(Set.of(new LoadedRules())).settings().get(SessionSettingRegistry.HASH_JOIN_KEY);
        assertBooleanNonEmptySetting(sessionSettings::hashJoinsEnabled, setting, true);
    }

    @Test
    public void testSettingErrorOnUnknownObjectKey() {
        SessionSetting<?> setting = new SessionSettingRegistry(Set.of(new LoadedRules())).settings().get(SessionSettingRegistry.ERROR_ON_UNKNOWN_OBJECT_KEY);
        assertBooleanNonEmptySetting(sessionSettings::errorOnUnknownObjectKey, setting, true);
    }

    @Test
    public void test_search_path_session_setting() {
        SessionSetting<?> setting = new SessionSettingRegistry(Set.of(new LoadedRules())).settings().get("search_path");
        assertThat(setting.defaultValue(),is("doc"));
        setting.apply(sessionSettings, generateInput("a_schema"), eval);
        assertThat(setting.getValue(sessionSettings),is("a_schema"));
        setting.apply(sessionSettings, generateInput("a_schema,  pg_catalog ,b_schema", " c_schema "), eval);
        assertThat(setting.getValue(sessionSettings),is("a_schema, pg_catalog, b_schema, c_schema"));
    }

    @Test
    public void test_date_style_session_setting() {
        SessionSetting<?> setting = new SessionSettingRegistry(Set.of(new LoadedRules())).settings().get(SessionSettingRegistry.DATE_STYLE.name());
        assertThat(setting.defaultValue(),is("ISO"));
        setting.apply(sessionSettings, generateInput("iso"), eval);
        assertThat(sessionSettings.dateStyle(), is("ISO"));
        setting.apply(sessionSettings, generateInput("MDY"), eval);
        assertThat(sessionSettings.dateStyle(), is("ISO"));
        setting.apply(sessionSettings, generateInput("ISO, MDY"), eval);
        assertThat(sessionSettings.dateStyle(), is("ISO"));
        assertThrows(IllegalArgumentException.class,
                     () -> setting.apply(sessionSettings, generateInput("ISO, YDM"), eval),
                     "Invalid value for parameter \"datestyle\": \"YDM\". Valid values include: [\"ISO\"].");
        assertThrows(IllegalArgumentException.class,
                     () -> setting.apply(sessionSettings, generateInput("German,ISO"), eval),
                     "Invalid value for parameter \"datestyle\": \"GERMAN\". Valid values include: [\"ISO\"].");
        assertThrows(IllegalArgumentException.class,
                     () -> setting.apply(sessionSettings, generateInput("SQL, MDY"), eval),
                     "Invalid value for parameter \"datestyle\": \"SQL\". Valid values include: [\"ISO\"].");
    }

    private void assertBooleanNonEmptySetting(Supplier<Boolean> contextBooleanSupplier,
                                              SessionSetting<?> sessionSetting,
                                              boolean defaultValue) {
        assertThat(contextBooleanSupplier.get(), is(defaultValue));
        sessionSetting.apply(sessionSettings, generateInput("true"), eval);
        assertThat(contextBooleanSupplier.get(), is(true));
        sessionSetting.apply(sessionSettings, generateInput("false"), eval);
        assertThat(contextBooleanSupplier.get(), is(false));
        sessionSetting.apply(sessionSettings, generateInput("TrUe"), eval);
        assertThat(contextBooleanSupplier.get(), is(true));
        try {
            sessionSetting.apply(sessionSettings, generateInput(""), eval);
            fail("Should have failed to apply setting.");
        } catch (IllegalArgumentException e) {
            assertThat(contextBooleanSupplier.get(), is(true));
        }
        try {
            sessionSetting.apply(sessionSettings, generateInput("invalid", "input"), eval);
            fail("Should have failed to apply setting.");
        } catch (IllegalArgumentException e) {
            assertThat(contextBooleanSupplier.get(), is(true));
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
