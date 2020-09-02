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

package io.crate.metadata.settings.session;

import io.crate.action.sql.SessionContext;
import io.crate.analyze.SymbolEvaluator;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.NodeContext;
import io.crate.metadata.settings.MetadataSettings;
import io.crate.planner.optimizer.LoadedRules;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.crate.metadata.settings.MetadataSettings.EXPOSE_OBJECT_COLUMNS_SETTING;
import static io.crate.metadata.settings.MetadataSettings.EXPOSE_OBJECT_COLUMNS;
import static io.crate.testing.TestingHelpers.createNodeContext;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.fail;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

public class SessionSettingRegistryTest {

    private SessionContext sessionContext = SessionContext.systemSessionContext();
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
                     () -> setting.apply(sessionContext, generateInput("32"), eval),
                     "\"max_index_keys\" cannot be changed.");
    }

    @Test
    public void testHashJoinSessionSetting() {
        SessionSetting<?> setting = new SessionSettingRegistry(Set.of(new LoadedRules())).settings().get(SessionSettingRegistry.HASH_JOIN_KEY);
        assertBooleanNonEmptySetting(sessionContext::isHashJoinEnabled, setting, true);
    }

    @Test
    public void test_expose_object_column_session_setting() {
        var registry = new SessionSettingRegistry(Set.of(
            new MetadataSettings(
                Settings.EMPTY,
                new ClusterSettings(Settings.EMPTY, Set.of(EXPOSE_OBJECT_COLUMNS_SETTING.setting()))
            )
        ));
        SessionSetting<?> setting = registry.settings().get(EXPOSE_OBJECT_COLUMNS);
        assertBooleanNonEmptySetting(sessionContext::exposeObjectColumns, setting, true);
    }

    private void assertBooleanNonEmptySetting(Supplier<Boolean> contextBooleanSupplier,
                                              SessionSetting<?> sessionSetting,
                                              boolean defaultValue) {
        assertThat(contextBooleanSupplier.get(), is(defaultValue));
        sessionSetting.apply(sessionContext, generateInput("true"), eval);
        assertThat(contextBooleanSupplier.get(), is(true));
        sessionSetting.apply(sessionContext, generateInput("false"), eval);
        assertThat(contextBooleanSupplier.get(), is(false));
        sessionSetting.apply(sessionContext, generateInput("TrUe"), eval);
        assertThat(contextBooleanSupplier.get(), is(true));
        try {
            sessionSetting.apply(sessionContext, generateInput(""), eval);
            fail("Should have failed to apply setting.");
        } catch (IllegalArgumentException e) {
            assertThat(contextBooleanSupplier.get(), is(true));
        }
        try {
            sessionSetting.apply(sessionContext, generateInput("invalid", "input"), eval);
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
