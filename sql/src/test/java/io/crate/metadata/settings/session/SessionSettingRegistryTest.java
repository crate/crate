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
import io.crate.data.Row;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.StringLiteral;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.fail;

public class SessionSettingRegistryTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private SessionContext sessionContext = SessionContext.systemSessionContext();

    @Test
    public void testMaxIndexKeysSessionSettingCannotBeChanged() {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("\"max_index_keys\" cannot be changed.");
        SessionSetting<?> setting = SessionSettingRegistry.SETTINGS.get(SessionSettingRegistry.MAX_INDEX_KEYS);
        setting.apply(Row.EMPTY, generateInput("32"), sessionContext);
    }

    @Test
    public void testHashJoinSessionSetting() {
        SessionSetting<?> setting = SessionSettingRegistry.SETTINGS.get(SessionSettingRegistry.HASH_JOIN_KEY);
        assertBooleanNonEmptySetting(sessionContext::isHashJoinEnabled, setting, true);
    }

    private void assertBooleanNonEmptySetting(Supplier<Boolean> contextBooleanSupplier,
                                              SessionSetting<?> sessionSetting,
                                              boolean defaultValue) {
        assertThat(contextBooleanSupplier.get(), is(defaultValue));
        sessionSetting.apply(Row.EMPTY, generateInput("true"), sessionContext);
        assertThat(contextBooleanSupplier.get(), is(true));
        sessionSetting.apply(Row.EMPTY, generateInput("false"), sessionContext);
        assertThat(contextBooleanSupplier.get(), is(false));
        sessionSetting.apply(Row.EMPTY, generateInput("TrUe"), sessionContext);
        assertThat(contextBooleanSupplier.get(), is(true));
        try {
            sessionSetting.apply(Row.EMPTY, generateInput(""), sessionContext);
            fail("Should have failed to apply setting.");
        } catch (IllegalArgumentException e) {
            assertThat(contextBooleanSupplier.get(), is(true));
        }
        try {
            sessionSetting.apply(Row.EMPTY, generateInput("invalid", "input"), sessionContext);
            fail("Should have failed to apply setting.");
        } catch (IllegalArgumentException e) {
            assertThat(contextBooleanSupplier.get(), is(true));
        }
    }

    private static List<Expression> generateInput(String... inputs) {
        ArrayList<Expression> expressions = new ArrayList<>(inputs.length);
        for (String input : inputs) {
            expressions.add(StringLiteral.fromObject(input));
        }
        return expressions;
    }
}
