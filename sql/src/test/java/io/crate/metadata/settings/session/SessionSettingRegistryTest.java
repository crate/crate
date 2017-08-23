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
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.fail;

public class SessionSettingRegistryTest {

    @Test
    public void testSemiJoinSessionSetting() {
        SessionContext sessionContext = new SessionContext(null, null, x -> {}, x -> {});
        SessionSettingApplier applier = SessionSettingRegistry.getApplier(SessionSettingRegistry.SEMI_JOIN_KEY);

        assertThat(sessionContext.getSemiJoinsRewriteEnabled(), is(false));
        applier.apply(Row.EMPTY, generateInput("true"), sessionContext);
        assertThat(sessionContext.getSemiJoinsRewriteEnabled(), is(true));
        applier.apply(Row.EMPTY, generateInput("false"), sessionContext);
        assertThat(sessionContext.getSemiJoinsRewriteEnabled(), is(false));
        applier.apply(Row.EMPTY, generateInput("TrUe"), sessionContext);
        assertThat(sessionContext.getSemiJoinsRewriteEnabled(), is(true));
        try {
            applier.apply(Row.EMPTY, generateInput(""), sessionContext);
            fail("Should have failed to apply setting.");
        } catch (IllegalArgumentException e) {
            assertThat(sessionContext.getSemiJoinsRewriteEnabled(), is(true));
        }
        try {
            applier.apply(Row.EMPTY, generateInput("invalid", "input"), sessionContext);
            fail("Should have failed to apply setting.");
        } catch (IllegalArgumentException e) {
            assertThat(sessionContext.getSemiJoinsRewriteEnabled(), is(true));
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
