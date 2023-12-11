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

package io.crate.role.scalar;

import static io.crate.testing.Asserts.isLiteral;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Before;
import org.junit.Test;

import io.crate.expression.scalar.ScalarTestCase;
import io.crate.expression.symbol.Symbol;
import io.crate.testing.SqlExpressions;
import io.crate.role.Role;

public class UserFunctionTest extends ScalarTestCase {

    private static final Role TEST_USER = Role.userOf("testUser");

    @Before
    public void prepare() {
        sqlExpressions = new SqlExpressions(tableSources, null, TEST_USER);
    }

    @Test
    public void testNormalizeCurrentUser() {
        assertNormalize("current_user", isLiteral("testUser"), false);
    }

    @Test
    public void testNormalizeSessionUser() {
        assertNormalize("session_user", isLiteral("testUser"), false);
    }

    @Test
    public void testNormalizeUser() {
        assertNormalize("user", isLiteral("testUser"), false);
    }

    @Test
    public void testFormatFunctionsWithoutBrackets() {
        sqlExpressions.context().allowEagerNormalize(false);
        Symbol f = sqlExpressions.asSymbol("current_user");
        assertThat(f).hasToString("CURRENT_USER");

        f = sqlExpressions.asSymbol("session_user");
        assertThat(f).hasToString("SESSION_USER");

        f = sqlExpressions.asSymbol("user");
        assertThat(f).hasToString("CURRENT_USER");
    }
}
