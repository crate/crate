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

package io.crate.user.scalar;

import io.crate.user.User;
import io.crate.expression.scalar.ScalarTestCase;
import io.crate.expression.symbol.Symbol;
import io.crate.testing.SqlExpressions;
import org.junit.Before;
import org.junit.Test;

import static io.crate.testing.SymbolMatchers.isLiteral;
import static org.hamcrest.Matchers.is;

public class UserFunctionTest extends ScalarTestCase {

    private static final User TEST_USER = User.of("testUser");

    @Before
    private void prepare() {
        sqlExpressions = new SqlExpressions(tableSources, null, TEST_USER, new UsersScalarFunctionModule());
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
        assertThat(f.toString(), is("CURRENT_USER"));

        f = sqlExpressions.asSymbol("session_user");
        assertThat(f.toString(), is("SESSION_USER"));

        f = sqlExpressions.asSymbol("user");
        assertThat(f.toString(), is("CURRENT_USER"));
    }
}
