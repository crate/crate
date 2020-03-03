/*
 * This file is part of a module with proprietary Enterprise Features.
 *
 * Licensed to Crate.io Inc. ("Crate.io") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 *
 * To use this file, Crate.io must have given you permission to enable and
 * use such Enterprise Features and you must have a valid Enterprise or
 * Subscription Agreement with Crate.io.  If you enable or use the Enterprise
 * Features, you represent and warrant that you have a valid Enterprise or
 * Subscription Agreement with Crate.io.  Your use of the Enterprise Features
 * if governed by the terms and conditions of your Enterprise or Subscription
 * Agreement with Crate.io.
 */

package io.crate.scalar.systeminformation;

import io.crate.auth.user.User;
import io.crate.expression.scalar.AbstractScalarFunctionsTest;
import io.crate.expression.symbol.Symbol;
import io.crate.scalar.UsersScalarFunctionModule;
import io.crate.testing.SqlExpressions;
import org.junit.Test;

import static io.crate.testing.SymbolMatchers.isLiteral;
import static org.hamcrest.Matchers.is;

public class UserFunctionTest extends AbstractScalarFunctionsTest {

    private static final User TEST_USER = User.of("testUser");

    private void setupFunctionsFor(User user) {
        sqlExpressions = new SqlExpressions(tableSources, null, null, user,
            new UsersScalarFunctionModule());
        functions = sqlExpressions.functions();
    }

    @Test
    public void testNormalizeCurrentUser() {
        setupFunctionsFor(TEST_USER);
        assertNormalize("current_user", isLiteral("testUser"), false);
    }

    @Test
    public void testNormalizeSessionUser() {
        setupFunctionsFor(TEST_USER);
        assertNormalize("session_user", isLiteral("testUser"), false);
    }

    @Test
    public void testNormalizeUser() {
        setupFunctionsFor(TEST_USER);
        assertNormalize("user", isLiteral("testUser"), false);
    }

    @Test
    public void testFormatFunctionsWithoutBrackets() {
        setupFunctionsFor(TEST_USER);
        Symbol f = sqlExpressions.asSymbol("current_user");
        assertThat(f.toString(), is("CURRENT_USER"));

        f = sqlExpressions.asSymbol("session_user");
        assertThat(f.toString(), is("SESSION_USER"));

        f = sqlExpressions.asSymbol("user");
        assertThat(f.toString(), is("CURRENT_USER"));
    }
}
