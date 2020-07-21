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
import org.junit.Before;
import org.junit.Test;

import static io.crate.testing.SymbolMatchers.isLiteral;
import static org.hamcrest.Matchers.is;

public class UserFunctionTest extends AbstractScalarFunctionsTest {

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
