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

import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.format.SymbolPrinter;
import io.crate.operation.scalar.AbstractScalarFunctionsTest;
import io.crate.operation.user.User;
import io.crate.scalar.UsersScalarFunctionModule;
import io.crate.testing.SqlExpressions;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.Collections;

import static io.crate.testing.SymbolMatchers.isLiteral;
import static org.hamcrest.Matchers.is;

public class UserFunctionTest extends AbstractScalarFunctionsTest {

    private static final User TEST_USER = new User("testUser", Collections.emptySet(), Collections.emptySet());

    private void setupFunctionsFor(@Nullable User user) {
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
    public void testCurrentUserForMissingUserReturnsNull() {
        setupFunctionsFor(null);
        assertNormalize("current_user", isLiteral(null), false);
    }

    @Test
    public void testUserForMissingUserReturnsNull() {
        setupFunctionsFor(null);
        assertNormalize("user", isLiteral(null), false);
    }

    @Test
    public void testSessionUserForMissingUserReturnsNull() {
        setupFunctionsFor(null);
        assertNormalize("session_user", isLiteral(null), false);
    }

    @Test
    public void testFormatFunctionsWithoutBrackets() {
        setupFunctionsFor(TEST_USER);
        SymbolPrinter printer = new SymbolPrinter(sqlExpressions.functions());
        Symbol f = sqlExpressions.asSymbol("current_user");
        assertThat(printer.printFullQualified(f), is("current_user"));

        f = sqlExpressions.asSymbol("session_user");
        assertThat(printer.printFullQualified(f), is("session_user"));

        f = sqlExpressions.asSymbol("user");
        assertThat(printer.printFullQualified(f), is("current_user"));
    }
}
