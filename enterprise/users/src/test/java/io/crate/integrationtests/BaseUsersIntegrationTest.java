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

package io.crate.integrationtests;

import io.crate.action.sql.Option;
import io.crate.action.sql.SQLOperations;
import io.crate.action.sql.Session;
import io.crate.auth.user.User;
import io.crate.auth.user.UserManager;
import io.crate.testing.SQLResponse;
import org.junit.Before;

import java.util.Objects;

public abstract class BaseUsersIntegrationTest extends SQLTransportIntegrationTest {

    private Session superUserSession;
    private Session normalUserSession;

    private Session createSuperUserSession() {
        SQLOperations sqlOperations = internalCluster().getInstance(SQLOperations.class);
        return sqlOperations.createSession(null, User.CRATE_USER, Option.NONE);
    }

    private Session createUserSession() {
        SQLOperations sqlOperations = internalCluster().getInstance(SQLOperations.class);
        return sqlOperations.createSession(null, User.of("normal"), Option.NONE);
    }

    @Before
    public void setUpSessions() {
        createSessions();
    }

    public void createSessions() {
        superUserSession = createSuperUserSession();
        normalUserSession = createUserSession();
    }

    public SQLResponse executeAsSuperuser(String stmt) {
        return executeAsSuperuser(stmt, null);
    }

    public SQLResponse executeAsSuperuser(String stmt, Object[] args) {
        return execute(stmt, args, superUserSession);
    }

    public SQLResponse executeAsNormalUser(String stmt) {
        return execute(stmt, null, normalUserSession);
    }

    public SQLResponse executeAs(String stmt, String userName) {
        SQLOperations sqlOperations = internalCluster().getInstance(SQLOperations.class);
        UserManager userManager = internalCluster().getInstance(UserManager.class);
        User user = Objects.requireNonNull(userManager.findUser(userName), "User " + userName + " must exist");
        Session session = sqlOperations.createSession(null, user, Option.NONE);
        return execute(stmt, null, session);
    }
}
