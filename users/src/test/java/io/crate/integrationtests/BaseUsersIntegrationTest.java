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

import com.google.common.collect.ImmutableSet;
import io.crate.action.sql.Option;
import io.crate.action.sql.SQLOperations;
import io.crate.analyze.user.Privilege;
import io.crate.operation.user.User;
import io.crate.operation.user.UserManagerService;
import io.crate.testing.SQLResponse;
import org.junit.Before;

import static io.crate.testing.SQLTransportExecutor.DEFAULT_SOFT_LIMIT;

public abstract class BaseUsersIntegrationTest extends SQLTransportIntegrationTest {

    private SQLOperations.Session superUserSession;
    private SQLOperations.Session normalUserSession;
    private SQLOperations.Session ddlUserSession;
    private SQLOperations.Session dmlUserSession;
    private SQLOperations.Session dqlUserSession;

    protected SQLOperations.Session createSuperUserSession() {
        return createSuperUserSession(null);
    }

    SQLOperations.Session createSuperUserSession(String node) {
        SQLOperations sqlOperations = internalCluster().getInstance(SQLOperations.class, node);
        return sqlOperations.createSession(null, UserManagerService.CRATE_USER, Option.NONE, DEFAULT_SOFT_LIMIT);
    }

    protected SQLOperations.Session createUserSession() {
        return createUserSession(null);
    }
    protected SQLOperations.Session createDDLUserSession() {
        return createDDLUserSession(null);
    }
    protected SQLOperations.Session createDMLUserSession() {
        return createDMLUserSession(null);
    }
    protected SQLOperations.Session createDQLUserSession() {
        return createDQLUserSession(null);
    }

    SQLOperations.Session createUserSession(String node) {
        SQLOperations sqlOperations = internalCluster().getInstance(SQLOperations.class, node);
        return sqlOperations.createSession(null, new User("normal", ImmutableSet.of(), ImmutableSet.of()), Option.NONE, DEFAULT_SOFT_LIMIT);
    }


    SQLOperations.Session createDDLUserSession(String node) {
        SQLOperations sqlOperations = internalCluster().getInstance(SQLOperations.class, node);
        return sqlOperations.createSession(null, new User("ddlUser", ImmutableSet.of(), ImmutableSet.of(new Privilege(Privilege.State.GRANT, Privilege.Type.DDL, Privilege.Clazz.CLUSTER, null, "crate"))), Option.NONE, DEFAULT_SOFT_LIMIT);
    }

    SQLOperations.Session createDMLUserSession(String node) {
        SQLOperations sqlOperations = internalCluster().getInstance(SQLOperations.class, node);
        return sqlOperations.createSession(null, new User("dmlUser", ImmutableSet.of(), ImmutableSet.of(new Privilege(Privilege.State.GRANT, Privilege.Type.DML, Privilege.Clazz.CLUSTER, null, "crate"))), Option.NONE, DEFAULT_SOFT_LIMIT);
    }

    SQLOperations.Session createDQLUserSession(String node) {
        SQLOperations sqlOperations = internalCluster().getInstance(SQLOperations.class, node);
        return sqlOperations.createSession(null, new User("dqlUser", ImmutableSet.of(), ImmutableSet.of(new Privilege(Privilege.State.GRANT, Privilege.Type.DQL, Privilege.Clazz.CLUSTER, null, "crate"))), Option.NONE, DEFAULT_SOFT_LIMIT);
    }

    @Before
    public void setUpSessions() {
        createSessions();
    }

    public void createSessions() {
        superUserSession = createSuperUserSession();
        normalUserSession = createUserSession();

        ddlUserSession = createDDLUserSession();
        dmlUserSession = createDMLUserSession();
        dqlUserSession = createDQLUserSession();

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
    public SQLResponse executeAsDDLUser(String stmt) {
        return execute(stmt, null, ddlUserSession);
    }
    public SQLResponse executeAsDQLUser(String stmt) {
        return execute(stmt, null, dqlUserSession);
    }
    public SQLResponse executeAsDMLUser(String stmt) {
        return execute(stmt, null, dmlUserSession);
    }
}
