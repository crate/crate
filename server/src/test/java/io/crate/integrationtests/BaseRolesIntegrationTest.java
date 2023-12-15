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

package io.crate.integrationtests;

import static io.crate.testing.Asserts.assertThat;

import java.util.Objects;

import org.elasticsearch.test.IntegTestCase;
import org.junit.After;
import org.junit.Before;

import io.crate.action.sql.Session;
import io.crate.action.sql.Sessions;
import io.crate.role.Role;
import io.crate.role.Roles;
import io.crate.role.metadata.RolesHelper;
import io.crate.testing.SQLResponse;

public abstract class BaseRolesIntegrationTest extends IntegTestCase {

    private Session superUserSession;
    private Session normalUserSession;

    protected Session createSuperUserSession() {
        Sessions sqlOperations = cluster().getInstance(Sessions.class);
        return sqlOperations.newSession(null, Role.CRATE_USER);
    }

    private Session createUserSession() {
        Sessions sqlOperations = cluster().getInstance(Sessions.class);
        return sqlOperations.newSession(null, RolesHelper.userOf("normal"));
    }

    @Before
    public void setUpSessions() {
        superUserSession = createSuperUserSession();
        normalUserSession = createUserSession();
    }

    @After
    public void closeSessions() {
        superUserSession.close();
        normalUserSession.close();
    }

    @After
    public void dropAllUsersAndRoles() {
        // clean all created users
        executeAsSuperuser("SELECT name FROM sys.users WHERE superuser = FALSE");
        for (Object[] objects : response.rows()) {
            String user = (String) objects[0];
            executeAsSuperuser("DROP USER " + user);
        }
        // clean all created roles
        executeAsSuperuser("SELECT name FROM sys.roles");
        for (Object[] objects : response.rows()) {
            String role = (String) objects[0];
            executeAsSuperuser("DROP ROLE " + role);
        }
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
        Sessions sqlOperations = cluster().getInstance(Sessions.class);
        Roles roles = cluster().getInstance(Roles.class);
        Role user = Objects.requireNonNull(roles.findUser(userName), "User " + userName + " must exist");
        try (Session session = sqlOperations.newSession(null, user)) {
            return execute(stmt, null, session);
        }
    }

    protected void assertUserIsCreated(String userName) {
        SQLResponse response = executeAsSuperuser("select count(*) from sys.users where name = ?",
            new Object[]{userName});
        assertThat(response).hasRows("1");
    }

    protected void assertRoleIsCreated(String roleName) {
        SQLResponse response = executeAsSuperuser("select count(*) from sys.roles where name = ?",
            new Object[]{roleName});
        assertThat(response).hasRows("1");
    }

    protected void assertUserDoesntExist(String userName) {
        SQLResponse response = executeAsSuperuser("select count(*) from sys.users where name = ?",
            new Object[]{userName});
        assertThat(response).hasRows("0");
    }
}
