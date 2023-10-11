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

import java.util.Objects;

import org.elasticsearch.test.IntegTestCase;
import org.junit.After;
import org.junit.Before;

import io.crate.action.sql.Session;
import io.crate.action.sql.Sessions;
import io.crate.testing.SQLResponse;
import io.crate.user.User;
import io.crate.user.UserLookup;

public abstract class BaseUsersIntegrationTest extends IntegTestCase {

    private Session superUserSession;
    private Session normalUserSession;

    protected Session createSuperUserSession() {
        Sessions sqlOperations = cluster().getInstance(Sessions.class);
        return sqlOperations.newSession(null, User.CRATE_USER);
    }

    private Session createUserSession() {
        Sessions sqlOperations = cluster().getInstance(Sessions.class);
        return sqlOperations.newSession(null, User.of("normal"));
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
        UserLookup userLookup = cluster().getInstance(UserLookup.class);
        User user = Objects.requireNonNull(userLookup.findUser(userName), "User " + userName + " must exist");
        try (Session session = sqlOperations.newSession(null, user)) {
            return execute(stmt, null, session);
        }
    }
}
