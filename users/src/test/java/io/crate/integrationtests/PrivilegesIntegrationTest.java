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
import io.crate.action.sql.SQLActionException;
import io.crate.action.sql.SQLOperations;
import io.crate.operation.user.User;
import io.crate.operation.user.UserManagerService;
import io.crate.settings.SharedSettings;
import io.crate.testing.SQLResponse;
import org.elasticsearch.common.settings.Settings;
import org.junit.Before;
import org.junit.Test;

import static io.crate.testing.SQLTransportExecutor.DEFAULT_SOFT_LIMIT;
import static org.hamcrest.core.Is.is;

public class PrivilegesIntegrationTest extends SQLTransportIntegrationTest {

    private SQLOperations.Session superuserSession;

    private SQLOperations.Session createSuperUserSession() {
        SQLOperations sqlOperations = internalCluster().getInstance(SQLOperations.class);
        return sqlOperations.createSession(null, UserManagerService.CRATE_USER, Option.NONE,
            DEFAULT_SOFT_LIMIT);
    }

    private SQLOperations.Session createUserSession() {
        SQLOperations sqlOperations = internalCluster().getInstance(SQLOperations.class);
        return sqlOperations.createSession(null, new User("normal", ImmutableSet.of(),
            ImmutableSet.of()), Option.NONE, DEFAULT_SOFT_LIMIT);
    }

    private void assertPrivilegeIsGranted(String privilege, String userName) throws Exception {
        assertBusy(() -> {
            SQLResponse response = execute("select count(*) from sys.privileges where grantee = ? and type = ?",
                new Object[]{userName, privilege},
                superuserSession);
            assertThat(response.rows()[0][0], is(1L));
        });
    }

    private void assertPrivilegeIsRevoked(String privilege, String userName) throws Exception {
        assertBusy(() -> {
            SQLResponse response = execute("select count(*) from sys.privileges where grantee = ? and type = ?",
                new Object[]{userName, privilege},
                superuserSession);
            assertThat(response.rows()[0][0], is(0L));
        });
    }

    @Before
    public void setupSuperuserSession() {
        superuserSession = createSuperUserSession();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Settings.Builder builder = Settings.builder()
            .put(super.nodeSettings(nodeOrdinal));
        builder.put(SharedSettings.ENTERPRISE_LICENSE_SETTING.getKey(), true);
        return builder.build();
    }

    @Test
    public void testNormalUserGrantsPrivilegeThrowsException() throws Exception {
        execute("create user joey", null, superuserSession);

        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("UnauthorizedException: User \"normal\" is not authorized to execute statement");
        execute("grant DQL to joey", null, createUserSession());
    }

    @Test
    public void testSuperUserGrantsPrivilege() throws Exception {
        execute("create user chandler", null, superuserSession);

        execute("grant DQL to chandler", null, superuserSession);
        assertPrivilegeIsGranted("DQL", "chandler");
    }

    @Test
    public void testGrantRevokeALLPrivileges() throws Exception {
        execute("create user phoebe", null, superuserSession);

        execute("grant ALL to phoebe", null, superuserSession);
        assertPrivilegeIsGranted("DQL", "phoebe");
        assertPrivilegeIsGranted("DML", "phoebe");
        assertPrivilegeIsGranted("DDL", "phoebe");

        execute("revoke ALL from phoebe", null, superuserSession);
        assertPrivilegeIsRevoked("DQL", "phoebe");
        assertPrivilegeIsRevoked("DML", "phoebe");
        assertPrivilegeIsRevoked("DDL", "phoebe");
    }

    @Test
    public void testSuperUserRevokesPrivilege() throws Exception {
        execute("create user ross", null, superuserSession);

        execute("grant DQL to ross", null, superuserSession);
        assertThat(response.rowCount(), is(1L));

        execute("revoke DQL from ross", null, superuserSession);
        assertThat(response.rowCount(), is(1L));

        assertPrivilegeIsRevoked("DQL", "ross");
    }

    @Test
    public void testGrantPrivilegeToSuperuserThrowsException() {
        String superuserName = UserManagerService.CRATE_USER.name();
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("SQLParseException: Cannot alter privileges for superuser " +
                                        superuserName);
        execute("grant DQL to " + superuserName, null, superuserSession);
    }
}
