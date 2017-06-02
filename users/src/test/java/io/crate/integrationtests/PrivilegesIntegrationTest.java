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

import io.crate.action.sql.SQLActionException;
import io.crate.operation.user.UserManagerService;
import io.crate.testing.SQLResponse;
import org.junit.Test;

import static org.hamcrest.core.Is.is;

public class PrivilegesIntegrationTest extends BaseUsersIntegrationTest {

    private void assertPrivilegeIsGranted(String privilege, String userName) throws Exception {
        SQLResponse response = executeAsSuperuser("select count(*) from sys.privileges where grantee = ? and type = ?",
            new Object[]{userName, privilege});
        assertThat(response.rows()[0][0], is(1L));
    }

    private void assertPrivilegeIsRevoked(String privilege, String userName) throws Exception {
        SQLResponse response = executeAsSuperuser("select count(*) from sys.privileges where grantee = ? and type = ?",
            new Object[]{userName, privilege});
        assertThat(response.rows()[0][0], is(0L));
    }

    @Test
    public void testNormalUserGrantsPrivilegeThrowsException() throws Exception {
        executeAsSuperuser("create user joey");

        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("UnauthorizedException: User \"normal\" is not authorized to execute statement");
        executeAsNormalUser("grant DQL to joey");
    }

    @Test
    public void testNewUserHasNoPrivilegesByDefault() throws Exception {
        executeAsSuperuser("create user ford");
        executeAsSuperuser("select * from sys.privileges where grantee = ?", new Object[]{"ford"});
        assertThat(response.rowCount(), is(0L));
    }

    @Test
    public void testSuperUserGrantsPrivilege() throws Exception {
        executeAsSuperuser("create user chandler");

        executeAsSuperuser("grant DQL to chandler");
        assertPrivilegeIsGranted("DQL", "chandler");
    }

    @Test
    public void testGrantRevokeALLPrivileges() throws Exception {
        executeAsSuperuser("create user phoebe");

        executeAsSuperuser("grant ALL to phoebe");
        assertPrivilegeIsGranted("DQL", "phoebe");
        assertPrivilegeIsGranted("DML", "phoebe");
        assertPrivilegeIsGranted("DDL", "phoebe");

        executeAsSuperuser("revoke ALL from phoebe");
        assertPrivilegeIsRevoked("DQL", "phoebe");
        assertPrivilegeIsRevoked("DML", "phoebe");
        assertPrivilegeIsRevoked("DDL", "phoebe");
    }

    @Test
    public void testSuperUserRevokesPrivilege() throws Exception {
        executeAsSuperuser("create user ross");

        executeAsSuperuser("grant DQL to ross");
        assertThat(response.rowCount(), is(1L));

        executeAsSuperuser("revoke DQL from ross");
        assertThat(response.rowCount(), is(1L));

        assertPrivilegeIsRevoked("DQL", "ross");
    }

    @Test
    public void testGrantPrivilegeToSuperuserThrowsException() {
        String superuserName = UserManagerService.CRATE_USER.name();
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("UnsupportedFeatureException: Cannot alter privileges for superuser '" +
                                        superuserName + "'");
        executeAsSuperuser("grant DQL to " + superuserName);
    }

    @Test
    public void testApplyPrivilegesToUnknownUserThrowsException() throws Exception {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("UserUnknownException: User 'unknown_user' does not exist");
        executeAsSuperuser("grant DQL to unknown_user");
    }

    @Test
    public void testApplyPrivilegesToMultipleUnknownUsersThrowsException() throws Exception {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("UserUnknownException: Users 'unknown_user, also_unknown' do not exist");
        executeAsSuperuser("grant DQL to unknown_user, also_unknown");
    }
}
