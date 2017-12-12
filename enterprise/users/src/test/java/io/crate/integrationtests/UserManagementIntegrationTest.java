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
import io.crate.testing.SQLResponse;
import io.crate.testing.TestingHelpers;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.After;
import org.junit.Test;

import static org.hamcrest.core.Is.is;

@ESIntegTestCase.ClusterScope(minNumDataNodes = 2)
public class UserManagementIntegrationTest extends BaseUsersIntegrationTest {

    private void assertUserIsCreated(String userName) throws Exception {
        SQLResponse response = executeAsSuperuser("select count(*) from sys.users where name = ?",
            new Object[]{userName});
        assertThat(response.rows()[0][0], is(1L));
    }

    private void assertUserDoesntExist(String userName) throws Exception {
        SQLResponse response = executeAsSuperuser("select count(*) from sys.users where name = ?",
            new Object[]{userName});
        assertThat(response.rows()[0][0], is(0L));
    }

    @After
    public void dropAllUsers() throws Exception {
        // clean all created users
        executeAsSuperuser("SELECT name FROM sys.users WHERE superuser = FALSE");
        for (Object[] objects : response.rows()) {
            String user = (String) objects[0];
            executeAsSuperuser("DROP user " + user);
        }
    }

    @Test
    public void testCreateUser() throws Exception {
        executeAsSuperuser("create user trillian");
        assertThat(response.rowCount(), is(1L));
        assertUserIsCreated("trillian");
    }

    @Test
    public void testSysUsersTableColumns() throws Exception {
        // The sys users table contains two columns, name and superuser
        executeAsSuperuser("select column_name, data_type from information_schema.columns where table_name='users' and table_schema='sys'");
        assertThat(TestingHelpers.printedTable(response.rows()), is("name| string\n" +
                                                                    "password| string\n" +
                                                                    "superuser| boolean\n"));
    }

    @Test
    public void testSysUsersTableDefaultUser() throws Exception {
        // The sys.users table always contains the superuser crate
        executeAsSuperuser("select name, password, superuser from sys.users where name = 'crate'");
        assertThat(TestingHelpers.printedTable(response.rows()), is("crate| NULL| true\n"));
    }

    @Test
    public void testSysUsersTable() throws Exception {
        executeAsSuperuser("CREATE USER arthur");
        assertUserIsCreated("arthur");
        executeAsSuperuser("CREATE USER ford WITH (password = 'foo')");
        assertUserIsCreated("ford");
        executeAsSuperuser("SELECT name, password, superuser FROM sys.users WHERE superuser = FALSE ORDER BY name");
        // Every created user is not a superuser
        assertThat(TestingHelpers.printedTable(response.rows()), is("arthur| NULL| false\n" +
                                                                    "ford| ********| false\n"));
    }

    @Test
    public void testCreateUserWithPasswordExpression() throws Exception {
        executeAsSuperuser("CREATE USER arthur WITH (password = substr(?, 3))", new Object[]{"**password"});
        assertUserIsCreated("arthur");
    }

    @Test
    public void testAlterUserPassword() throws Exception {
        executeAsSuperuser("CREATE USER arthur");
        assertUserIsCreated("arthur");
        executeAsSuperuser("CREATE USER ford WITH (password = ?)", new Object[]{"foo"});
        assertUserIsCreated("ford");
        executeAsSuperuser("ALTER USER arthur SET (password = ?)", new Object[]{"pass"});
        executeAsSuperuser("ALTER USER ford SET (password = ?)", new Object[]{"pass"});
        executeAsSuperuser("SELECT name, password, superuser FROM sys.users WHERE superuser = FALSE ORDER BY name");
        assertThat(TestingHelpers.printedTable(response.rows()), is("arthur| ********| false\n" +
                                                                    "ford| ********| false\n"));
    }

    @Test
    public void testAlterUserResetPassword() throws Exception {
        executeAsSuperuser("CREATE USER arthur WITH (password = 'foo')");
        assertUserIsCreated("arthur");
        executeAsSuperuser("ALTER USER arthur SET (password = null)");
        executeAsSuperuser("SELECT name, password, superuser FROM sys.users WHERE superuser = FALSE ORDER BY name");
        assertThat(TestingHelpers.printedTable(response.rows()), is("arthur| NULL| false\n"));
    }

    @Test
    public void testAlterNonExistingUserThrowsException() throws Exception {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("UserUnknownException: User 'unknown_user' does not exist");
        executeAsSuperuser("alter user unknown_user set (password = 'unknown')");
    }

    @Test
    public void testDropUser() throws Exception {
        executeAsSuperuser("create user ford");
        assertUserIsCreated("ford");
        executeAsSuperuser("drop user ford");
        assertUserDoesntExist("ford");
    }

    @Test
    public void testDropUserIfExists() throws Exception {
        executeAsSuperuser("drop user if exists not_exist_if_exists");
        assertThat(response.rowCount(), is(0L));
    }

    @Test
    public void testDropUserUnAuthorized() throws Exception {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("User \"normal\" is not authorized to execute statement");
        executeAsNormalUser("drop user ford");
    }

    @Test
    public void testCreateUserUnAuthorized() throws Exception {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("User \"normal\" is not authorized to execute statement");
        executeAsNormalUser("create user ford");
    }

    @Test
    public void testCreateNormalUserUnAuthorized() throws Exception {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("User \"normal\" is not authorized to execute statement");
        executeAsNormalUser("create user ford");
    }

    @Test
    public void testCreateExistingUserThrowsException() throws Exception {
        executeAsSuperuser("create user ford_exists");
        assertUserIsCreated("ford_exists");

        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("UserAlreadyExistsException: User 'ford_exists' already exists");
        executeAsSuperuser("create user ford_exists");
    }

    @Test
    public void testDropNonExistingUserThrowsException() throws Exception {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("UserUnknownException: User 'not_exists' does not exist");
        executeAsSuperuser("drop user not_exists");
    }

    @Test
    public void testDropSuperUserThrowsException() throws Exception {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("UnsupportedFeatureException: Cannot drop a superuser 'crate'");
        executeAsSuperuser("drop user crate");
    }
}
