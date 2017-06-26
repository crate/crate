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
import io.crate.testing.SQLResponse;
import io.crate.testing.TestingHelpers;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import static io.crate.testing.SQLTransportExecutor.DEFAULT_SOFT_LIMIT;
import static org.hamcrest.core.Is.is;

@ESIntegTestCase.ClusterScope(minNumDataNodes = 2)
public class UserManagementIntegrationTest extends SQLTransportIntegrationTest {

    private SQLOperations.Session createSuperUserSession() {
        SQLOperations sqlOperations = internalCluster().getInstance(SQLOperations.class);
        return sqlOperations.createSession(null, UserManagerService.CRATE_USER, Option.NONE, DEFAULT_SOFT_LIMIT);
    }

    private SQLOperations.Session createUserSession() {
        SQLOperations sqlOperations = internalCluster().getInstance(SQLOperations.class);
        return sqlOperations.createSession(null, new User("normal", ImmutableSet.of()), Option.NONE, DEFAULT_SOFT_LIMIT);
    }

    private void assertUserIsCreated(String userName) throws Exception {
        SQLResponse response = execute("select count(*) from sys.users where name = ?",
            new Object[]{userName},
            createSuperUserSession());
        assertThat(response.rows()[0][0], is(1L));
    }

    private void assertUserDoesntExist(String userName) throws Exception {
        SQLResponse response = execute("select count(*) from sys.users where name = ?",
            new Object[]{userName},
            createSuperUserSession());
        assertThat(response.rows()[0][0], is(0L));
    }

    @Test
    public void testCreateUser() throws Exception {
        execute("create user trillian", null, createSuperUserSession());
        assertThat(response.rowCount(), is(1L));
        assertUserIsCreated("trillian");
    }

    @Test
    public void testSysUsersTableColumns() throws Exception {
        // The sys users table contains two columns, name and superuser
        execute("select column_name, data_type from information_schema.columns where table_name='users' and table_schema='sys'");
        assertThat(TestingHelpers.printedTable(response.rows()), is("name| string\n" +
                                                                    "superuser| boolean\n"));
    }

    @Test
    public void testSysUsersTableDefaultUser() throws Exception {
        // The sys.users table always contains the superuser crate
        execute("select name, superuser from sys.users where name = 'crate'", null, createSuperUserSession());
        assertThat(TestingHelpers.printedTable(response.rows()), is("crate| true\n"));
    }

    @Test
    public void testSysUsersTable() throws Exception {
        execute("create user arthur", null, createSuperUserSession());
        assertUserIsCreated("arthur");
        execute("select name, superuser from sys.users order by name limit 1", null, createSuperUserSession());
        // Every created user is not a superuser
        assertThat(TestingHelpers.printedTable(response.rows()), is("arthur| false\n"));
    }

    @Test
    public void testDropUser() throws Exception {
        execute("create user ford", null, createSuperUserSession());
        assertUserIsCreated("ford");
        execute("drop user ford", null, createSuperUserSession());
        assertUserDoesntExist("ford");
    }

    @Test
    public void testDropUserIfExists() throws Exception {
        execute("drop user if exists not_exist_if_exists", null, createSuperUserSession());
        assertThat(response.rowCount(), is(0L));
    }

    @Test
    public void testDropUserUnAuthorized() throws Exception {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("User \"null\" is not authorized to execute statement");
        execute("drop user ford");
    }

    @Test
    public void testCreateUserUnAuthorized() throws Exception {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("User \"null\" is not authorized to execute statement");
        execute("create user ford");
    }

    @Test
    public void testCreateNormalUserUnAuthorized() throws Exception {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("User \"normal\" is not authorized to execute statement");
        execute("create user ford", null, createUserSession());
    }

    @Test
    public void testSelectUsersUnAuthorized() throws Exception {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage(
            "UnauthorizedException: User \"null\" is not authorized to access table \"sys.users\"");
        execute("select * from sys.users");
    }

    @Test
    public void testCreateExistingUserThrowsException() throws Exception {
        execute("create user ford_exists", null, createSuperUserSession());
        assertUserIsCreated("ford_exists");

        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("UserAlreadyExistsException: User 'ford_exists' already exists");
        execute("create user ford_exists", null, createSuperUserSession());
    }

    @Test
    public void testDropNonExistingUserThrowsException() throws Exception {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("UserUnknownException: User 'not_exists' does not exist");
        execute("drop user not_exists", null, createSuperUserSession());
    }

    @Test
    public void testDropSuperUserThrowsException() throws Exception {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("UnsupportedFeatureException: Cannot drop a superuser 'crate'");
        execute("drop user crate", null, createSuperUserSession());
    }
}
