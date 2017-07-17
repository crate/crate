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
import io.crate.action.sql.SQLOperations;
import io.crate.operation.udf.UserDefinedFunctionService;
import io.crate.operation.user.User;
import io.crate.operation.user.UserManager;
import io.crate.operation.user.UserManagerService;
import io.crate.testing.SQLResponse;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static io.crate.testing.TestingHelpers.printedTable;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.core.Is.is;

public class PrivilegesIntegrationTest extends BaseUsersIntegrationTest {

    private static final String TEST_USERNAME = "privileges_test_user";

    private final UserDefinedFunctionsIntegrationTest.DummyLang dummyLang = new UserDefinedFunctionsIntegrationTest.DummyLang();
    private SQLOperations sqlOperations;
    private UserManager userManager;

    private void assertPrivilegeIsGranted(String privilege) throws Exception {
        SQLResponse response = executeAsSuperuser("select count(*) from sys.privileges where grantee = ? and type = ?",
            new Object[]{TEST_USERNAME, privilege});
        assertThat(response.rows()[0][0], is(1L));
    }

    private void assertPrivilegeIsRevoked(String privilege) throws Exception {
        SQLResponse response = executeAsSuperuser("select count(*) from sys.privileges where grantee = ? and type = ?",
            new Object[]{TEST_USERNAME, privilege});
        assertThat(response.rows()[0][0], is(0L));
    }

    private SQLOperations.Session testUserSession() {
        User user = userManager.findUser(TEST_USERNAME);
        assertThat(user, notNullValue());
        return sqlOperations.createSession(null, user);
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        Iterable<UserDefinedFunctionService> udfServices = internalCluster().getInstances(UserDefinedFunctionService.class);
        for (UserDefinedFunctionService udfService : udfServices) {
            udfService.registerLanguage(dummyLang);
        }
        userManager = internalCluster().getInstance(UserManager.class);
        sqlOperations = internalCluster().getInstance(SQLOperations.class, null);
        executeAsSuperuser("create user " + TEST_USERNAME);
    }

    @After
    public void dropUsers() {
        executeAsSuperuser("drop user " + TEST_USERNAME);
    }

    @Test
    public void testNormalUserGrantsPrivilegeThrowsException() throws Exception {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("UnauthorizedException: User \"normal\" is not authorized to execute statement");
        executeAsNormalUser("grant DQL to " + TEST_USERNAME);
    }

    @Test
    public void testNewUserHasNoPrivilegesByDefault() throws Exception {
        executeAsSuperuser("select * from sys.privileges where grantee = ?", new Object[]{TEST_USERNAME});
        assertThat(response.rowCount(), is(0L));
    }

    @Test
    public void testSuperUserGrantsPrivilege() throws Exception {
        executeAsSuperuser("grant DQL to " + TEST_USERNAME);
        assertPrivilegeIsGranted("DQL");
    }

    @Test
    public void testGrantRevokeALLPrivileges() throws Exception {
        executeAsSuperuser("grant ALL to " + TEST_USERNAME);
        assertPrivilegeIsGranted("DQL");
        assertPrivilegeIsGranted("DML");
        assertPrivilegeIsGranted("DDL");

        executeAsSuperuser("revoke ALL from " + TEST_USERNAME);
        assertPrivilegeIsRevoked("DQL");
        assertPrivilegeIsRevoked("DML");
        assertPrivilegeIsRevoked("DDL");
    }

    @Test
    public void testSuperUserRevokesPrivilege() throws Exception {
        executeAsSuperuser("grant DQL to " + TEST_USERNAME);
        assertThat(response.rowCount(), is(1L));

        executeAsSuperuser("revoke DQL from " + TEST_USERNAME);
        assertThat(response.rowCount(), is(1L));

        assertPrivilegeIsRevoked("DQL");
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

    @Test
    public void testQuerySysShardsReturnsOnlyRowsRegardingTablesUserHasAccessOn() throws Exception {
        execute("create table t1 (x int) partitioned by (x) clustered into 1 shards with (number_of_replicas = 0)");
        execute("insert into t1 values (1)");
        execute("insert into t1 values (2)");
        execute("insert into t1 values (3)");
        execute("create table doc.t2 (x int) clustered into 1 shards with (number_of_replicas = 0)");
        execute("create table t3 (x int) clustered into 1 shards with (number_of_replicas = 0)");

        executeAsSuperuser("grant dql on table t1 to " + TEST_USERNAME);
        executeAsSuperuser("grant dml on table t2 to " + TEST_USERNAME);
        executeAsSuperuser("grant dql on table sys.shards to " + TEST_USERNAME);

        execute("select table_name from sys.shards order by table_name", null, testUserSession());
        assertThat(response.rowCount(), is(4L));
        assertThat(response.rows()[0][0], is("t1"));
        assertThat(response.rows()[1][0], is("t1"));
        assertThat(response.rows()[2][0], is("t1"));
        assertThat(response.rows()[3][0], is("t2"));
    }

    @Test
    public void testQueryInformationSchemaShowsOnlyRowsRegardingTablesUserHasAccessOn() throws Exception {
        execute("create table t1 (x int) partitioned by (x) clustered into 1 shards with (number_of_replicas = 0)");
        execute("insert into t1 values (1)");
        execute("insert into t1 values (2)");
        execute("create table my_schema.t2 (x int) clustered into 1 shards with (number_of_replicas = 0)");
        execute("create table other_schema.t3 (x int) clustered into 1 shards with (number_of_replicas = 0)");

        execute("create function my_schema.foo(long)" +
                " returns string language dummy_lang as 'function foo(x) { return \"1\"; }'");
        execute("create function other_func(long)" +
                " returns string language dummy_lang as 'function foo(x) { return \"1\"; }'");

        executeAsSuperuser("grant dql on table t1 to " + TEST_USERNAME);
        executeAsSuperuser("grant dml on table my_schema.t2 to " + TEST_USERNAME);
        executeAsSuperuser("grant dql on schema my_schema to " + TEST_USERNAME);

        execute("select schema_name from information_schema.schemata order by schema_name", null, testUserSession());
        assertThat(printedTable(response.rows()), is("my_schema\n"));
        execute("select table_name from information_schema.tables order by table_name", null, testUserSession());
        assertThat(printedTable(response.rows()), is("t1\n" +
                                                     "t2\n"));
        execute("select table_name from information_schema.table_partitions order by table_name", null, testUserSession());
        assertThat(printedTable(response.rows()), is("t1\n" +
                                                     "t1\n"));
        execute("select table_name from information_schema.columns order by table_name", null, testUserSession());
        assertThat(printedTable(response.rows()), is("t1\n" +
                                                     "t2\n"));
        execute("select table_name from information_schema.table_constraints order by table_name", null, testUserSession());
        assertThat(printedTable(response.rows()), is("t2\n"));
        execute("select routine_schema from information_schema.routines order by routine_schema", null, testUserSession());
        assertThat(printedTable(response.rows()), is("my_schema\n"));
    }

    @Test
    public void testRenameTableTransfersPrivilegesToNewTable() {
        execute("create table doc.t1 (x int) clustered into 1 shards with (number_of_replicas = 0)");
        executeAsSuperuser("grant dql on table t1 to "+ TEST_USERNAME);

        executeAsSuperuser("alter table doc.t1 rename to t1_renamed");
        ensureYellow();

        execute("select * from t1_renamed", null, testUserSession());
        assertThat(response.rowCount(), is(0L));
    }

    @Test
    public void testRenamePartitionedTableTransfersPrivilegesToNewTable() {
        executeAsSuperuser("create table t1 (x int) partitioned by (x) clustered into 1 shards with (number_of_replicas = 0)");
        executeAsSuperuser("insert into t1 values (1)");
        executeAsSuperuser("grant dql on table t1 to " + TEST_USERNAME);

        executeAsSuperuser("alter table doc.t1 rename to t1_renamed");
        ensureYellow();

        execute("select * from t1_renamed", null, testUserSession());
        assertThat(response.rowCount(), is(1L));
    }

    @Test
    public void testDropTableRemovesPrivileges() {
        executeAsSuperuser("create table doc.t1 (x int) clustered into 1 shards with (number_of_replicas = 0)");
        executeAsSuperuser("grant dql on table t1 to "+ TEST_USERNAME);

        executeAsSuperuser("drop table t1");
        ensureYellow();

        executeAsSuperuser("create table doc.t1 (x int) clustered into 1 shards with (number_of_replicas = 0)");
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage(containsString("Missing 'DQL' privilege for user '"+ TEST_USERNAME + "'"));
        execute("select * from t1", null, testUserSession());
    }
}
