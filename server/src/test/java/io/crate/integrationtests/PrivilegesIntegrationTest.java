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

import io.crate.action.sql.SQLOperations;
import io.crate.action.sql.Session;
import io.crate.expression.udf.UserDefinedFunctionService;
import io.crate.testing.SQLResponse;
import io.crate.user.User;
import io.crate.user.UserLookup;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static io.crate.protocols.postgres.PGErrorStatus.INTERNAL_ERROR;
import static io.crate.testing.Asserts.assertThrowsMatches;
import static io.crate.testing.SQLErrorMatcher.isSQLError;
import static io.crate.testing.TestingHelpers.printedTable;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.UNAUTHORIZED;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.core.Is.is;

public class PrivilegesIntegrationTest extends BaseUsersIntegrationTest {

    private static final String TEST_USERNAME = "privileges_test_user";

    private final UserDefinedFunctionsIntegrationTest.DummyLang dummyLang = new UserDefinedFunctionsIntegrationTest.DummyLang();
    private SQLOperations sqlOperations;
    private UserLookup userLookup;

    private void assertPrivilegeIsGranted(String privilege) {
        SQLResponse response = executeAsSuperuser("select count(*) from sys.privileges where grantee = ? and type = ?",
            new Object[]{TEST_USERNAME, privilege});
        assertThat(response.rows()[0][0], is(1L));
    }

    private void assertPrivilegeIsRevoked(String privilege) {
        SQLResponse response = executeAsSuperuser("select count(*) from sys.privileges where grantee = ? and type = ?",
            new Object[]{TEST_USERNAME, privilege});
        assertThat(response.rows()[0][0], is(0L));
    }

    private Session testUserSession() {
        return testUserSession(null);
    }

    private Session testUserSession(String defaultSchema) {
        User user = userLookup.findUser(TEST_USERNAME);
        assertThat(user, notNullValue());
        return sqlOperations.createSession(defaultSchema, user);
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        Iterable<UserDefinedFunctionService> udfServices = internalCluster().getInstances(UserDefinedFunctionService.class);
        for (UserDefinedFunctionService udfService : udfServices) {
            udfService.registerLanguage(dummyLang);
        }
        userLookup = internalCluster().getInstance(UserLookup.class);
        sqlOperations = internalCluster().getInstance(SQLOperations.class, null);
        executeAsSuperuser("create user " + TEST_USERNAME);
    }

    @After
    public void dropUsers() {
        executeAsSuperuser("drop user " + TEST_USERNAME);
        executeAsSuperuser("drop view if exists v1, my_schema.v2, other_schema.v3");
    }

    @Test
    public void testNormalUserGrantsPrivilegeThrowsException() {
        assertThrowsMatches(() -> executeAsNormalUser("grant DQL to " + TEST_USERNAME),
                     isSQLError(is("Missing 'AL' privilege for user 'normal'"),
                                INTERNAL_ERROR,
                                UNAUTHORIZED,
                                4011));
    }

    @Test
    public void testNewUserHasNoPrivilegesByDefault() {
        executeAsSuperuser("select * from sys.privileges where grantee = ?", new Object[]{TEST_USERNAME});
        assertThat(response.rowCount(), is(0L));
    }

    @Test
    public void testSuperUserGrantsPrivilege() {
        executeAsSuperuser("grant DQL to " + TEST_USERNAME);
        assertPrivilegeIsGranted("DQL");
    }

    @Test
    public void testGrantRevokeALLPrivileges() {
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
    public void testSuperUserRevokesPrivilege() {
        executeAsSuperuser("grant DQL to " + TEST_USERNAME);
        assertThat(response.rowCount(), is(1L));

        executeAsSuperuser("revoke DQL from " + TEST_USERNAME);
        assertThat(response.rowCount(), is(1L));

        assertPrivilegeIsRevoked("DQL");
    }

    @Test
    public void testGrantPrivilegeToSuperuserThrowsException() {
        String superuserName = User.CRATE_USER.name();
        assertThrowsMatches(() -> executeAsSuperuser("grant DQL to " + superuserName),
                     isSQLError(is("Cannot alter privileges for superuser '" + superuserName + "'"),
                                INTERNAL_ERROR,
                                BAD_REQUEST,
                                4004));
    }

    @Test
    public void testApplyPrivilegesToUnknownUserThrowsException() {
        assertThrowsMatches(() -> executeAsSuperuser("grant DQL to unknown_user"),
                     isSQLError(is("User 'unknown_user' does not exist"),
                                INTERNAL_ERROR,
                                NOT_FOUND,
                                40410));
    }

    @Test
    public void testApplyPrivilegesToMultipleUnknownUsersThrowsException() {
        assertThrowsMatches(() -> executeAsSuperuser("grant DQL to unknown_user, also_unknown"),
                     isSQLError(is("Users 'unknown_user, also_unknown' do not exist"),
                                INTERNAL_ERROR,
                                NOT_FOUND,
                                40410));
    }

    @Test
    public void testQuerySysShardsReturnsOnlyRowsRegardingTablesUserHasAccessOn() {
        executeAsSuperuser("create table t1 (x int) partitioned by (x) clustered into 1 shards with (number_of_replicas = 0)");
        executeAsSuperuser("insert into t1 values (1)");
        executeAsSuperuser("insert into t1 values (2)");
        executeAsSuperuser("insert into t1 values (3)");
        executeAsSuperuser("create table doc.t2 (x int) clustered into 1 shards with (number_of_replicas = 0)");
        executeAsSuperuser("create table t3 (x int) clustered into 1 shards with (number_of_replicas = 0)");

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
    public void testQuerySysJobsLogReturnsLogEntriesRelevantToUser() {
        executeAsSuperuser("grant dql on table sys.jobs_log to " + TEST_USERNAME);
        executeAsSuperuser("select 2");
        executeAsSuperuser("select 3");
        execute("select 1", null, testUserSession());

        String stmt = "select username, stmt " +
                      "from sys.jobs_log " +
                      "where stmt in ('select 1', 'select 2', 'select 3') " +
                      "order by stmt";

        executeAsSuperuser(stmt);
        assertThat(printedTable(response.rows()),
                   is("privileges_test_user| select 1\n" +
                      "crate| select 2\n" +
                      "crate| select 3\n"));

        execute(stmt, null, testUserSession());
        assertThat(printedTable(response.rows()), is("privileges_test_user| select 1\n"));
    }

    @Test
    public void testQuerySysAllocationsReturnsOnlyRowsRegardingTablesUserHasAccessOn() {
        executeAsSuperuser("CREATE TABLE su.t1 (i INTEGER) CLUSTERED INTO 1 SHARDS WITH (number_of_replicas = 0)");
        executeAsSuperuser("CREATE TABLE u.t1 (i INTEGER) CLUSTERED INTO 1 SHARDS WITH (number_of_replicas = 0)");

        executeAsSuperuser("GRANT DQL ON SCHEMA u TO " + TEST_USERNAME);
        executeAsSuperuser("GRANT DQL ON TABLE sys.allocations TO " + TEST_USERNAME);

        execute("SELECT table_schema, table_name, shard_id FROM sys.allocations", null, testUserSession());
        assertThat(response.rowCount(), is(1L));
        assertThat(response.rows()[0][0], is("u"));
        assertThat(response.rows()[0][1], is("t1"));
        assertThat(response.rows()[0][2], is(0));
    }

    @Test
    public void testQuerySysHealthReturnsOnlyRowsRegardingTablesUserHasAccessOn() {
        executeAsSuperuser("CREATE TABLE su.t1 (i INTEGER) CLUSTERED INTO 1 SHARDS WITH (number_of_replicas = 0)");
        executeAsSuperuser("CREATE TABLE u.t1 (i INTEGER) CLUSTERED INTO 1 SHARDS WITH (number_of_replicas = 0)");

        executeAsSuperuser("GRANT DQL ON SCHEMA u TO " + TEST_USERNAME);
        executeAsSuperuser("GRANT DQL ON TABLE sys.health TO " + TEST_USERNAME);

        execute("SELECT table_schema, table_name, health FROM sys.health", null, testUserSession());
        assertThat(printedTable(response.rows()), is("u| t1| GREEN\n"));
    }

    @Test
    public void testQueryInformationSchemaShowsOnlyRowsRegardingTablesUserHasAccessOn() {
        executeAsSuperuser("create table t1 (x int) partitioned by (x) clustered into 1 shards with (number_of_replicas = 0)");
        executeAsSuperuser("insert into t1 values (1)");
        executeAsSuperuser("insert into t1 values (2)");
        executeAsSuperuser("create table my_schema.t2 (x int primary key) clustered into 1 shards with (number_of_replicas = 0)");
        executeAsSuperuser("create table other_schema.t3 (x int) clustered into 1 shards with (number_of_replicas = 0)");

        executeAsSuperuser("create view v1 as Select * from my_schema.t2");
        executeAsSuperuser("create view my_schema.v2 as Select * from other_schema.t3");
        executeAsSuperuser("create view other_schema.v3 as Select * from t1");

        executeAsSuperuser("create function my_schema.foo(long)" +
                " returns string language dummy_lang as 'function foo(x) { return \"1\"; }'");
        executeAsSuperuser("create function other_func(long)" +
                " returns string language dummy_lang as 'function foo(x) { return \"1\"; }'");

        executeAsSuperuser("grant dql on table t1 to " + TEST_USERNAME);
        executeAsSuperuser("grant dml on table my_schema.t2 to " + TEST_USERNAME);
        executeAsSuperuser("grant dql on schema my_schema to " + TEST_USERNAME);
        executeAsSuperuser("grant dql on view v1 to " + TEST_USERNAME);

        execute("select schema_name from information_schema.schemata order by schema_name", null, testUserSession());
        assertThat(printedTable(response.rows()), is("my_schema\n"));
        execute("select table_name from information_schema.tables order by table_name", null, testUserSession());
        assertThat(printedTable(response.rows()), is("t1\n" +
                                                     "t2\n" +
                                                     "v1\n" +
                                                     "v2\n"));
        execute("select table_name from information_schema.table_partitions order by table_name", null, testUserSession());
        assertThat(printedTable(response.rows()), is("t1\n" +
                                                     "t1\n"));
        execute("select table_name from information_schema.columns order by table_name", null, testUserSession());
        assertThat(printedTable(response.rows()),  is("t1\n" +
                                                      "t2\n" +
                                                      "v1\n" +
                                                      "v2\n"));
        execute("select table_name from information_schema.table_constraints order by table_name", null, testUserSession());
        assertThat(printedTable(response.rows()), is("t2\n"));
        execute("select routine_schema from information_schema.routines order by routine_schema", null, testUserSession());
        assertThat(printedTable(response.rows()), is("my_schema\n"));

        execute("select table_name from information_schema.views order by table_name", null, testUserSession());
        assertThat(printedTable(response.rows()), is("v1\n" +
                                                     "v2\n"));

        executeAsSuperuser("drop function other_func(long)");
        executeAsSuperuser("drop function my_schema.foo(long)");
    }

    @Test
    public void testRenameTableTransfersPrivilegesToNewTable() {
        executeAsSuperuser("create table doc.t1 (x int) clustered into 1 shards with (number_of_replicas = 0)");
        executeAsSuperuser("grant dql on table t1 to "+ TEST_USERNAME);

        executeAsSuperuser("alter table doc.t1 rename to t1_renamed");
        ensureYellow();

        execute("select * from t1_renamed", null, testUserSession());
        assertThat(response.rowCount(), is(0L));
    }

    @Test
    public void testPrivilegeIsSwappedWithSwapTable() {
        executeAsSuperuser("create table doc.t1 (x int)");
        executeAsSuperuser("create table doc.t2 (x int)");
        executeAsSuperuser("grant dql on table doc.t1 to " + TEST_USERNAME);

        executeAsSuperuser("alter cluster swap table doc.t1 to doc.t2 with (drop_source = true)");
        execute("select * from doc.t2", null, testUserSession());
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

        assertThrowsMatches(() -> execute("select * from t1", null, testUserSession()),
                     isSQLError(is("Schema 'doc' unknown"),
                                INTERNAL_ERROR,
                                NOT_FOUND,
                                4045));
    }

    @Test
    public void testDropViewRemovesPrivileges() {
        executeAsSuperuser("create view doc.v1 as select 1");
        executeAsSuperuser("grant dql on view v1 to "+ TEST_USERNAME);

        executeAsSuperuser("drop view v1");
        ensureYellow();

        executeAsSuperuser("create view doc.v1 as select 1");
        assertThrowsMatches(() -> execute("select * from v1", null, testUserSession()),
                     isSQLError(is("Schema 'doc' unknown"),
                                INTERNAL_ERROR,
                                NOT_FOUND,
                                4045));
    }

    @Test
    public void testDropEmptyPartitionedTableRemovesPrivileges() {
        executeAsSuperuser("create table doc.t1 (x int) partitioned by (x) clustered into 1 shards with (number_of_replicas = 0)");
        executeAsSuperuser("grant dql on table t1 to "+ TEST_USERNAME);

        executeAsSuperuser("drop table t1");
        ensureYellow();

        executeAsSuperuser("select * from sys.privileges where grantee = ? and ident = ?",
            new Object[]{TEST_USERNAME, "doc.t1"});
        assertThat(response.rowCount(), is(0L));

        executeAsSuperuser("create table doc.t1 (x int) clustered into 1 shards with (number_of_replicas = 0)");
        assertThrowsMatches(() -> execute("select * from t1", null, testUserSession()),
                     isSQLError(is("Schema 'doc' unknown"),
                                INTERNAL_ERROR,
                                NOT_FOUND,
                                4045));
    }

    @Test
    public void testGrantWithCustomDefaultSchema() {
        executeAsSuperuser("create table doc.t1 (x int)");
        executeAsSuperuser("set search_path to 'custom_schema'");
        executeAsSuperuser("create table t2 (x int)");
        ensureYellow();

        executeAsSuperuser("grant dql on table t2 to "+ TEST_USERNAME);
        assertThat(response.rowCount(), is(1L));

        assertThrowsMatches(() -> executeAsSuperuser("grant dql on table t1 to "+ TEST_USERNAME),
                     isSQLError(is("Relation 't1' unknown"),
                                INTERNAL_ERROR,
                                NOT_FOUND,
                                4041));
    }

    @Test
    public void testAlterClusterRerouteRetryFailedPrivileges() {
        executeAsSuperuser("alter cluster reroute retry failed");
        assertThat(response.rowCount(), is (0L));

        assertThrowsMatches(() -> executeAsNormalUser("alter cluster reroute retry failed"),
                     isSQLError(containsString("Missing 'AL' privilege for user 'normal'"),
                                INTERNAL_ERROR,
                                UNAUTHORIZED,
                                4011));
    }

    @Test
    public void testOperationOnClosedTableAsAuthorizedUser() {
        executeAsSuperuser("create table s.t1 (x int)");
        executeAsSuperuser("alter table s.t1 close");
        ensureYellow();

        executeAsSuperuser("grant dql on schema s to " + TEST_USERNAME);
        assertThat(response.rowCount(), is(1L));

        assertThrowsMatches(() ->  execute("refresh table s.t1", null, testUserSession()),
                     isSQLError(containsString("The relation \"s.t1\" doesn't support or allow REFRESH " +
                                               "operations, as it is currently closed."),
                                INTERNAL_ERROR,
                                BAD_REQUEST,
                                4007));
    }

    @Test
    public void testPermissionsValidOnTableAlias() {
        executeAsSuperuser("create table test.test (x int)");

        executeAsSuperuser("grant dql on schema test to " + TEST_USERNAME);
        executeAsSuperuser("deny dql on schema doc to " + TEST_USERNAME);
        assertThat(response.rowCount(), is(1L));
        ensureYellow();

        execute("select t.x from test.test as t", null, testUserSession("s"));
        assertThat(response.rowCount(), is(0L));
    }

    @Test
    public void testPermissionsValidOnSubselectAlias() {
        executeAsSuperuser("create table s.t1 (x int)");

        executeAsSuperuser("grant dql on schema s to " + TEST_USERNAME);
        executeAsSuperuser("deny dql on schema doc to " + TEST_USERNAME);
        assertThat(response.rowCount(), is(1L));
        ensureYellow();

        execute("select t.x from (select x from t1) as t", null, testUserSession("s"));
        assertThat(response.rowCount(), is(0L));
    }

    @Test
    public void testAccessesToPgClassEntriesWithRespectToPrivileges() throws Exception {
        //make sure a new user has default accesses to pg tables with information and pg catalog schema related entries
        execute("select relname from pg_catalog.pg_class order by relname", null, testUserSession());
        assertThat(response.rowCount(), is(40L));
        assertThat(printedTable(response.rows()), is(
            """
                character_sets
                columns
                columns_pkey
                key_column_usage
                key_column_usage_pkey
                pg_am
                pg_attrdef
                pg_attribute
                pg_class
                pg_constraint
                pg_database
                pg_description
                pg_enum
                pg_index
                pg_indexes
                pg_namespace
                pg_proc
                pg_publication
                pg_publication_tables
                pg_range
                pg_roles
                pg_settings
                pg_stats
                pg_tablespace
                pg_type
                referential_constraints
                referential_constraints_pkey
                routines
                schemata
                schemata_pkey
                sql_features
                sql_features_pkey
                table_constraints
                table_constraints_pkey
                table_partitions
                table_partitions_pkey
                tables
                tables_pkey
                views
                views_pkey
                """
        ));

        //create table that a new user is not privileged to access
        executeAsSuperuser("create table test_schema.my_table (my_col int)");
        executeAsSuperuser("insert into test_schema.my_table values (1),(2)");

        //make sure a new user cannot access my_table without privilege
        execute("select * from pg_catalog.pg_class where relname = 'my_table' order by relname", null, testUserSession());
        assertThat(response.rowCount(), is(0L));

        //if privilege is granted, the new user can access
        executeAsSuperuser("grant DQL on table test_schema.my_table to " + TEST_USERNAME);
        execute("select * from pg_catalog.pg_class where relname = 'my_table' order by relname", null, testUserSession());
        assertThat(response.rowCount(), is(1L));

        //values are identical
        String newUserWithPrivilegesResult = printedTable(response.rows());
        executeAsSuperuser("select * from pg_catalog.pg_class where relname = 'my_table' order by relname");
        String superUserResult = printedTable(response.rows());
        assertThat(newUserWithPrivilegesResult, is(superUserResult));
    }

    @Test
    public void testAccessesToPgProcEntriesWithRespectToPrivileges() throws Exception {
        //make sure a new user has default accesses to pg tables with information and pg catalog schema related entries
        execute("select proname from pg_catalog.pg_proc order by proname", null, testUserSession());
        assertThat(response.rowCount(), not(0L));

        //create a table and a function that a new user is not privileged to access
        executeAsSuperuser("create table test_schema.my_table (my_col int)");
        executeAsSuperuser("create function test_schema.bar(long)" +
                           " returns string language dummy_lang as 'function bar(x) { return \"1\"; }'");

        //make sure a new user cannot access function bar without privilege
        execute("select * from pg_catalog.pg_proc where proname = 'bar' order by proname", null, testUserSession());
        assertThat(response.rowCount(), is(0L));

        //if privilege is granted, the new user can access
        executeAsSuperuser("grant DQL on schema test_schema to " + TEST_USERNAME);
        execute("select * from pg_catalog.pg_proc where proname = 'bar' order by proname", null, testUserSession());
        assertThat(response.rowCount(), is(1L));

        //values are identical
        String newUserWithPrivilegesResult = printedTable(response.rows());
        executeAsSuperuser("select * from pg_catalog.pg_proc where proname = 'bar' order by proname");
        String superUserResult = printedTable(response.rows());
        assertThat(newUserWithPrivilegesResult, is(superUserResult));

        executeAsSuperuser("drop function test_schema.bar(bigint)");
    }

    @Test
    public void testAccessesToPgNamespaceEntriesWithRespectToPrivileges() throws Exception {
        //make sure a new user has default accesses to pg tables with information and pg catalog schema related entries
        execute("select nspname from pg_catalog.pg_namespace " +
                "where nspname='information_schema' or nspname='pg_catalog' or nspname='sys' order by nspname",
                null, testUserSession());
        assertThat(printedTable(response.rows()), is("""
                                                         information_schema
                                                         pg_catalog
                                                         """));
        execute("select * from pg_catalog.pg_namespace " +
                "where nspname='blob' or nspname='doc'", null, testUserSession());
        assertThat(response.rowCount(), is(0L));

        //create a schema that a new user is not privileged to access
        executeAsSuperuser("create table test_schema.my_table (my_col int)");
        executeAsSuperuser("insert into test_schema.my_table values (1),(2)");

        //make sure a new user cannot access test_schema without privilege
        execute("select * from pg_catalog.pg_namespace where nspname = 'test_schema' order by nspname", null, testUserSession());
        assertThat(response.rowCount(), is(0L));

        //if privilege is granted, the new user can access
        executeAsSuperuser("grant DQL on schema test_schema to " + TEST_USERNAME);
        execute("select * from pg_catalog.pg_namespace where nspname = 'test_schema' order by nspname", null, testUserSession());
        assertThat(response.rowCount(), is(1L));

        //values are identical
        String newUserWithPrivilegesResult = printedTable(response.rows());
        executeAsSuperuser("select * from pg_catalog.pg_namespace where nspname = 'test_schema' order by nspname");
        String superUserResult = printedTable(response.rows());
        assertThat(newUserWithPrivilegesResult, is(superUserResult));
    }

    @Test
    public void testAccessesToPgAttributeEntriesWithRespectToPrivileges() throws Exception {
        //make sure a new user has default accesses to pg tables with information and pg catalog schema related entries
        execute("select * from pg_catalog.pg_attribute order by attname", null, testUserSession());
        assertThat(response.rowCount(), is(445L));

        //create a table with an attribute that a new user is not privileged to access
        executeAsSuperuser("create table test_schema.my_table (my_col int)");
        executeAsSuperuser("insert into test_schema.my_table values (1),(2)");

        //make sure a new user cannot access my_col without privilege
        execute("select * from pg_catalog.pg_attribute where attname = 'my_col' order by attname", null, testUserSession());
        assertThat(response.rowCount(), is(0L));

        //if privilege is granted, the new user can access
        executeAsSuperuser("grant DQL on table test_schema.my_table to " + TEST_USERNAME);
        execute("select * from pg_catalog.pg_attribute where attname = 'my_col' order by attname", null, testUserSession());
        assertThat(response.rowCount(), is(1L));

        //values are identical
        String newUserWithPrivilegesResult = printedTable(response.rows());
        executeAsSuperuser("select * from pg_catalog.pg_attribute where attname = 'my_col' order by attname");
        String superUserResult = printedTable(response.rows());
        assertThat(newUserWithPrivilegesResult, is(superUserResult));
    }

    @Test
    public void testAccessesToPgConstraintEntriesWithRespectToPrivileges() throws Exception {
        //make sure a new user has default accesses to pg tables with information and pg catalog schema related entries
        execute("select conname from pg_catalog.pg_constraint order by conname", null, testUserSession());
        String newUserResult = printedTable(response.rows());
        assertThat(newUserResult, is(
            """
                columns_pk
                information_schema_columns_column_name_not_null
                information_schema_columns_data_type_not_null
                information_schema_columns_is_generated_not_null
                information_schema_columns_is_nullable_not_null
                information_schema_columns_ordinal_position_not_null
                information_schema_columns_table_catalog_not_null
                information_schema_columns_table_name_not_null
                information_schema_columns_table_schema_not_null
                key_column_usage_pk
                referential_constraints_pk
                schemata_pk
                sql_features_pk
                table_constraints_pk
                table_partitions_pk
                tables_pk
                views_pk
                """
        ));

        //create a table with constraints that a new user is not privileged to access
        executeAsSuperuser("create table test_schema.my_table (my_pk int primary key, my_col int check (my_col > 0))");
        executeAsSuperuser("insert into test_schema.my_table values (1,10),(2,20)");

        //make sure a new user cannot access constraints without privilege
        execute("select * from pg_catalog.pg_constraint" +
                " where conname = 'my_table_pk' or conname like 'test_schema_my_table_my_col_check_%' order by conname",
                null, testUserSession());
        assertThat(response.rowCount(), is(0L));

        //if privilege is granted, the new user can access
        executeAsSuperuser("grant DQL on table test_schema.my_table to " + TEST_USERNAME);
        execute("select * from pg_catalog.pg_constraint" +
                " where conname = 'my_table_pk' or conname like 'test_schema_my_table_my_col_check_%' order by conname",
                null, testUserSession());
        assertThat(response.rowCount(), is(2L));

        //values are identical
        String newUserWithPrivilegesResult = printedTable(response.rows());
        executeAsSuperuser("select * from pg_catalog.pg_constraint" +
                           " where conname = 'my_table_pk' or conname like 'test_schema_my_table_my_col_check_%' order by conname");
        String superUserResult = printedTable(response.rows());
        assertThat(newUserWithPrivilegesResult, is(superUserResult));
    }
}
