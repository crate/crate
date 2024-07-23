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

import static io.crate.protocols.postgres.PGErrorStatus.INTERNAL_ERROR;
import static io.crate.protocols.postgres.PGErrorStatus.UNDEFINED_OBJECT;
import static io.crate.protocols.postgres.PGErrorStatus.UNDEFINED_TABLE;
import static io.crate.testing.Asserts.assertThat;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.CONFLICT;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.UNAUTHORIZED;

import org.elasticsearch.test.IntegTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.crate.testing.Asserts;

@IntegTestCase.ClusterScope(minNumDataNodes = 2)
public class RoleManagementIntegrationTest extends BaseRolesIntegrationTest {

    private static final String GRANTOR_USER = "the_grantor";

    @Before
    public void createGrantorUser() {
        execute("CREATE USER " + GRANTOR_USER);
        execute("GRANT AL TO " + GRANTOR_USER);
    }

    @After
    public void cleanUp() {
        execute("DROP USER " + GRANTOR_USER);
    }

    @Test
    public void testCreateUser() {
        executeAsSuperuser("create user trillian");
        assertThat(response).hasRowCount(1);
        assertUserIsCreated("trillian");
    }

    @Test
    public void testCreateRole() {
        executeAsSuperuser("create role dummy_role");
        assertThat(response).hasRowCount(1);
        assertRoleIsCreated("dummy_role");
    }

    @Test
    public void test_create_role_with_password_is_not_allowed() {
        Asserts.assertSQLError(() -> executeAsSuperuser("create role dummy with (password='pwd')"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4004)
            .hasMessageContaining("Creating a ROLE with a password is not allowed, use CREATE USER instead");
    }

    @Test
    public void testSysUsersTableColumns() {
        // The sys.users table contains 3 columns, name, password and superuser flag
        executeAsSuperuser("select column_name, data_type from information_schema.columns where table_name='users' and table_schema='sys'");
        assertThat(response).hasRows(
            "granted_roles| object_array",
            "granted_roles['grantor']| text_array",
            "granted_roles['role']| text_array",
            "jwt| object",
            "jwt['aud']| text",
            "jwt['iss']| text",
            "jwt['username']| text",
            "name| text",
            "password| text",
            "session_settings| object",
            "superuser| boolean"
        );
    }

    @Test
    public void testSysUsersTableDefaultUser() {
        // The sys.users table always contains the superuser crate
        executeAsSuperuser("SELECT name, granted_roles, password, superuser FROM sys.users WHERE name = 'crate'");
        assertThat(response).hasRows("crate| []| NULL| true");
    }

    @Test
    public void testSysUsersTable() {
        executeAsSuperuser("CREATE USER arthur");
        assertUserIsCreated("arthur");
        executeAsSuperuser("ALTER USER arthur SET (enable_hashjoin = false)");
        executeAsSuperuser("CREATE USER ford WITH (password = 'foo')");
        assertUserIsCreated("ford");
        executeAsSuperuser("CREATE ROLE a_role");
        assertRoleIsCreated("a_role");
        executeAs("GRANT a_role to arthur", GRANTOR_USER);
        executeAsSuperuser(
            "SELECT name, granted_roles, session_settings, password, superuser FROM sys.users " +
            "WHERE superuser = FALSE ORDER BY name");
        // Every created user is not a superuser
        assertThat(response).hasRows(
            "arthur| [{role=a_role, grantor=the_grantor}]| {enable_hashjoin=false}| NULL| false",
            "ford| []| {}| ********| false",
            "normal| []| {}| NULL| false",
            "the_grantor| []| {}| NULL| false"
        );
        executeAsSuperuser(
            "SELECT session_settings['enable_hashjoin'] FROM sys.users " +
                "WHERE name = 'arthur'");
        assertThat(response).hasRows("f");
    }

    @Test
    public void testSysRolesTableColumns() {
        executeAsSuperuser("select column_name, data_type from information_schema.columns where table_name='roles' and table_schema='sys'");
        assertThat(response).hasRows(
            "granted_roles| object_array",
            "granted_roles['grantor']| text_array",
            "granted_roles['role']| text_array",
            "name| text");
    }

    @Test
    public void testSysRolesTable() {
        executeAsSuperuser("CREATE ROLE role1");
        assertRoleIsCreated("role1");
        executeAsSuperuser("CREATE ROLE role2");
        assertRoleIsCreated("role2");
        executeAsSuperuser("CREATE ROLE role3");
        assertRoleIsCreated("role3");
        executeAs("GRANT role3, role2 TO role1", GRANTOR_USER);
        executeAsSuperuser("SELECT name, granted_roles FROM sys.roles ORDER BY name");
        assertThat(response).hasRows(
            "role1| [{role=role2, grantor=the_grantor}, {role=role3, grantor=the_grantor}]",
            "role2| []",
            "role3| []"
        );
    }

    @Test
    public void testCreateUserWithPasswordExpression() {
        executeAsSuperuser("CREATE USER arthur WITH (password = substr(?, 3))", new Object[]{"**password"});
        assertUserIsCreated("arthur");
    }

    @Test
    public void testAlterUserPassword() {
        executeAsSuperuser("CREATE USER arthur");
        assertUserIsCreated("arthur");
        executeAsSuperuser("CREATE USER ford WITH (password = ?)", new Object[]{"foo"});
        assertUserIsCreated("ford");
        executeAsSuperuser("ALTER USER arthur SET (password = ?)", new Object[]{"pass"});
        executeAsSuperuser("ALTER USER ford SET (password = ?)", new Object[]{"pass"});
        executeAsSuperuser("SELECT name, password, superuser FROM sys.users WHERE superuser = FALSE ORDER BY name");
        assertThat(response).hasRows(
            "arthur| ********| false",
            "ford| ********| false",
            "normal| NULL| false",
            "the_grantor| NULL| false"
        );
    }

    @Test
    public void testAlterRolePassword() {
        executeAsSuperuser("CREATE ROLE dummy");
        Asserts.assertSQLError(() -> executeAsSuperuser("ALTER ROLE dummy SET (password = ?)", new Object[]{"pass"}))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4004)
            .hasMessageContaining("Setting a password to a ROLE is not allowed");
    }

    @Test
    public void testAlterUserResetPassword() {
        executeAsSuperuser("CREATE USER arthur WITH (password = 'foo')");
        assertUserIsCreated("arthur");
        executeAsSuperuser("ALTER USER arthur SET (password = null)");
        executeAsSuperuser("SELECT name, password, superuser FROM sys.users WHERE superuser = FALSE ORDER BY name");
        assertThat(response).hasRows("arthur| NULL| false", "normal| NULL| false", "the_grantor| NULL| false");
    }

    @Test
    public void testAlterNonExistingUserThrowsException() {
        Asserts.assertSQLError(() -> executeAsSuperuser("alter user unknown_user set (password = 'unknown')"))
            .hasPGError(UNDEFINED_OBJECT)
            .hasHTTPError(NOT_FOUND, 40410)
            .hasMessageContaining("Role 'unknown_user' does not exist");
    }

    @Test
    public void testDropUser() {
        executeAsSuperuser("create user ford");
        assertUserIsCreated("ford");
        executeAsSuperuser("drop user ford");
        assertUserDoesntExist("ford");
    }

    @Test
    public void testDropUserIfExists() {
        executeAsSuperuser("drop user if exists not_exist_if_exists");
        assertThat(response).hasRowCount(0);
    }

    @Test
    public void testDropUserUnAuthorized() {
        Asserts.assertSQLError(() -> executeAs("drop user ford", NORMAL_USER))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(UNAUTHORIZED, 4011)
            .hasMessageContaining("Missing 'AL' privilege for user 'normal'");
    }

    @Test
    public void testCreateUserUnAuthorized() {
        Asserts.assertSQLError(() -> executeAs("create user ford", NORMAL_USER))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(UNAUTHORIZED, 4011)
            .hasMessageContaining("Missing 'AL' privilege for user 'normal'");
    }

    @Test
    public void testCreateNormalUserUnAuthorized() {
        Asserts.assertSQLError(() -> executeAs("create user ford", NORMAL_USER))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(UNAUTHORIZED, 4011)
            .hasMessageContaining("Missing 'AL' privilege for user 'normal'");
    }

    @Test
    public void test_create_user_as_regular_user_with_al_privileges() {
        executeAsSuperuser("CREATE USER trillian");
        executeAsSuperuser("GRANT AL TO trillian");
        executeAs("CREATE USER arthur", "trillian");
        assertUserIsCreated("arthur");
        executeAs("DROP USER arthur", "trillian");
        assertUserDoesntExist("arthur");
    }

    @Test
    public void testCreateExistingUserThrowsException() {
        executeAsSuperuser("create user ford_exists");
        assertUserIsCreated("ford_exists");

        Asserts.assertSQLError(() -> executeAsSuperuser("create user ford_exists"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(CONFLICT, 4099)
            .hasMessageContaining("Role 'ford_exists' already exists");
    }

    @Test
    public void testDropNonExistingUserThrowsException() {
        Asserts.assertSQLError(() -> executeAsSuperuser("drop user not_exists"))
            .hasPGError(UNDEFINED_OBJECT)
            .hasHTTPError(NOT_FOUND, 40410)
            .hasMessageContaining("Role 'not_exists' does not exist");
    }

    @Test
    public void testDropSuperUserThrowsException() {
        Asserts.assertSQLError(() -> executeAsSuperuser("drop user crate"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4004)
            .hasMessageContaining("Cannot drop a superuser 'crate'");
    }

    @Test
    public void test_create_user_jwt_iss_and_username_must_be_unique() {
        // 'aud' field is intentionally different in 2 CREATE statements as it's not taken to account on JWT uniqueness check.
        execute("CREATE USER user1 WITH (jwt = {\"iss\" = 'dummy.org/keys', \"username\" = 'app_user', \"aud\" = 'aud1'})");
        execute("SELECT name, jwt from sys.users WHERE name = 'user1'");
        assertThat(response).hasRows("user1| {aud=aud1, iss=dummy.org/keys, username=app_user}");
        Asserts.assertSQLError(() -> execute("CREATE USER user2 WITH (jwt = {\"iss\" = 'dummy.org/keys', \"username\" = 'app_user',  \"aud\" = 'aud2'})"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(CONFLICT, 4099)
            .hasMessageContaining("Another role with the same combination of iss/username jwt properties already exists");
    }

    @Test
    public void test_alter_user_jwt_iss_and_username_must_be_unique() {
        execute("CREATE USER user1 WITH (password = 'pwd', jwt = {\"iss\" = 'issuer1', \"username\" = 'user1'})");
        execute("CREATE USER user2 WITH (password = 'pwd', jwt = {\"iss\" = 'issuer2', \"username\" = 'user2'})");

        // Updating JWT properties clashes with JWT properties of an existing user.
        Asserts.assertSQLError(() -> execute("ALTER USER user1 set (jwt = {\"iss\" = 'issuer2', \"username\" = 'user2'})"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(CONFLICT, 4099)
            .hasMessageContaining("Another role with the same combination of iss/username jwt properties already exists");
    }

    @Test
    public void test_user_session_settings_are_applied_and_reset() {
        execute("CREATE USER user1");
        execute("ALTER USER user1 SET (search_path='my_schema')");
        assertUserIsCreated("user1");
        execute("GRANT DQL ON SCHEMA my_schema TO user1");
        execute("CREATE table my_schema.t(a int);");
        execute("INSERT INTO my_schema.t(a) values(1),(2)");
        execute("REFRESH TABLE my_schema.t");

        executeAs("SELECT * FROM t ORDER BY 1", "user1");
        assertThat(response).hasRows("1", "2");

        execute("ALTER USER user1 RESET search_path");
        Asserts.assertSQLError(() -> executeAs("SELECT * FROM t", "user1"))
            .hasPGError(UNDEFINED_TABLE)
            .hasHTTPError(NOT_FOUND, 4041)
            .hasMessageContaining("Relation 't' unknown");
    }
}
