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
import static io.crate.testing.Asserts.assertThat;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.CONFLICT;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.UNAUTHORIZED;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.elasticsearch.test.IntegTestCase;
import org.junit.After;
import org.junit.Test;

import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.testing.Asserts;
import io.crate.testing.SQLResponse;

@IntegTestCase.ClusterScope(minNumDataNodes = 2)
public class RoleManagementIntegrationTest extends BaseRolesIntegrationTest {

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
        assertThatThrownBy(() -> executeAsSuperuser("create role dummy with (password='pwd')"))
            .isExactlyInstanceOf(UnsupportedFeatureException.class)
            .hasMessage("Creating a ROLE with a password is not allowed, use CREATE USER instead");
    }

    @Test
    public void testSysUsersTableColumns() {
        // The sys.users table contains 3 columns, name, password and superuser flag
        executeAsSuperuser("select column_name, data_type from information_schema.columns where table_name='users' and table_schema='sys'");
        assertThat(response).hasRows(
                "name| text",
                "password| text",
                "superuser| boolean");
    }

    @Test
    public void testSysUsersTableDefaultUser() {
        // The sys.users table always contains the superuser crate
        executeAsSuperuser("select name, password, superuser from sys.users where name = 'crate'");
        assertThat(response).hasRows("crate| NULL| true");
    }

    @Test
    public void testSysUsersTable() {
        executeAsSuperuser("CREATE USER arthur");
        assertUserIsCreated("arthur");
        executeAsSuperuser("CREATE USER ford WITH (password = 'foo')");
        assertUserIsCreated("ford");
        executeAsSuperuser("SELECT name, password, superuser FROM sys.users WHERE superuser = FALSE ORDER BY name");
        // Every created user is not a superuser
        assertThat(response).hasRows(
                "arthur| NULL| false",
                "ford| ********| false");
    }

    @Test
    public void testSysRolesTableColumns() {
        // The sys.roles table contains one column
        executeAsSuperuser("select column_name, data_type from information_schema.columns where table_name='roles' and table_schema='sys'");
        assertThat(response).hasRows("name| text");
    }

    @Test
    public void testSysRolesTable() {
        executeAsSuperuser("CREATE ROLE role1");
        assertRoleIsCreated("role1");
        executeAsSuperuser("CREATE ROLE role2");
        assertRoleIsCreated("role2");
        executeAsSuperuser("SELECT * FROM sys.roles ORDER BY name");
        assertThat(response).hasRows(
            "role1",
            "role2");
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
                "ford| ********| false");
    }

    @Test
    public void testAlterRolePassword() {
        executeAsSuperuser("CREATE ROLE dummy");
        assertThatThrownBy(() -> executeAsSuperuser("ALTER ROLE dummy SET (password = ?)", new Object[]{"pass"}))
            .isExactlyInstanceOf(UnsupportedFeatureException.class)
            .hasMessage("Setting a password to a ROLE is not allowed");
    }

    @Test
    public void testAlterUserResetPassword() {
        executeAsSuperuser("CREATE USER arthur WITH (password = 'foo')");
        assertUserIsCreated("arthur");
        executeAsSuperuser("ALTER USER arthur SET (password = null)");
        executeAsSuperuser("SELECT name, password, superuser FROM sys.users WHERE superuser = FALSE ORDER BY name");
        assertThat(response).hasRows("arthur| NULL| false");
    }

    @Test
    public void testAlterNonExistingUserThrowsException() {
        Asserts.assertSQLError(() -> executeAsSuperuser("alter user unknown_user set (password = 'unknown')"))
            .hasPGError(INTERNAL_ERROR)
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
        Asserts.assertSQLError(() -> executeAsNormalUser("drop user ford"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(UNAUTHORIZED, 4011)
            .hasMessageContaining("Missing 'AL' privilege for user 'normal'");
    }

    @Test
    public void testCreateUserUnAuthorized() {
        Asserts.assertSQLError(() -> executeAsNormalUser("create user ford"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(UNAUTHORIZED, 4011)
            .hasMessageContaining("Missing 'AL' privilege for user 'normal'");
    }

    @Test
    public void testCreateNormalUserUnAuthorized() {
        Asserts.assertSQLError(() -> executeAsNormalUser("create user ford"))
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
            .hasPGError(INTERNAL_ERROR)
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

    private void assertUserIsCreated(String userName) {
        SQLResponse response = executeAsSuperuser("select count(*) from sys.users where name = ?",
            new Object[]{userName});
        assertThat(response).hasRows("1");
    }

    private void assertRoleIsCreated(String roleName) {
        SQLResponse response = executeAsSuperuser("select count(*) from sys.roles where name = ?",
            new Object[]{roleName});
        assertThat(response).hasRows("1");
    }

    private void assertUserDoesntExist(String userName) {
        SQLResponse response = executeAsSuperuser("select count(*) from sys.users where name = ?",
            new Object[]{userName});
        assertThat(response).hasRows("0");
    }
}
