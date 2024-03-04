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

import java.util.Set;

import org.elasticsearch.test.IntegTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.crate.action.sql.Session;
import io.crate.action.sql.Sessions;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.role.Permission;
import io.crate.role.Policy;
import io.crate.role.Privilege;
import io.crate.role.Securable;
import io.crate.role.metadata.RolesHelper;
import io.crate.testing.Asserts;

@IntegTestCase.ClusterScope(minNumDataNodes = 2)
public class RoleManagementIntegrationTest extends BaseRolesIntegrationTest {

    private Session grantorUserSession;

    private Session createGrantorUserSession() {
        Sessions sqlOperations = cluster().getInstance(Sessions.class);
        var alPriv = new Privilege(
            Policy.GRANT,
            Permission.AL,
            Securable.CLUSTER,
            null,
            "crate");
        return sqlOperations.newSession(null, RolesHelper.userOf("the_grantor", Set.of(alPriv), null));
    }

    @Before
    public void setUpAdditionalSessions() {
        grantorUserSession = createGrantorUserSession();
    }

    @After
    public void closeAdditionalSessions() {
        grantorUserSession.close();
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
            "granted_roles| object_array",
            "granted_roles['grantor']| text_array",
            "granted_roles['role']| text_array",
            "jwt| object",
            "jwt['iss']| text",
            "jwt['username']| text",
            "name| text",
            "password| text",
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
        executeAsSuperuser("CREATE USER ford WITH (password = 'foo')");
        assertUserIsCreated("ford");
        executeAsSuperuser("CREATE ROLE a_role");
        assertRoleIsCreated("a_role");
        execute("GRANT a_role to arthur", null, grantorUserSession);
        executeAsSuperuser(
            "SELECT name, granted_roles, password, superuser FROM sys.users " +
            "WHERE superuser = FALSE ORDER BY name");
        // Every created user is not a superuser
        assertThat(response).hasRows(
            "arthur| [{role=a_role, grantor=the_grantor}]| NULL| false",
            "ford| []| ********| false");
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
        execute("GRANT role3, role2 TO role1", null, grantorUserSession);
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

    @Test
    public void test_create_user_jwt_properties_must_be_unique() {
        execute("CREATE USER user1 WITH (jwt = {\"iss\" = 'dummy.org/keys', \"username\" = 'app_user'})");
        execute("SELECT name, jwt from sys.users WHERE name = 'user1'");
        assertThat(response).hasRows("user1| {iss=dummy.org/keys, username=app_user}");
        Asserts.assertSQLError(() -> execute("CREATE USER user2 WITH (jwt = {\"iss\" = 'dummy.org/keys', \"username\" = 'app_user'})"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(CONFLICT, 4099)
            .hasMessageContaining("Another role with the same combination of jwt properties already exists");
    }
}
