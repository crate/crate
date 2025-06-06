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

package io.crate.role;

import static io.crate.role.metadata.RolesHelper.DUMMY_USERS;
import static io.crate.role.metadata.RolesHelper.DUMMY_USERS_AND_ROLES;
import static io.crate.role.metadata.RolesHelper.DUMMY_USERS_AND_ROLES_WITHOUT_PASSWORD;
import static io.crate.role.metadata.RolesHelper.DUMMY_USERS_WITHOUT_PASSWORD;
import static io.crate.role.metadata.RolesHelper.OLD_DUMMY_USERS_PRIVILEGES;
import static io.crate.role.metadata.RolesHelper.SINGLE_USER_ONLY;
import static io.crate.role.metadata.RolesHelper.getSecureHash;
import static io.crate.role.metadata.RolesHelper.userOf;
import static io.crate.role.metadata.RolesHelper.usersMetadataOf;
import static io.crate.testing.Asserts.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ClusterServiceUtils;
import org.junit.Test;

import io.crate.exceptions.RoleAlreadyExistsException;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.expression.symbol.Literal;
import io.crate.fdw.AddServerTask;
import io.crate.fdw.CreateServerRequest;
import io.crate.role.metadata.RolesHelper;
import io.crate.role.metadata.RolesMetadata;
import io.crate.role.metadata.UsersMetadata;
import io.crate.role.metadata.UsersPrivilegesMetadata;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class TransportRoleTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void testCreateFirstUser() throws Exception {
        Metadata.Builder mdBuilder = new Metadata.Builder();
        TransportCreateRole.putRole(mdBuilder,
            "root",
            true,
            null,
            new JwtProperties("https:dummy.org", "test", "test_aud"),
            Map.of("session_timeout", "1h"));
        RolesMetadata metadata = (RolesMetadata) mdBuilder.getCustom(RolesMetadata.TYPE);
        assertThat(metadata.roleNames()).containsExactly("root");
        Role role = metadata.roles().get("root");
        var jwtProps = role.jwtProperties();
        assertThat(jwtProps).isNotNull();
        assertThat(jwtProps.iss()).isEqualTo("https:dummy.org");
        assertThat(jwtProps.username()).isEqualTo("test");
        assertThat(jwtProps.aud()).isEqualTo("test_aud");
    }

    @Test
    public void test_create_user_with_matching_jwt_props_exists() throws Exception {
        // Users have different "aud" property values - still clashing as only iss/username matters.
        Metadata.Builder mdBuilder = new Metadata.Builder();
        TransportCreateRole.putRole(mdBuilder,
            "user1",
            true,
            null,
            new JwtProperties("https:dummy.org", "test", "aud1"),
            Map.of());

        assertThatThrownBy(() -> TransportCreateRole.putRole(mdBuilder,
                "user2",
                true,
                null,
                new JwtProperties("https:dummy.org", "test", "aud2"),
            Map.of()))
            .isExactlyInstanceOf(RoleAlreadyExistsException.class)
            .hasMessage("Another role with the same combination of iss/username jwt properties already exists");
    }

    @Test
    public void test_create_user_with_existing_name_but_different_jwt_props() throws Exception {
        Metadata.Builder mdBuilder = new Metadata.Builder();
        TransportCreateRole.putRole(mdBuilder,
            "user1",
            true,
            null,
            new JwtProperties("https:dummy.org", "test", null),
            Map.of());

        boolean exists = TransportCreateRole.putRole(mdBuilder,
            "user1",
            true,
            null,
            new JwtProperties("https:dummy.org", "test2", null),
            Map.of());
        assertThat(exists).isTrue();
    }

    @Test
    public void testCreateUserAlreadyExists() throws Exception {
        Metadata.Builder mdBuilder = new Metadata.Builder()
            .putCustom(RolesMetadata.TYPE, new RolesMetadata(SINGLE_USER_ONLY));
        assertThat(TransportCreateRole.putRole(mdBuilder, "Arthur", true, null, null, Map.of())).isTrue();
    }

    @Test
    public void testCreateUser() throws Exception {
        Metadata.Builder mdBuilder = new Metadata.Builder()
            .putCustom(RolesMetadata.TYPE, new RolesMetadata(SINGLE_USER_ONLY));
        TransportCreateRole.putRole(mdBuilder, "Trillian", true, null, null, Map.of());
        RolesMetadata newMetadata = (RolesMetadata) mdBuilder.getCustom(RolesMetadata.TYPE);
        assertThat(newMetadata.roleNames()).containsExactlyInAnyOrder("Trillian", "Arthur");
    }

    @Test
    public void test_create_user_with_old_users_metadata() throws Exception {
        var oldUsersMetadata = usersMetadataOf(DUMMY_USERS);
        var oldRolesMetadata = new RolesMetadata(DUMMY_USERS_AND_ROLES);
        Metadata.Builder mdBuilder = Metadata.builder()
            .putCustom(UsersMetadata.TYPE, oldUsersMetadata)
            .putCustom(RolesMetadata.TYPE, oldRolesMetadata);
        boolean res = TransportCreateRole.putRole(mdBuilder, "RoleFoo", false, null, null, Map.of());
        assertThat(res).isFalse();
        assertThat(roles(mdBuilder)).containsExactlyInAnyOrderEntriesOf(
            Map.of("Arthur", DUMMY_USERS.get("Arthur"),
                "Ford", DUMMY_USERS.get("Ford"),
                "RoleFoo", RolesHelper.roleOf("RoleFoo")));
    }

    @Test
    public void test_alter_user_with_old_users_metadata() throws Exception {
        var oldUsersMetadata = usersMetadataOf(DUMMY_USERS_WITHOUT_PASSWORD);
        var oldUsersPrivilegesMetadata = new UsersPrivilegesMetadata(OLD_DUMMY_USERS_PRIVILEGES);
        var oldRolesMetadata = new RolesMetadata(DUMMY_USERS_AND_ROLES_WITHOUT_PASSWORD);
        Metadata.Builder mdBuilder = Metadata.builder()
            .putCustom(UsersMetadata.TYPE, oldUsersMetadata)
            .putCustom(UsersPrivilegesMetadata.TYPE, oldUsersPrivilegesMetadata)
            .putCustom(RolesMetadata.TYPE, oldRolesMetadata);
        var newPasswd = getSecureHash("arthurs-new-passwd");
        boolean res = TransportAlterRole.alterRole(mdBuilder,
            "Arthur",
            newPasswd,
            null,
            false,
            false,
            Map.of()
        );
        assertThat(res).isTrue();

        var newFordUser = DUMMY_USERS_WITHOUT_PASSWORD.get("Ford")
                .with(new RolePrivileges(OLD_DUMMY_USERS_PRIVILEGES.get("Ford")));
        var newArthurUser = DUMMY_USERS_WITHOUT_PASSWORD.get("Arthur")
                .with(new RolePrivileges(OLD_DUMMY_USERS_PRIVILEGES.get("Arthur")))
                .with(newPasswd, null, Map.of());
        assertThat(roles(mdBuilder)).containsExactlyInAnyOrderEntriesOf(
            Map.of("Arthur", newArthurUser,
                "Ford", newFordUser));
    }

    @Test
    public void testDropUserNoUsersAtAll() throws Exception {
        assertThat(DropRoleTask.dropRole(Metadata.builder(), "root")).isFalse();
    }

    @Test
    public void testDropNonExistingUser() throws Exception {
        boolean res = DropRoleTask.dropRole(
                Metadata.builder().putCustom(RolesMetadata.TYPE, new RolesMetadata(SINGLE_USER_ONLY)),
                "trillian"
        );
        assertThat(res).isFalse();
    }

    @Test
    public void testDropUser() throws Exception {
        RolesMetadata metadata = new RolesMetadata(DUMMY_USERS);
        Metadata.Builder mdBuilder = Metadata.builder().putCustom(RolesMetadata.TYPE, metadata);
        boolean res = DropRoleTask.dropRole(mdBuilder, "Arthur");
        assertThat(roles(mdBuilder)).containsExactlyEntriesOf(Map.of("Ford", DUMMY_USERS.get("Ford")));
        assertThat(res).isTrue();
    }

    @Test
    public void test_drop_role_with_children_is_not_allowed() {
        var role1 = RolesHelper.roleOf("role1");
        var role2 = RolesHelper.roleOf("role1", List.of("role1"));
        var role3 = RolesHelper.roleOf("role3", List.of("role2"));
        Map<String, Role> roles = Map.of(
            "role1", role1,
            "role2", role2,
            "role3", role3
        );
        RolesMetadata metadata = new RolesMetadata(roles);
        Metadata.Builder mdBuilder = Metadata.builder().putCustom(RolesMetadata.TYPE, metadata);
        assertThatThrownBy(() -> DropRoleTask.dropRole(mdBuilder, "role2"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Cannot drop ROLE: role2 as it is granted on role: role3");
    }

    @Test
    public void test_drop_user_with_old_users_metadata() throws Exception {
        var oldUsersMetadata = usersMetadataOf(DUMMY_USERS);
        var oldRolesMetadata = new RolesMetadata(DUMMY_USERS_AND_ROLES);
        Metadata.Builder mdBuilder = Metadata.builder()
            .putCustom(UsersMetadata.TYPE, oldUsersMetadata)
            .putCustom(RolesMetadata.TYPE, oldRolesMetadata);
        boolean res = DropRoleTask.dropRole(mdBuilder, "Arthur");
        assertThat(roles(mdBuilder)).containsExactlyEntriesOf(Map.of("Ford", DUMMY_USERS.get("Ford")));
        assertThat(res).isTrue();
    }

    @Test
    public void test_cannot_drop_user_mapped_to_foreign_servers() throws Exception {
        Map<String, Role> roles = Map.of(
            "role1", RolesHelper.roleOf("role1")
        );
        RolesMetadata rolesMetadata = new RolesMetadata(roles);
        Metadata metadata = Metadata.builder()
            .putCustom(RolesMetadata.TYPE, rolesMetadata)
            .build();
        ClusterState clusterState = ClusterState.builder(clusterService.state()).metadata(metadata).build();

        var e = SQLExecutor.builder(clusterService).build();
        CreateServerRequest createServerRequest = new CreateServerRequest(
            "pg",
            "jdbc",
            "role1",
            false,
            Settings.builder().put("url", "jdbc:postgresql://localhost:5432/").build());
        AddServerTask addServerTask = new AddServerTask(e.foreignDataWrappers, createServerRequest);
        ClusterServiceUtils.setState(clusterService, addServerTask.execute(clusterState));

        DropRoleTask dropRoleTask = new DropRoleTask(new DropRoleRequest("role1", false));
        assertThatThrownBy(() -> dropRoleTask.execute(clusterService.state()))
            .isExactlyInstanceOf(IllegalStateException.class)
            .hasMessage("User 'role1' cannot be dropped. The user mappings for foreign servers '[pg]' needs to be dropped first.");
    }

    @Test
    public void test_alter_user_cannot_set_password_to_role() throws Exception {
        var oldRolesMetadata = new RolesMetadata(DUMMY_USERS_AND_ROLES_WITHOUT_PASSWORD);
        Metadata.Builder mdBuilder = Metadata.builder()
            .putCustom(RolesMetadata.TYPE, oldRolesMetadata);
        // Set new password
        assertThatThrownBy(() -> TransportAlterRole.alterRole(
            mdBuilder,
            "DummyRole",
            getSecureHash("new-passwd"),
            null,
            false,
            false,
            Map.of()))
            .isExactlyInstanceOf(UnsupportedFeatureException.class)
            .hasMessage("Setting a password to a ROLE is not allowed");

        // Set NULL - reset
        assertThatThrownBy(() -> TransportAlterRole.alterRole(
            mdBuilder,
            "DummyRole",
            null,
            null,
            true,
            false,
            Map.of()))
            .isExactlyInstanceOf(UnsupportedFeatureException.class)
            .hasMessage("Setting a password to a ROLE is not allowed");
    }

    @Test
    public void test_alter_user_cannot_set_jwt_to_role() throws Exception {
        var oldRolesMetadata = new RolesMetadata(DUMMY_USERS_AND_ROLES_WITHOUT_PASSWORD);
        Metadata.Builder mdBuilder = Metadata.builder()
            .putCustom(RolesMetadata.TYPE, oldRolesMetadata);
        // Set new jwt
        assertThatThrownBy(() -> TransportAlterRole.alterRole(
            mdBuilder,
            "DummyRole",
            null,
            new JwtProperties("iss", "username", null),
            false,
            false,
            Map.of()))
            .isExactlyInstanceOf(UnsupportedFeatureException.class)
            .hasMessage("Setting JWT properties to a ROLE is not allowed");

        // Set NULL - reset
        assertThatThrownBy(() -> TransportAlterRole.alterRole(
            mdBuilder,
            "DummyRole",
            null,
            null,
            false,
            true,
            Map.of()))
            .isExactlyInstanceOf(UnsupportedFeatureException.class)
            .hasMessage("Setting JWT properties to a ROLE is not allowed");
    }

    @Test
    public void test_alter_user_change_jwt_and_keep_password() throws Exception {
        Map<String, Role> roleWithJwtAndPassword = new HashMap<>();
        var oldPassword = getSecureHash("johns-pwd"); // Has randomness, keep it for assertions.
        roleWithJwtAndPassword.put("John", userOf(
            "John",
            Set.of(),
            new HashSet<>(),
            oldPassword,
            new JwtProperties("https:dummy.org", "test", null))
        );
        var oldRolesMetadata = new RolesMetadata(roleWithJwtAndPassword);
        Metadata.Builder mdBuilder = Metadata.builder()
            .putCustom(RolesMetadata.TYPE, oldRolesMetadata);
        boolean exists = TransportAlterRole.alterRole(
            mdBuilder,
            "John",
            null,
            new JwtProperties("new_issuer", "new_username", null),
            false, // No reset, keep pwd
            false,
            Map.of()
        );
        assertThat(exists).isTrue();
        assertThat(roles(mdBuilder)).containsExactlyInAnyOrderEntriesOf(
            Map.of("John", userOf(
                    "John",
                    Set.of(),
                    new HashSet<>(),
                    oldPassword,
                    new JwtProperties("new_issuer", "new_username", null))
            )
        );
    }

    @Test
    public void test_alter_user_change_or_reset_password_and_keep_jwt() {
        Map<String, Role> roleWithJwtAndPassword = new HashMap<>();
        var oldJwtProperties = new JwtProperties("https:dummy.org", "test", null);
        roleWithJwtAndPassword.put("John", userOf(
            "John",
            Set.of(),
            new HashSet<>(),
            getSecureHash("old-pwd"),
            oldJwtProperties
           )
        );
        var oldRolesMetadata = new RolesMetadata(roleWithJwtAndPassword);
        Metadata.Builder mdBuilder = Metadata.builder()
            .putCustom(RolesMetadata.TYPE, oldRolesMetadata);
        var newPwd = getSecureHash("new-pwd");

        // Update password
        boolean exists = TransportAlterRole.alterRole(
            mdBuilder,
            "John",
            newPwd,
            null,
            false,
            false, // No reset, keep jwt
            Map.of()
        );
        assertThat(exists).isTrue();
        assertThat(roles(mdBuilder)).containsExactlyInAnyOrderEntriesOf(
            Map.of("John", userOf(
                "John",
                Set.of(),
                new HashSet<>(),
                newPwd,
                oldJwtProperties)
            )
        );

        // Reset password
        exists = TransportAlterRole.alterRole(
            mdBuilder,
            "John",
            null,
            null,
            true, // Reset password
            false, // No reset, keep jwt
            Map.of()
        );
        assertThat(exists).isTrue();
        assertThat(roles(mdBuilder)).containsExactlyInAnyOrderEntriesOf(
            Map.of("John", userOf(
                "John",
                Set.of(),
                new HashSet<>(),
                null, // Password has been reset
                oldJwtProperties)
            )
        );
    }

    @Test
    public void test_alter_user_reset_jwt_and_password() throws Exception {
        Map<String, Role> roleWithJwtAndPassword = new HashMap<>();
        var oldPassword = getSecureHash("johns-pwd"); // Has randomness, keep it for assertions.
        roleWithJwtAndPassword.put("John", userOf(
            "John",
            Set.of(),
            new HashSet<>(),
            oldPassword,
            new JwtProperties("https:dummy.org", "test", null))
        );
        var oldRolesMetadata = new RolesMetadata(roleWithJwtAndPassword);
        Metadata.Builder mdBuilder = Metadata.builder()
            .putCustom(RolesMetadata.TYPE, oldRolesMetadata);
        boolean exists = TransportAlterRole.alterRole(
            mdBuilder,
            "John",
            null,
            null,
            true,
            true,
            Map.of()
        );
        assertThat(exists).isTrue();
        assertThat(roles(mdBuilder)).containsExactlyInAnyOrderEntriesOf(
            Map.of("John", userOf(
                    "John",
                    Set.of(),
                    new HashSet<>(),
                    null,
                    null
                )
            )
        );
    }

    @Test
    public void test_alter_user_throws_error_on_jwt_properties_clash() throws Exception {
        Map<String, Role> roleWithJwtAndPassword = new HashMap<>();
        // Users have different "aud" property values - still clashing as only iss/username matters.
        roleWithJwtAndPassword.put("another_user_causing_clash", userOf(
            "another_user_causing_clash",
            Set.of(),
            new HashSet<>(),
            null,
            new JwtProperties("https:dummy.org", "test", "aud1"))
        );
        roleWithJwtAndPassword.put("John", userOf(
            "John",
            Set.of(),
            new HashSet<>(),
            null,
            new JwtProperties("john's valid iss", "john's valid username", "aud2"))
        );
        var oldRolesMetadata = new RolesMetadata(roleWithJwtAndPassword);
        Metadata.Builder mdBuilder = Metadata.builder()
            .putCustom(RolesMetadata.TYPE, oldRolesMetadata);
        assertThatThrownBy(() -> TransportAlterRole.alterRole(
            mdBuilder,
            "John",
            null,
            new JwtProperties("https:dummy.org", "test", null),
            false,
            false,
            Map.of()))
            .isExactlyInstanceOf(RoleAlreadyExistsException.class)
            .hasMessage("Another role with the same combination of iss/username jwt properties already exists");
    }

    @Test
    public void test_alter_user_add_session_setting() throws Exception {
        Map<String, Role> role = new HashMap<>();
        var password = getSecureHash("johns-pwd"); // Has randomness, keep it for assertions.
        role.put("John", userOf(
            "John",
            password,
            Map.of("search_path", "my_schema"))
        );
        var oldRolesMetadata = new RolesMetadata(role);
        Metadata.Builder mdBuilder = Metadata.builder()
            .putCustom(RolesMetadata.TYPE, oldRolesMetadata);
        boolean exists = TransportAlterRole.alterRole(
            mdBuilder,
            "John",
            null,
            null,
            false,
            false,
            Map.of(false, Map.of("enable_hashjoin", false))
        );
        assertThat(exists).isTrue();
        assertThat(roles(mdBuilder)).containsExactlyEntriesOf(
            Map.of("John", userOf(
                    "John",
                    password,
                    Map.of("search_path", "my_schema", "enable_hashjoin", false)
                )
            )
        );
    }

    @Test
    public void test_alter_user_add_and_override_session_setting() throws Exception {
        Map<String, Role> role = new HashMap<>();
        var password = getSecureHash("johns-pwd"); // Has randomness, keep it for assertions.
        role.put("John", userOf(
            "John",
            password,
            Map.of("search_path", "my_schema"))
        );
        var oldRolesMetadata = new RolesMetadata(role);
        Metadata.Builder mdBuilder = Metadata.builder()
            .putCustom(RolesMetadata.TYPE, oldRolesMetadata);
        boolean exists = TransportAlterRole.alterRole(
            mdBuilder,
            "John",
            null,
            null,
            false,
            false,
            Map.of(false, Map.of("enable_hashjoin", false, "search_path", "schema1, schema2"))
        );
        assertThat(exists).isTrue();
        assertThat(roles(mdBuilder)).containsExactlyEntriesOf(
            Map.of("John", userOf(
                    "John",
                    password,
                    Map.of("search_path", "schema1, schema2", "enable_hashjoin", false)
                )
            )
        );
    }

    @Test
    public void test_alter_user_reset_session_setting() throws Exception {
        Map<String, Role> role = new HashMap<>();
        var password = getSecureHash("johns-pwd"); // Has randomness, keep it for assertions.
        role.put("John", userOf(
            "John",
            password,
            Map.of("search_path", "my_schema", "enable_hashjoin", false))
        );
        var oldRolesMetadata = new RolesMetadata(role);
        Metadata.Builder mdBuilder = Metadata.builder()
            .putCustom(RolesMetadata.TYPE, oldRolesMetadata);
        boolean exists = TransportAlterRole.alterRole(
            mdBuilder,
            "John",
            null,
            null,
            false,
            false,
            Map.of(true, Map.of("search_path", Literal.NULL))
        );
        assertThat(exists).isTrue();
        assertThat(roles(mdBuilder)).containsExactlyEntriesOf(
            Map.of("John", userOf(
                    "John",
                    password,
                    Map.of("enable_hashjoin", false)
                )
            )
        );
    }

    @Test
    public void test_alter_user_reset_all_session_settings() throws Exception {
        Map<String, Role> role = new HashMap<>();
        var password = getSecureHash("johns-pwd"); // Has randomness, keep it for assertions.
        role.put("John", userOf(
            "John",
            password,
            Map.of("search_path", "my_schema", "enable_hashjoin", false))
        );
        var oldRolesMetadata = new RolesMetadata(role);
        Metadata.Builder mdBuilder = Metadata.builder()
            .putCustom(RolesMetadata.TYPE, oldRolesMetadata);
        boolean exists = TransportAlterRole.alterRole(
            mdBuilder,
            "John",
            null,
            null,
            false,
            false,
            Map.of(true, Map.of())
        );
        assertThat(exists).isTrue();
        assertThat(roles(mdBuilder)).containsExactlyEntriesOf(
            Map.of("John", userOf(
                    "John",
                    password,
                    Map.of()
                )
            )
        );
    }

    @Test
    public void test_cannot_set_session_settings_to_a_role() throws Exception {
        var oldRolesMetadata = new RolesMetadata(DUMMY_USERS_AND_ROLES_WITHOUT_PASSWORD);
        Metadata.Builder mdBuilder = Metadata.builder()
            .putCustom(RolesMetadata.TYPE, oldRolesMetadata);
        assertThatThrownBy(() -> TransportAlterRole.alterRole(
            mdBuilder,
            "DummyRole",
            null,
            null,
            false,
            false,
            Map.of(false, Map.of("error_on_unknown_object_key", false))))
            .isExactlyInstanceOf(UnsupportedFeatureException.class)
            .hasMessage("Setting or resetting session settings to a ROLE is not allowed");
    }

    @Test
    public void test_cannot_reset_session_settings_to_a_role() throws Exception {
        var oldRolesMetadata = new RolesMetadata(DUMMY_USERS_AND_ROLES_WITHOUT_PASSWORD);
        Metadata.Builder mdBuilder = Metadata.builder()
            .putCustom(RolesMetadata.TYPE, oldRolesMetadata);
        // Reset ALL
        assertThatThrownBy(() -> TransportAlterRole.alterRole(
            mdBuilder,
            "DummyRole",
            null,
            null,
            false,
            false,
            Map.of(true, Map.of("error_on_unknown_object_key", Literal.NULL))))
            .isExactlyInstanceOf(UnsupportedFeatureException.class)
            .hasMessage("Setting or resetting session settings to a ROLE is not allowed");

        // Reset ALL
        assertThatThrownBy(() -> TransportAlterRole.alterRole(
            mdBuilder,
            "DummyRole",
            null,
            null,
            false,
            false,
            Map.of(true, Map.of())))
            .isExactlyInstanceOf(UnsupportedFeatureException.class)
            .hasMessage("Setting or resetting session settings to a ROLE is not allowed");
    }

    private static Map<String, Role> roles(Metadata.Builder mdBuilder) {
        return ((RolesMetadata) mdBuilder.getCustom(RolesMetadata.TYPE)).roles();
    }
}
