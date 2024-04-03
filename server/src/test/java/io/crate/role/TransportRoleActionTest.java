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
import static org.assertj.core.api.Assertions.assertThat;
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
import io.crate.fdw.AddServerTask;
import io.crate.fdw.CreateServerRequest;
import io.crate.role.metadata.RolesHelper;
import io.crate.role.metadata.RolesMetadata;
import io.crate.role.metadata.UsersMetadata;
import io.crate.role.metadata.UsersPrivilegesMetadata;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class TransportRoleActionTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void testCreateFirstUser() throws Exception {
        Metadata.Builder mdBuilder = new Metadata.Builder();
        TransportCreateRoleAction.putRole(mdBuilder,
            "root",
            true,
            null,
            new JwtProperties("https:dummy.org", "test", "test_aud"));
        RolesMetadata metadata = (RolesMetadata) mdBuilder.getCustom(RolesMetadata.TYPE);
        assertThat(metadata.roleNames()).containsExactly("root");
        var jwtProps = metadata.roles().get("root").jwtProperties();
        assertThat(jwtProps).isNotNull();
        assertThat(jwtProps.iss()).isEqualTo("https:dummy.org");
        assertThat(jwtProps.username()).isEqualTo("test");
        assertThat(jwtProps.aud()).isEqualTo("test_aud");
    }

    @Test
    public void test_create_user_with_matching_jwt_props_exists() throws Exception {
        // Users have different "aud" property values - still clashing as only iss/username matters.
        Metadata.Builder mdBuilder = new Metadata.Builder();
        TransportCreateRoleAction.putRole(mdBuilder,
            "user1",
            true,
            null,
            new JwtProperties("https:dummy.org", "test", "aud1"));

        assertThatThrownBy(() -> TransportCreateRoleAction.putRole(mdBuilder,
                "user2",
                true,
                null,
                new JwtProperties("https:dummy.org", "test", "aud2")))
            .isExactlyInstanceOf(RoleAlreadyExistsException.class)
            .hasMessage("Another role with the same combination of iss/username jwt properties already exists");
    }

    @Test
    public void test_create_user_with_existing_name_but_different_jwt_props() throws Exception {
        Metadata.Builder mdBuilder = new Metadata.Builder();
        TransportCreateRoleAction.putRole(mdBuilder,
            "user1",
            true,
            null,
            new JwtProperties("https:dummy.org", "test", null));

        boolean exists = TransportCreateRoleAction.putRole(mdBuilder,
            "user1",
            true,
            null,
            new JwtProperties("https:dummy.org", "test2", null));
        assertThat(exists).isTrue();
    }

    @Test
    public void testCreateUserAlreadyExists() throws Exception {
        Metadata.Builder mdBuilder = new Metadata.Builder()
            .putCustom(RolesMetadata.TYPE, new RolesMetadata(SINGLE_USER_ONLY));
        assertThat(TransportCreateRoleAction.putRole(mdBuilder, "Arthur", true, null, null)).isTrue();
    }

    @Test
    public void testCreateUser() throws Exception {
        Metadata.Builder mdBuilder = new Metadata.Builder()
            .putCustom(RolesMetadata.TYPE, new RolesMetadata(SINGLE_USER_ONLY));
        TransportCreateRoleAction.putRole(mdBuilder, "Trillian", true, null, null);
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
        boolean res = TransportCreateRoleAction.putRole(mdBuilder, "RoleFoo", false, null, null);
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
        boolean res = TransportAlterRoleAction.alterRole(mdBuilder,
            "Arthur",
            newPasswd,
            null,
            false,
            false
        );
        assertThat(res).isTrue();

        var newFordUser = DUMMY_USERS_WITHOUT_PASSWORD.get("Ford")
                .with(OLD_DUMMY_USERS_PRIVILEGES.get("Ford"));
        var newArthurUser = DUMMY_USERS_WITHOUT_PASSWORD.get("Arthur")
                .with(OLD_DUMMY_USERS_PRIVILEGES.get("Arthur"))
                .with(newPasswd, null);
        assertThat(roles(mdBuilder)).containsExactlyInAnyOrderEntriesOf(
            Map.of("Arthur", newArthurUser,
                "Ford", newFordUser));
    }

    @Test
    public void testDropUserNoUsersAtAll() throws Exception {
        var currentState = clusterService.state();
        DropRoleTask dropRoleTask = new DropRoleTask(new DropRoleRequest("root", true));
        var updatedMetadata = dropRoleTask.execute(currentState).metadata();
        assertThat((RolesMetadata) currentState.metadata().custom(RolesMetadata.TYPE)).isNull();
        assertThat((RolesMetadata) updatedMetadata.custom(RolesMetadata.TYPE)).isNull();
    }

    @Test
    public void testDropNonExistingUser() throws Exception {
        Metadata metadata = Metadata.builder().putCustom(RolesMetadata.TYPE, new RolesMetadata(SINGLE_USER_ONLY)).build();
        ClusterState currentState = ClusterState.builder(clusterService.state()).metadata(metadata).build();

        DropRoleTask dropRoleTask = new DropRoleTask(new DropRoleRequest("trillian", false));
        var updatedMetadata = dropRoleTask.execute(currentState).metadata();
        assertThat(roles(Metadata.builder(updatedMetadata))).isEqualTo(roles(Metadata.builder(currentState.metadata())));
    }

    @Test
    public void testDropUser() throws Exception {
        Metadata metadata = Metadata.builder().putCustom(RolesMetadata.TYPE, new RolesMetadata(DUMMY_USERS)).build();
        ClusterState clusterState = ClusterState.builder(clusterService.state()).metadata(metadata).build();

        DropRoleTask dropRoleTask = new DropRoleTask(new DropRoleRequest("Arthur", false));
        var updatedMetadata = Metadata.builder(dropRoleTask.execute(clusterState).metadata());
        assertThat(roles(updatedMetadata)).containsExactlyEntriesOf(Map.of("Ford", DUMMY_USERS.get("Ford")));
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
        ClusterState clusterState = ClusterState.builder(clusterService.state()).metadata(mdBuilder.build()).build();

        DropRoleTask dropRoleTask = new DropRoleTask(new DropRoleRequest("role2", false));
        assertThatThrownBy(() -> dropRoleTask.execute(clusterState))
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
        ClusterState clusterState = ClusterState.builder(clusterService.state()).metadata(mdBuilder.build()).build();

        DropRoleTask dropRoleTask = new DropRoleTask(new DropRoleRequest("Arthur", false));
        var updatedMetadata = Metadata.builder(dropRoleTask.execute(clusterState).metadata());
        assertThat(roles(updatedMetadata)).containsExactlyEntriesOf(Map.of("Ford", DUMMY_USERS.get("Ford")));
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
        assertThatThrownBy(() -> TransportAlterRoleAction.alterRole(
            mdBuilder,
            "DummyRole",
            getSecureHash("new-passwd"),
            null,
            false,
            false))
            .isExactlyInstanceOf(UnsupportedFeatureException.class)
            .hasMessage("Setting a password to a ROLE is not allowed");

        // Set NULL - reset
        assertThatThrownBy(() -> TransportAlterRoleAction.alterRole(
            mdBuilder,
            "DummyRole",
            null,
            null,
            true,
            false))
            .isExactlyInstanceOf(UnsupportedFeatureException.class)
            .hasMessage("Setting a password to a ROLE is not allowed");
    }

    @Test
    public void test_alter_user_cannot_set_jwt_to_role() throws Exception {
        var oldRolesMetadata = new RolesMetadata(DUMMY_USERS_AND_ROLES_WITHOUT_PASSWORD);
        Metadata.Builder mdBuilder = Metadata.builder()
            .putCustom(RolesMetadata.TYPE, oldRolesMetadata);
        // Set new jwt
        assertThatThrownBy(() -> TransportAlterRoleAction.alterRole(
            mdBuilder,
            "DummyRole",
            null,
            new JwtProperties("iss", "username", null),
            false,
            false))
            .isExactlyInstanceOf(UnsupportedFeatureException.class)
            .hasMessage("Setting JWT properties to a ROLE is not allowed");

        // Set NULL - reset
        assertThatThrownBy(() -> TransportAlterRoleAction.alterRole(
            mdBuilder,
            "DummyRole",
            null,
            null,
            false,
            true))
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
        boolean exists = TransportAlterRoleAction.alterRole(
            mdBuilder,
            "John",
            null,
            new JwtProperties("new_issuer", "new_username", null),
            false, // No reset, keep pwd
            false
        );
        assertThat(exists).isTrue();
        assertThat(roles(mdBuilder)).containsExactlyInAnyOrderEntriesOf(
            Map.of("John", userOf(
                    "John",
                    Set.of(),
                    new HashSet<>(),
                    oldPassword,
                    new JwtProperties("new_issuer", "new_username", null)
                )
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
        boolean exists = TransportAlterRoleAction.alterRole(
            mdBuilder,
            "John",
            null,
            null,
            true,
            true
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
        assertThatThrownBy(() -> TransportAlterRoleAction.alterRole(
            mdBuilder,
            "John",
            null,
            new JwtProperties("https:dummy.org", "test", null),
            false,
            false))
            .isExactlyInstanceOf(RoleAlreadyExistsException.class)
            .hasMessage("Another role with the same combination of iss/username jwt properties already exists");
    }


    private static Map<String, Role> roles(Metadata.Builder mdBuilder) {
        return ((RolesMetadata) mdBuilder.build().custom(RolesMetadata.TYPE)).roles();
    }
}
