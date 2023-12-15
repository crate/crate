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

package io.crate.role.metadata;

import static io.crate.role.metadata.RolesHelper.OLD_DUMMY_USERS_PRIVILEGES;
import static io.crate.role.metadata.RolesHelper.usersMetadataOf;
import static io.crate.testing.Asserts.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;
import org.junit.Test;

import io.crate.role.GrantedRole;
import io.crate.role.PrivilegeState;
import io.crate.role.Role;
import io.crate.role.RolePrivilegeToApply;

public class RolesMetadataTest extends ESTestCase {

    private final Map<String, Role> DummyUsersAndRolesWithParentRoles = new HashMap<>();
    private final Set<GrantedRole> DummyParentRoles = Set.of(
        new GrantedRole("role1", "theGrantor"),
        new GrantedRole("role2", "theGrantor")
    );

    @Before
    public void setupUsersAndRoles() {
        DummyUsersAndRolesWithParentRoles.put("Ford", RolesHelper.userOf(
            "Ford",
            Set.of(),
            DummyParentRoles,
            RolesHelper.getSecureHash("fords-pwd")));
        DummyUsersAndRolesWithParentRoles.put("John", RolesHelper.userOf(
            "John",
            Set.of(),
            new HashSet<>(),
            RolesHelper.getSecureHash("johns-pwd")));
        DummyUsersAndRolesWithParentRoles.put("role1", RolesHelper.roleOf("role1"));
        DummyUsersAndRolesWithParentRoles.put("role2", RolesHelper.roleOf("role2"));
        DummyUsersAndRolesWithParentRoles.put("role3", RolesHelper.roleOf("role3"));
    }

    @Test
    public void test_roles_metadata_streaming() throws IOException {
        RolesMetadata roles = new RolesMetadata(DummyUsersAndRolesWithParentRoles);
        BytesStreamOutput out = new BytesStreamOutput();
        roles.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        RolesMetadata roles2 = new RolesMetadata(in);
        assertThat(roles2).isEqualTo(roles);
    }

    @Test
    public void test_roles_metadata_to_x_content() throws IOException {
        XContentBuilder builder = JsonXContent.builder();

        // reflects the logic used to process custom metadata in the cluster state
        builder.startObject();

        RolesMetadata roles = new RolesMetadata(DummyUsersAndRolesWithParentRoles);
        roles.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        XContentParser parser = JsonXContent.JSON_XCONTENT.createParser(
            xContentRegistry(),
            DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
            Strings.toString(builder));
        parser.nextToken(); // start object
        RolesMetadata roles2 = RolesMetadata.fromXContent(parser);
        assertThat(roles2).isEqualTo(roles);

        // a metadata custom must consume the surrounded END_OBJECT token, no token must be left
        assertThat(parser.nextToken()).isNull();
    }

    @Test
    public void test_roles_metadata_without_attributes_to_xcontent() throws IOException {
        XContentBuilder builder = JsonXContent.builder();

        // reflects the logic used to process custom metadata in the cluster state
        builder.startObject();

        RolesMetadata roles = new RolesMetadata(DummyUsersAndRolesWithParentRoles);
        roles.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        XContentParser parser = JsonXContent.JSON_XCONTENT.createParser(
            xContentRegistry(),
            DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
            Strings.toString(builder));
        parser.nextToken(); // start object
        RolesMetadata roles2 = RolesMetadata.fromXContent(parser);
        assertThat(roles2).isEqualTo(roles);

        // a metadata custom must consume the surrounded END_OBJECT token, no token must be left
        assertThat(parser.nextToken()).isNull();
    }

    @Test
    public void test_roles_metadata_with_attributes_streaming() throws Exception {
        RolesMetadata writeRolesMeta = new RolesMetadata(DummyUsersAndRolesWithParentRoles);
        BytesStreamOutput out = new BytesStreamOutput();
        writeRolesMeta.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        RolesMetadata readRolesMeta = new RolesMetadata(in);

        assertThat(readRolesMeta.roles()).isEqualTo(writeRolesMeta.roles());
    }

    @Test
    public void test_add_old_users_metadata_to_roles_metadata() {
        RolesMetadata rolesMetadata = RolesMetadata.ofOldUsersMetadata(
            usersMetadataOf(RolesHelper.DUMMY_USERS),
            new UsersPrivilegesMetadata(OLD_DUMMY_USERS_PRIVILEGES)
        );
        assertThat(rolesMetadata.roles()).containsExactlyInAnyOrderEntriesOf(
            Map.of("Arthur", RolesHelper.DUMMY_USERS.get("Arthur").with(OLD_DUMMY_USERS_PRIVILEGES.get("Arthur")),
                "Ford", RolesHelper.DUMMY_USERS.get("Ford").with(OLD_DUMMY_USERS_PRIVILEGES.get("Ford"))));
    }

    @Test
    public void test_roles_metadata_from_cluster_state() {
        var oldUsersMetadata = usersMetadataOf(RolesHelper.DUMMY_USERS);
        var oldUserPrivilegesMetadata = new UsersPrivilegesMetadata(OLD_DUMMY_USERS_PRIVILEGES);
        var oldRolesMetadata = new RolesMetadata(RolesHelper.DUMMY_USERS_AND_ROLES);
        Metadata.Builder mdBuilder = new Metadata.Builder()
            .putCustom(UsersMetadata.TYPE, oldUsersMetadata)
            .putCustom(RolesMetadata.TYPE, oldRolesMetadata);
        var newRolesMetadata = RolesMetadata.of(mdBuilder, oldUsersMetadata, oldUserPrivilegesMetadata, oldRolesMetadata);
        assertThat(newRolesMetadata.roles()).containsExactlyInAnyOrderEntriesOf(
            Map.of("Arthur", RolesHelper.DUMMY_USERS.get("Arthur").with(OLD_DUMMY_USERS_PRIVILEGES.get("Arthur")),
                "Ford", RolesHelper.DUMMY_USERS.get("Ford").with(OLD_DUMMY_USERS_PRIVILEGES.get("Ford"))));
    }

    @Test
    public void test_grant_revoke_user_to_another_user_is_not_allowed() {
        var rolesMetadata = new RolesMetadata(DummyUsersAndRolesWithParentRoles);
        for (PrivilegeState state : List.of(PrivilegeState.GRANT, PrivilegeState.REVOKE)) {
            assertThatThrownBy(() -> rolesMetadata.applyPrivileges(
                List.of("Ford"), new RolePrivilegeToApply(state, Set.of("John"), null)))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("Cannot " + state + " a USER to a ROLE");
        }
    }

    @Test
    public void test_grant_roles_to_user() {
        var rolesMetadata = new RolesMetadata(DummyUsersAndRolesWithParentRoles);
        var affectedRolePrivileges = rolesMetadata.applyPrivileges(List.of("Ford", "John"), new RolePrivilegeToApply(
            PrivilegeState.GRANT,
            Set.of("role1", "role3"),
            "theGrantor"));
        assertThat(affectedRolePrivileges).isEqualTo(3);
        assertThat(rolesMetadata.roles().get("Ford").grantedRoles()).containsExactlyInAnyOrder(
            new GrantedRole("role1", "theGrantor"),
            new GrantedRole("role2", "theGrantor"),
            new GrantedRole("role3", "theGrantor"));
        assertThat(rolesMetadata.roles().get("John").grantedRoles()).containsExactlyInAnyOrder(
            new GrantedRole("role1", "theGrantor"),
            new GrantedRole("role3", "theGrantor"));
    }

    @Test
    public void test_revoke_roles_from_user() {
        var rolesMetadata = new RolesMetadata(DummyUsersAndRolesWithParentRoles);
        var affectedRolePrivileges = rolesMetadata.applyPrivileges(List.of("Ford", "John"), new RolePrivilegeToApply(
            PrivilegeState.REVOKE,
            Set.of("role1", "role3"),
            "theGrantor"));
        assertThat(affectedRolePrivileges).isEqualTo(1);
        assertThat(rolesMetadata.roles().get("Ford").grantedRoles()).containsExactlyInAnyOrder(
            new GrantedRole("role2", "theGrantor"));
        assertThat(rolesMetadata.roles().get("John").grantedRoles()).isEmpty();
    }
}
