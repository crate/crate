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

import static io.crate.role.metadata.RolesHelper.DUMMY_USERS;
import static io.crate.role.metadata.RolesHelper.DUMMY_USERS_AND_ROLES;
import static io.crate.role.metadata.RolesHelper.OLD_DUMMY_USERS_PRIVILEGES;
import static io.crate.role.metadata.RolesHelper.getSecureHash;
import static io.crate.role.metadata.RolesHelper.roleOf;
import static io.crate.role.metadata.RolesHelper.userOf;
import static io.crate.role.metadata.RolesHelper.usersMetadataOf;
import static io.crate.testing.Asserts.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.ElasticsearchParseException;
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
import io.crate.role.GrantedRolesChange;
import io.crate.role.JwtProperties;
import io.crate.role.Permission;
import io.crate.role.Policy;
import io.crate.role.Privilege;
import io.crate.role.Role;
import io.crate.role.RolePrivileges;
import io.crate.role.Securable;

public class RolesMetadataTest extends ESTestCase {

    private final Map<String, Role> DummyUsersAndRolesWithParentRoles = new HashMap<>();
    private final Set<GrantedRole> DummyParentRoles = Set.of(
        new GrantedRole("role1", "theGrantor"),
        new GrantedRole("role2", "theGrantor")
    );

    @Before
    public void setupUsersAndRoles() {
        DummyUsersAndRolesWithParentRoles.put("Ford", userOf(
            "Ford",
            Set.of(),
            DummyParentRoles,
            getSecureHash("fords-pwd")));
        DummyUsersAndRolesWithParentRoles.put("John", userOf(
            "John",
            Set.of(),
            new HashSet<>(),
            getSecureHash("johns-pwd"),
            new JwtProperties("https:dummy.org", "test", null))
        );
        DummyUsersAndRolesWithParentRoles.put("role1", roleOf("role1"));
        DummyUsersAndRolesWithParentRoles.put("role2", roleOf("role2"));
        DummyUsersAndRolesWithParentRoles.put("role3", roleOf("role3"));
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
            usersMetadataOf(DUMMY_USERS),
            new UsersPrivilegesMetadata(OLD_DUMMY_USERS_PRIVILEGES)
        );
        assertThat(rolesMetadata.roles()).containsExactlyInAnyOrderEntriesOf(
            Map.of("Arthur", DUMMY_USERS.get("Arthur").with(new RolePrivileges(OLD_DUMMY_USERS_PRIVILEGES.get("Arthur"))),
                "Ford", DUMMY_USERS.get("Ford").with(new RolePrivileges(OLD_DUMMY_USERS_PRIVILEGES.get("Ford")))));
    }

    @Test
    public void test_roles_metadata_from_cluster_state() {
        var oldUsersMetadata = usersMetadataOf(DUMMY_USERS);
        var oldUserPrivilegesMetadata = new UsersPrivilegesMetadata(OLD_DUMMY_USERS_PRIVILEGES);
        var oldRolesMetadata = new RolesMetadata(DUMMY_USERS_AND_ROLES);
        Metadata.Builder mdBuilder = new Metadata.Builder()
            .putCustom(UsersMetadata.TYPE, oldUsersMetadata)
            .putCustom(RolesMetadata.TYPE, oldRolesMetadata);
        var newRolesMetadata = RolesMetadata.of(mdBuilder, oldUsersMetadata, oldUserPrivilegesMetadata, oldRolesMetadata);
        assertThat(newRolesMetadata.roles()).containsExactlyInAnyOrderEntriesOf(
            Map.of("Arthur", DUMMY_USERS.get("Arthur").with(new RolePrivileges(OLD_DUMMY_USERS_PRIVILEGES.get("Arthur"))),
                "Ford", DUMMY_USERS.get("Ford").with(new RolePrivileges(OLD_DUMMY_USERS_PRIVILEGES.get("Ford")))));
    }

    @Test
    public void test_grant_roles_to_user() {
        var rolesMetadata = new RolesMetadata(DummyUsersAndRolesWithParentRoles);
        var affectedRolePrivileges = rolesMetadata.applyRolePrivileges(List.of("Ford", "John"), new GrantedRolesChange(
            Policy.GRANT,
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
        var affectedRolePrivileges = rolesMetadata.applyRolePrivileges(List.of("Ford", "John"), new GrantedRolesChange(
            Policy.REVOKE,
            Set.of("role1", "role3"),
            "theGrantor"));
        assertThat(affectedRolePrivileges).isEqualTo(1);
        assertThat(rolesMetadata.roles().get("Ford").grantedRoles()).containsExactlyInAnyOrder(
            new GrantedRole("role2", "theGrantor"));
        assertThat(rolesMetadata.roles().get("John").grantedRoles()).isEmpty();
    }

    @Test
    public void test_jwt_properties_from_invalid_x_content() throws IOException {
        XContentBuilder xContentBuilder = JsonXContent.builder();
        xContentBuilder.startObject();
        xContentBuilder.field("iss", 1);
        xContentBuilder.field("username", "test");
        xContentBuilder.endObject();

        XContentParser parser = JsonXContent.JSON_XCONTENT.createParser(
            xContentRegistry(),
            DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
            Strings.toString(xContentBuilder));

        XContentParser finalParser = parser;
        assertThatThrownBy(() -> JwtProperties.fromXContent(finalParser))
            .isExactlyInstanceOf(ElasticsearchParseException.class)
            .hasMessage("failed to parse jwt, 'iss' value is not a string [VALUE_NUMBER]");

        xContentBuilder = JsonXContent.builder();
        xContentBuilder.startObject();
        xContentBuilder.field("iss", "dummy");
        xContentBuilder.field("username", 2);
        xContentBuilder.endObject();

        parser = JsonXContent.JSON_XCONTENT.createParser(
            xContentRegistry(),
            DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
            Strings.toString(xContentBuilder));

        XContentParser finalParser1 = parser;
        assertThatThrownBy(() -> JwtProperties.fromXContent(finalParser1))
            .isExactlyInstanceOf(ElasticsearchParseException.class)
            .hasMessage("failed to parse jwt, 'username' value is not a string [VALUE_NUMBER]");

        xContentBuilder = JsonXContent.builder();
        xContentBuilder.startObject();
        xContentBuilder.field("iss", "dummy");
        xContentBuilder.field("username", "dummy");
        xContentBuilder.field("aud", 5);
        xContentBuilder.endObject();

        parser = JsonXContent.JSON_XCONTENT.createParser(
            xContentRegistry(),
            DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
            Strings.toString(xContentBuilder));

        XContentParser finalParser2 = parser;
        assertThatThrownBy(() -> JwtProperties.fromXContent(finalParser2))
            .isExactlyInstanceOf(ElasticsearchParseException.class)
            .hasMessage("failed to parse jwt, 'aud' value is not a string [VALUE_NUMBER]");

        xContentBuilder = JsonXContent.builder();
        xContentBuilder.startObject();
        xContentBuilder.field("prop", "dummy");
        xContentBuilder.endObject();

        parser = JsonXContent.JSON_XCONTENT.createParser(
            xContentRegistry(),
            DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
            Strings.toString(xContentBuilder));

        XContentParser finalParser3 = parser;
        assertThatThrownBy(() -> JwtProperties.fromXContent(finalParser3))
            .isExactlyInstanceOf(ElasticsearchParseException.class)
            .hasMessage("failed to parse jwt, unknown property 'prop'");

    }

    @Test
    public void test_grant_roles_do_not_loose_existing_privileges() {
        var rolesMetadata = new RolesMetadata();
        rolesMetadata.roles().put("Ford", userOf(
            "Ford",
            Set.of(new Privilege(Policy.GRANT, Permission.DQL, Securable.CLUSTER, null, "crate")),
            Set.of(),
            getSecureHash("fords-pwd"))
        );
        rolesMetadata.roles().put("role1", roleOf("role1"));
        assertThat(rolesMetadata.roles().get("Ford").privileges().size()).isEqualTo(1);

        var affectedRolePrivileges = rolesMetadata.applyRolePrivileges(List.of("Ford"), new GrantedRolesChange(
            Policy.GRANT,
            Set.of("role1"),
            "theGrantor"));

        assertThat(affectedRolePrivileges).isEqualTo(1);
        assertThat(rolesMetadata.roles().get("Ford").privileges().size()).isEqualTo(1);
    }
}
