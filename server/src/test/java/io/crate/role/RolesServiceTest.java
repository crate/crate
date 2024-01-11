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

import static io.crate.role.Role.CRATE_USER;
import static io.crate.role.metadata.RolesHelper.DUMMY_USERS_AND_ROLES;
import static io.crate.role.metadata.RolesHelper.roleOf;
import static io.crate.role.metadata.RolesHelper.userOf;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.cluster.service.ClusterService;
import org.junit.Test;

import io.crate.metadata.pgcatalog.OidHash;
import io.crate.role.metadata.RolesMetadata;
import io.crate.role.metadata.UsersMetadata;
import io.crate.role.metadata.UsersPrivilegesMetadata;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;

public class RolesServiceTest extends CrateDummyClusterServiceUnitTest {

    private static final Map<String, Role> DEFAULT_USERS = Map.of(CRATE_USER.name(), CRATE_USER);

    @Test
    public void testNullAndEmptyMetadata() {
        // the users list will always contain a crate user
        Map<String, Role> roles = RolesService.getRoles(null, null, null);
        assertThat(roles).containsExactlyEntriesOf(DEFAULT_USERS);

        roles = RolesService.getRoles(new UsersMetadata(), new RolesMetadata(), new UsersPrivilegesMetadata());
        assertThat(roles).containsExactlyEntriesOf(DEFAULT_USERS);
    }

    @Test
    public void testUsersAndRoles() {
        Map<String, Role> roles = RolesService.getRoles(
            null,
            new RolesMetadata(DUMMY_USERS_AND_ROLES),
            new UsersPrivilegesMetadata());

        assertThat(roles).containsExactlyInAnyOrderEntriesOf(
            Map.of(
                "Ford", DUMMY_USERS_AND_ROLES.get("Ford"),
                "John", DUMMY_USERS_AND_ROLES.get("John"),
                "DummyRole", roleOf("DummyRole"),
                CRATE_USER.name(), CRATE_USER));
    }

    @Test
    public void test_has_schema_privilege() {
        var sysOid = OidHash.schemaOid("sys");
        var docOid = OidHash.schemaOid("doc");
        var grantDDLCluster = new Privilege(
            PrivilegeState.GRANT,
            Privilege.Type.DDL,
            Securable.CLUSTER,
            null,
            "crate"
        );

        var role1 = roleOf("role", Set.of(grantDDLCluster));
        RolesService roleService = new RolesService(mock(ClusterService.class)) {
            @Override
            public Role findRole(String roleName) {
                return role1;
            }
        };
        assertThat(roleService.hasSchemaPrivilege(role1, Privilege.Type.DDL, sysOid)).isTrue();
        assertThat(roleService.hasSchemaPrivilege(role1, Privilege.Type.DDL, docOid)).isTrue();

        var denyDDLSys = new Privilege(
            PrivilegeState.DENY,
            Privilege.Type.DDL,
            Securable.SCHEMA,
            "sys",
            "crate"
        );
        var role2 = roleOf("role", Set.of(grantDDLCluster, denyDDLSys));
        roleService = new RolesService(mock(ClusterService.class)) {
            @Override
            public Role findRole(String roleName) {
                return role2;
            }
        };
        assertThat(roleService.hasSchemaPrivilege(role2, Privilege.Type.DDL, sysOid)).isFalse();
        assertThat(roleService.hasSchemaPrivilege(role2, Privilege.Type.DDL, docOid)).isTrue();
    }

    @Test
    public void test_old_users_metadata_is_preferred_over_roles_metadata() {
        Map<String, Role> roles = RolesService.getRoles(
            new UsersMetadata(Collections.singletonMap("Arthur", null)),
            new RolesMetadata(DUMMY_USERS_AND_ROLES),
            new UsersPrivilegesMetadata());

        assertThat(roles).containsExactlyInAnyOrderEntriesOf(
            Map.of(
                "Arthur" , userOf("Arthur"),
                CRATE_USER.name(), CRATE_USER));
    }

    @Test
    public void test_resolve_privileges_from_parents() {
        /*
                       role1 (GRANT)
                         |
                       role2
                      /   |
                  role3   |
                   |  \   |
                   |  role4
                   |   |
                   role5
         */
        var privilege = new Privilege(
            PrivilegeState.GRANT,
            Privilege.Type.DDL,
            Securable.SCHEMA,
            "sys",
            "crate"
        );
        var role1 = roleOf("role1", Set.of(privilege));
        var role2 = roleOf("role2", List.of("role1"));
        var role3 = roleOf("role3", List.of("role2"));
        var role4 = roleOf("role4", List.of("role3", "role2"));
        var role5 = roleOf("role5", List.of("role4", "role3"));
        var roles = Map.of(
            "role1", role1,
            "role2", role2,
            "role3", role3,
            "role4", role4,
            "role5", role5
        );

        var roleService = new RolesService(mock(ClusterService.class)) {
            @Override
            public Role findRole(String roleName) {
                return roles.get(roleName);
            }
        };
        for (var role : roles.values()) {
            assertThat(roleService.hasPrivilege(role, Privilege.Type.DDL, Securable.SCHEMA, "sys"))
                .as("role=" + role.name())
                .isTrue();
            assertThat(roleService.hasAnyPrivilege(role, Securable.SCHEMA, "sys"))
                .isTrue();
            assertThat(roleService.hasSchemaPrivilege(role, Privilege.Type.DDL, OidHash.schemaOid("sys")))
                .isTrue();
        }
    }

    @Test
    public void test_resolve_privileges_from_parents_with_deny() {
        /*
                        role1 (GRANT Cluster)
                         /                   \
            role2 (DENY sys - GRANT doc)   role3 (GRANT sys - DENY doc)
                          \                 /
                            role4 (MISSING)
         */
        var grantDDLCluster = new Privilege(
            PrivilegeState.GRANT,
            Privilege.Type.DDL,
            Securable.CLUSTER,
            null,
            "crate"
        );
        var grantDDLSys = new Privilege(
            PrivilegeState.GRANT,
            Privilege.Type.DDL,
            Securable.SCHEMA,
            "sys",
            "crate"
        );
        var grantDDLDoc = new Privilege(
            PrivilegeState.GRANT,
            Privilege.Type.DDL,
            Securable.SCHEMA,
            "doc",
            "crate"
        );
        var denyDDLSys = new Privilege(
            PrivilegeState.DENY,
            Privilege.Type.DDL,
            Securable.SCHEMA,
            "sys",
            "crate"
        );
        var denyDDLDoc = new Privilege(
            PrivilegeState.DENY,
            Privilege.Type.DDL,
            Securable.SCHEMA,
            "doc",
            "crate"
        );
        var role1 = roleOf("role1", Set.of(grantDDLCluster));
        var role2 = roleOf("role2", Set.of(denyDDLSys, grantDDLDoc), List.of("role1"));
        var role3 = roleOf("role3", Set.of(grantDDLSys, denyDDLDoc), List.of("role1"));
        var role4 = roleOf("role4", List.of("role3", "role2"));
        var roles = Map.of(
            "role1", role1,
            "role2", role2,
            "role3", role3,
            "role4", role4);

        var roleService = new RolesService(mock(ClusterService.class)) {
            @Override
            public Role findRole(String roleName) {
                return roles.get(roleName);
            }
        };

        assertThat(roleService.hasPrivilege(role1, Privilege.Type.DDL, Securable.SCHEMA, "sys")).isTrue();
        assertThat(roleService.hasPrivilege(role1, Privilege.Type.DDL, Securable.SCHEMA, "doc")).isTrue();
        assertThat(roleService.hasAnyPrivilege(role1, Securable.SCHEMA, "sys")).isTrue();
        assertThat(roleService.hasAnyPrivilege(role1, Securable.SCHEMA, "doc")).isTrue();
        assertThat(roleService.hasSchemaPrivilege(role1, Privilege.Type.DDL, OidHash.schemaOid("sys"))).isTrue();
        assertThat(roleService.hasSchemaPrivilege(role1, Privilege.Type.DDL, OidHash.schemaOid("doc"))).isTrue();

        assertThat(roleService.hasPrivilege(role2, Privilege.Type.DDL, Securable.SCHEMA, "sys")).isFalse();
        assertThat(roleService.hasPrivilege(role2, Privilege.Type.DDL, Securable.SCHEMA, "doc")).isTrue();
        assertThat(roleService.hasAnyPrivilege(role2, Securable.SCHEMA, "sys")).isFalse();
        assertThat(roleService.hasAnyPrivilege(role2, Securable.SCHEMA, "doc")).isTrue();
        assertThat(roleService.hasSchemaPrivilege(role2, Privilege.Type.DDL, OidHash.schemaOid("sys"))).isFalse();
        assertThat(roleService.hasSchemaPrivilege(role2, Privilege.Type.DDL, OidHash.schemaOid("doc"))).isTrue();

        assertThat(roleService.hasPrivilege(role3, Privilege.Type.DDL, Securable.SCHEMA, "sys")).isTrue();
        assertThat(roleService.hasPrivilege(role3, Privilege.Type.DDL, Securable.SCHEMA, "doc")).isFalse();
        assertThat(roleService.hasAnyPrivilege(role3, Securable.SCHEMA, "sys")).isTrue();
        assertThat(roleService.hasAnyPrivilege(role3, Securable.SCHEMA, "doc")).isFalse();
        assertThat(roleService.hasSchemaPrivilege(role3, Privilege.Type.DDL, OidHash.schemaOid("sys"))).isTrue();
        assertThat(roleService.hasSchemaPrivilege(role3, Privilege.Type.DDL, OidHash.schemaOid("doc"))).isFalse();

        assertThat(roleService.hasPrivilege(role4, Privilege.Type.DDL, Securable.SCHEMA, "sys")).isFalse();
        assertThat(roleService.hasPrivilege(role4, Privilege.Type.DDL, Securable.SCHEMA, "doc")).isFalse();
        assertThat(roleService.hasAnyPrivilege(role4, Securable.SCHEMA, "sys")).isFalse();
        assertThat(roleService.hasAnyPrivilege(role4, Securable.SCHEMA, "doc")).isFalse();
        assertThat(roleService.hasSchemaPrivilege(role4, Privilege.Type.DDL, OidHash.schemaOid("sys"))).isFalse();
        assertThat(roleService.hasSchemaPrivilege(role4, Privilege.Type.DDL, OidHash.schemaOid("doc"))).isFalse();
    }
}
