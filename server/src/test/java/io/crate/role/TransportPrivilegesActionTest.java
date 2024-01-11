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

import static io.crate.role.metadata.RolesHelper.DUMMY_USERS_AND_ROLES;
import static io.crate.role.metadata.RolesHelper.userOf;
import static io.crate.testing.Asserts.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import io.crate.role.metadata.RolesHelper;
import io.crate.role.metadata.RolesMetadata;

public class TransportPrivilegesActionTest extends ESTestCase {

    private static final Privilege GRANT_DQL =
        new Privilege(PrivilegeState.GRANT, Privilege.Type.DQL, Securable.CLUSTER, null, "crate");
    private static final Privilege GRANT_DML =
        new Privilege(PrivilegeState.GRANT, Privilege.Type.DML, Securable.CLUSTER, null, "crate");
    private static final Privilege DENY_DQL =
        new Privilege(PrivilegeState.DENY, Privilege.Type.DQL, Securable.CLUSTER, null, "crate");
    private static final Set<Privilege> PRIVILEGES = new HashSet<>(Arrays.asList(GRANT_DQL, GRANT_DML));

    @Test
    public void testApplyPrivilegesCreatesNewPrivilegesInstance() {
        // given
        Metadata.Builder mdBuilder = Metadata.builder();
        Map<String, Role> rolesMap = new HashMap<>();
        rolesMap.put("Ford", userOf("Ford", new HashSet<>(PRIVILEGES), null));

        RolesMetadata initialRolesMetadata = new RolesMetadata(rolesMap);
        mdBuilder.putCustom(RolesMetadata.TYPE, initialRolesMetadata);
        PrivilegesRequest denyPrivilegeRequest =
            new PrivilegesRequest(Collections.singletonList("Ford"), Collections.singletonList(DENY_DQL), null);

        //when
        TransportPrivilegesAction.applyPrivileges(rolesMap::values, mdBuilder, denyPrivilegeRequest);

        // then
        RolesMetadata newRolesMetadata =
            (RolesMetadata) mdBuilder.getCustom(RolesMetadata.TYPE);
        assertThat(newRolesMetadata).isNotSameAs(initialRolesMetadata);
    }

    @Test
    public void testValidateUserNamesEmptyUsers() throws Exception {
        List<String> roleNames = List.of("ford", "arthur");
        List<String> unknownRoleNames = TransportPrivilegesAction.validateRoleNames(new RolesMetadata(), roleNames);
        assertThat(unknownRoleNames).isEqualTo(roleNames);
    }

    @Test
    public void testValidateUserNamesMissingUser() throws Exception {
        List<String> roleNames = List.of("Ford", "Arthur");
        List<String> unknownRoleNames = TransportPrivilegesAction.validateRoleNames(
            new RolesMetadata(RolesHelper.SINGLE_USER_ONLY), roleNames);
        assertThat(unknownRoleNames).containsExactly("Ford");
    }

    @Test
    public void testValidateUserNamesAllExists() throws Exception {
        List<String> unknownRoleNames = TransportPrivilegesAction.validateRoleNames(
                new RolesMetadata(RolesHelper.DUMMY_USERS), List.of("Ford", "Arthur"));
        assertThat(unknownRoleNames).isEmpty();
    }

    @Test
    public void test_validate_response_when_roles_do_not_exist() {
        Metadata.Builder mdBuilder = Metadata.builder();
        var rolesMetadata = new RolesMetadata(DUMMY_USERS_AND_ROLES);
        mdBuilder.putCustom(RolesMetadata.TYPE, rolesMetadata);

        PrivilegesRequest privilegeReq = new PrivilegesRequest(
            List.of("unknownUser"), Set.of(), new GrantedRolesChange(PrivilegeState.GRANT, Set.of("John"), null));

        var result = TransportPrivilegesAction.applyPrivileges(DUMMY_USERS_AND_ROLES::values, mdBuilder, privilegeReq);
        assertThat(result.affectedRows()).isEqualTo(-1);
        assertThat(result.unknownRoleNames()).containsExactly("unknownUser");

        privilegeReq = new PrivilegesRequest(
            List.of("John"), Set.of(), new GrantedRolesChange(PrivilegeState.GRANT, Set.of("unknownRole"), null));

        result = TransportPrivilegesAction.applyPrivileges(DUMMY_USERS_AND_ROLES::values, mdBuilder, privilegeReq);
        assertThat(result.affectedRows()).isEqualTo(-1);
        assertThat(result.unknownRoleNames()).containsExactly("unknownRole");
    }

    @Test
    public void test_grant_revoke_user_to_another_user_is_not_allowed() {
        var mdBuilder = Metadata.builder();
        var rolesMetadata = new RolesMetadata(DUMMY_USERS_AND_ROLES);
        mdBuilder.putCustom(RolesMetadata.TYPE, rolesMetadata);

        for (var state : List.of(PrivilegeState.GRANT, PrivilegeState.REVOKE)) {
            var privilegeReq = new PrivilegesRequest(
                List.of("DummyRole"), Set.of(), new GrantedRolesChange(state, Set.of("John"), null));

            assertThatThrownBy(() ->
                TransportPrivilegesAction.applyPrivileges(DUMMY_USERS_AND_ROLES::values, mdBuilder, privilegeReq))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("Cannot " + state + " a USER to a ROLE");
        }
    }

    @Test
    public void test_grant_role_which_creates_cycle_is_not_allowed() {
        /* Given:

                   role1
                     |
                   role2
                 /    |
             role3    |
               |  \   |
               |  role4
               |   |
               role5
         */

        var role1 = RolesHelper.roleOf("role1");
        var role2 = RolesHelper.roleOf("role2", List.of("role1"));
        var role3 = RolesHelper.roleOf("role3", List.of("role2"));
        var role4 = RolesHelper.roleOf("role4", List.of("role3", "role2"));
        var role5 = RolesHelper.roleOf("role5", List.of("role3", "role4"));
        var roles = Map.of(
            "role1", role1,
            "role2", role2,
            "role3", role3,
            "role4", role4,
            "role5", role5
        );


        /* Try to create:

                   role1-----+
                     |       |
                   role2     |
                 /    |      |
             role3    |      |
               |  \   |      |
               |  role4 <----+
               |   |
               role5
         */
        var privilegeReq1 = new PrivilegesRequest(
            List.of("role1"), Set.of(), new GrantedRolesChange(PrivilegeState.GRANT, Set.of("role4"), null));
        assertThatThrownBy(() ->
            TransportPrivilegesAction.detectCyclesInRolesHierarchy(roles::values, privilegeReq1))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Cannot grant role role4 to role1, role1 is a parent role of role4 and a cycle will " +
                "be created");


        /* Try to create:

                   role1
                     |
                   role2 ----+
                 /    |      |
             role3    |      |
               |  \   |      |
               |  role4      |
               |   |         |
               role5 <-------+
         */
        var privilegeReq2 = new PrivilegesRequest(
            List.of("role2"), Set.of(), new GrantedRolesChange(PrivilegeState.GRANT, Set.of("role5"), null));
        assertThatThrownBy(() ->
            TransportPrivilegesAction.detectCyclesInRolesHierarchy(roles::values, privilegeReq2))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Cannot grant role role5 to role2, role2 is a parent role of role5 and a cycle will " +
                "be created");
    }
}
