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
import static io.crate.role.RolesDefinitions.DUMMY_USERS_AND_ROLES;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.Set;

import org.junit.Test;

import io.crate.role.metadata.RolesMetadata;
import io.crate.role.metadata.UsersMetadata;
import io.crate.role.metadata.UsersPrivilegesMetadata;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;

public class RolesServiceTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void testNullAndEmptyMetadata() {
        // the users list will always contain a crate user
        Set<Role> roles = RolesService.getRoles(null, null, null);
        assertThat(roles).containsExactly(CRATE_USER);

        roles = RolesService.getRoles(new UsersMetadata(), new RolesMetadata(), new UsersPrivilegesMetadata());
        assertThat(roles).containsExactly(CRATE_USER);
    }

    @Test
    public void testUsersAndRoles() {
        Set<Role> roles = RolesService.getRoles(
            null,
            new RolesMetadata(RolesDefinitions.DUMMY_USERS_AND_ROLES),
            new UsersPrivilegesMetadata());

        assertThat(roles).containsExactlyInAnyOrder(
            DUMMY_USERS_AND_ROLES.get("Ford"),
            DUMMY_USERS_AND_ROLES.get("John"),
            Role.roleOf("DummyRole"),
            CRATE_USER);
    }

    @Test
    public void test_old_users_metadata_is_preferred_over_roles_metadata() {
        Set<Role> roles = RolesService.getRoles(
            new UsersMetadata(Collections.singletonMap("Arthur", null)),
            new RolesMetadata(RolesDefinitions.DUMMY_USERS_AND_ROLES),
            new UsersPrivilegesMetadata());

        assertThat(roles).containsExactlyInAnyOrder(
            Role.userOf("Arthur"),
            CRATE_USER);
    }
}
