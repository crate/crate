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
import static io.crate.role.metadata.RolesHelper.SINGLE_USER_ONLY;
import static io.crate.role.metadata.RolesHelper.getSecureHash;
import static io.crate.role.metadata.RolesHelper.usersMetadataOf;
import static io.crate.testing.Asserts.assertThat;

import java.util.Map;

import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import io.crate.role.metadata.RolesHelper;
import io.crate.role.metadata.RolesMetadata;
import io.crate.role.metadata.UsersMetadata;

public class TransportRoleActionTest extends ESTestCase {

    @Test
    public void testCreateFirstUser() throws Exception {
        Metadata.Builder mdBuilder = new Metadata.Builder();
        TransportCreateRoleAction.putRole(mdBuilder, "root", true, null);
        RolesMetadata metadata = (RolesMetadata) mdBuilder.getCustom(RolesMetadata.TYPE);
        assertThat(metadata.roleNames()).containsExactly("root");
    }

    @Test
    public void testCreateUserAlreadyExists() throws Exception {
        Metadata.Builder mdBuilder = new Metadata.Builder()
            .putCustom(RolesMetadata.TYPE, new RolesMetadata(SINGLE_USER_ONLY));
        assertThat(TransportCreateRoleAction.putRole(mdBuilder, "Arthur", true, null)).isTrue();
    }

    @Test
    public void testCreateUser() throws Exception {
        Metadata.Builder mdBuilder = new Metadata.Builder()
            .putCustom(RolesMetadata.TYPE, new RolesMetadata(SINGLE_USER_ONLY));
        TransportCreateRoleAction.putRole(mdBuilder, "Trillian", true, null);
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
        boolean res = TransportCreateRoleAction.putRole(mdBuilder, "RoleFoo", false, null);
        assertThat(res).isFalse();
        assertThat(roles(mdBuilder)).containsExactlyInAnyOrderEntriesOf(
            Map.of("Arthur", DUMMY_USERS.get("Arthur"),
                "Ford", DUMMY_USERS.get("Ford"),
                "RoleFoo", RolesHelper.roleOf("RoleFoo")));
    }

    @Test
    public void test_alter_user_with_old_users_metadata() throws Exception {
        var oldUsersMetadata = usersMetadataOf(DUMMY_USERS);
        var oldRolesMetadata = new RolesMetadata(DUMMY_USERS_AND_ROLES);
        Metadata.Builder mdBuilder = Metadata.builder()
            .putCustom(UsersMetadata.TYPE, oldUsersMetadata)
            .putCustom(RolesMetadata.TYPE, oldRolesMetadata);
        var newPasswd = getSecureHash("arthurs-new-passwd");
        boolean res = TransportAlterRoleAction.alterRole(mdBuilder, "Arthur", newPasswd);
        assertThat(res).isTrue();
        assertThat(roles(mdBuilder)).containsExactlyInAnyOrderEntriesOf(
            Map.of("Arthur", RolesHelper.userOf("Arthur", newPasswd),
                "Ford", DUMMY_USERS.get("Ford")));
    }

    @Test
    public void testDropUserNoUsersAtAll() throws Exception {
        assertThat(TransportDropRoleAction.dropRole(Metadata.builder(), "root")).isFalse();
    }

    @Test
    public void testDropNonExistingUser() throws Exception {
        boolean res = TransportDropRoleAction.dropRole(
                Metadata.builder().putCustom(RolesMetadata.TYPE, new RolesMetadata(SINGLE_USER_ONLY)),
                "trillian"
        );
        assertThat(res).isFalse();
    }

    @Test
    public void testDropUser() throws Exception {
        RolesMetadata oldMetadata = new RolesMetadata(DUMMY_USERS);
        Metadata.Builder mdBuilder = Metadata.builder().putCustom(RolesMetadata.TYPE, oldMetadata);
        boolean res = TransportDropRoleAction.dropRole(mdBuilder, "Arthur");
        assertThat(roles(mdBuilder)).containsExactlyEntriesOf(Map.of("Ford", DUMMY_USERS.get("Ford")));
        assertThat(res).isTrue();
    }

    @Test
    public void test_drop_user_with_old_users_metadata() throws Exception {
        var oldUsersMetadata = usersMetadataOf(DUMMY_USERS);
        var oldRolesMetadata = new RolesMetadata(DUMMY_USERS_AND_ROLES);
        Metadata.Builder mdBuilder = Metadata.builder()
            .putCustom(UsersMetadata.TYPE, oldUsersMetadata)
            .putCustom(RolesMetadata.TYPE, oldRolesMetadata);
        boolean res = TransportDropRoleAction.dropRole(mdBuilder, "Arthur");
        assertThat(roles(mdBuilder)).containsExactlyEntriesOf(Map.of("Ford", DUMMY_USERS.get("Ford")));
        assertThat(res).isTrue();
    }

    private static Map<String, Role> roles(Metadata.Builder mdBuilder) {
        return ((RolesMetadata) mdBuilder.build().custom(RolesMetadata.TYPE)).roles();
    }
}
