/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import static io.crate.role.metadata.RolesHelper.userOf;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import io.crate.metadata.RelationName;
import io.crate.role.metadata.RolesHelper;
import io.crate.role.metadata.RolesMetadata;
import io.crate.role.metadata.UsersMetadata;
import io.crate.role.metadata.UsersPrivilegesMetadata;

public class RoleManagerDDLModifierTest extends ESTestCase {

    // tests a bug fix: https://github.com/crate/crate/issues/15521
    @Test
    public void test_transferTablePrivileges_from_old_users_and_privileges_metadata() {
        // given
        Privilege oldGrantDQL = new Privilege(Policy.GRANT, Permission.DQL, Securable.SCHEMA, "mySchema", "crate");
        Metadata.Builder mdBuilder = Metadata.builder();
        Map<String, Role> rolesMap = new HashMap<>();
        rolesMap.put("Ford", userOf("Ford", Set.of(oldGrantDQL), null));

        UsersMetadata oldUsersMetada = RolesHelper.usersMetadataOf(rolesMap);
        UsersPrivilegesMetadata oldPrivilegesMetadata = RolesHelper.usersPrivilegesMetadataOf(rolesMap);
        mdBuilder.putCustom(UsersMetadata.TYPE, oldUsersMetada);
        mdBuilder.putCustom(UsersPrivilegesMetadata.TYPE, oldPrivilegesMetadata);

        // when
        boolean result = RoleManagerDDLModifier.transferTablePrivileges(
            Version.CURRENT,
            mdBuilder,
            new RelationName("mySchema", "oldName"),
            new RelationName("mySchema", "newName"));

        // then
        assertThat(result).isTrue();
        assertThat(mdBuilder.getCustom(UsersMetadata.TYPE)).isNull();
        assertThat(mdBuilder.getCustom(UsersPrivilegesMetadata.TYPE)).isNull();
        RolesMetadata newRolesMetadata = (RolesMetadata) mdBuilder.getCustom(RolesMetadata.TYPE);
        assertThat(newRolesMetadata.roles()).hasSize(1);

        var roleFord = newRolesMetadata.roles().get("Ford");
        assertThat(roleFord.privileges()).containsExactly(oldGrantDQL);
    }
}
