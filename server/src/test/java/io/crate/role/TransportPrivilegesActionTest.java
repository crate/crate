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

import static io.crate.role.metadata.RolesHelper.userOf;
import static io.crate.testing.Asserts.assertThat;

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
        new Privilege(PrivilegeState.GRANT, Privilege.Type.DQL, Privilege.Clazz.CLUSTER, null, "crate");
    private static final Privilege GRANT_DML =
        new Privilege(PrivilegeState.GRANT, Privilege.Type.DML, Privilege.Clazz.CLUSTER, null, "crate");
    private static final Privilege DENY_DQL =
        new Privilege(PrivilegeState.DENY, Privilege.Type.DQL, Privilege.Clazz.CLUSTER, null, "crate");
    private static final Set<Privilege> PRIVILEGES = new HashSet<>(Arrays.asList(GRANT_DQL, GRANT_DML));

    @Test
    public void testApplyPrivilegesCreatesNewPrivilegesInstance() {
        // given
        Metadata.Builder mdBuilder = Metadata.builder();
        Map<String, Role> roles = new HashMap<>();
        roles.put("Ford", userOf("Ford", new HashSet<>(PRIVILEGES), null));

        RolesMetadata initialRolesMetadata = new RolesMetadata(roles);
        mdBuilder.putCustom(RolesMetadata.TYPE, initialRolesMetadata);
        PrivilegesRequest denyPrivilegeRequest =
            new PrivilegesRequest(Collections.singletonList("Ford"), Collections.singletonList(DENY_DQL), null);

        //when
        TransportPrivilegesAction.applyPrivileges(mdBuilder, denyPrivilegeRequest);

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
}
