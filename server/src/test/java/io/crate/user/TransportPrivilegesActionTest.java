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

package io.crate.user;

import com.google.common.collect.Lists;
import io.crate.user.metadata.UserDefinitions;
import io.crate.user.metadata.UsersMetadata;
import io.crate.user.metadata.UsersPrivilegesMetadata;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.cluster.metadata.Metadata;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.core.Is.is;

public class TransportPrivilegesActionTest extends ESTestCase {

    private static final Privilege GRANT_DQL =
        new Privilege(Privilege.State.GRANT, Privilege.Type.DQL, Privilege.Clazz.CLUSTER, null, "crate");
    private static final Privilege GRANT_DML =
        new Privilege(Privilege.State.GRANT, Privilege.Type.DML, Privilege.Clazz.CLUSTER, null, "crate");
    private static final Privilege DENY_DQL =
        new Privilege(Privilege.State.DENY, Privilege.Type.DQL, Privilege.Clazz.CLUSTER, null, "crate");
    private static final Set<Privilege> PRIVILEGES = new HashSet<>(Arrays.asList(GRANT_DQL, GRANT_DML));

    @Test
    public void testApplyPrivilegesCreatesNewPrivilegesInstance() {
        // given
        Metadata.Builder mdBuilder = Metadata.builder();
        Map<String, Set<Privilege>> usersPrivileges = new HashMap<>();
        usersPrivileges.put("Ford", new HashSet<>(PRIVILEGES));
        UsersPrivilegesMetadata initialPrivilegesMetadata = new UsersPrivilegesMetadata(usersPrivileges);
        mdBuilder.putCustom(UsersPrivilegesMetadata.TYPE, initialPrivilegesMetadata);
        PrivilegesRequest denyPrivilegeRequest =
            new PrivilegesRequest(Collections.singletonList("Ford"), Collections.singletonList(DENY_DQL));

        //when
        TransportPrivilegesAction.applyPrivileges(mdBuilder, denyPrivilegeRequest);

        // then
        UsersPrivilegesMetadata newPrivilegesMetadata =
            (UsersPrivilegesMetadata) mdBuilder.getCustom(UsersPrivilegesMetadata.TYPE);
        assertNotSame(newPrivilegesMetadata, initialPrivilegesMetadata);
    }

    @Test
    public void testValidateUserNamesEmptyUsers() throws Exception {
        List<String> userNames = Lists.newArrayList("ford", "arthur");
        List<String> unknownUserNames = TransportPrivilegesAction.validateUserNames(Metadata.EMPTY_METADATA, userNames);
        assertThat(unknownUserNames, is(userNames));
    }

    @Test
    public void testValidateUserNamesMissingUser() throws Exception {
        Metadata metadata = Metadata.builder()
            .putCustom(UsersMetadata.TYPE, new UsersMetadata(UserDefinitions.SINGLE_USER_ONLY))
            .build();
        List<String> userNames = Lists.newArrayList("Ford", "Arthur");
        List<String> unknownUserNames = TransportPrivilegesAction.validateUserNames(metadata, userNames);
        assertThat(unknownUserNames, contains("Ford"));
    }

    @Test
    public void testValidateUserNamesAllExists() throws Exception {
        Metadata metadata = Metadata.builder()
            .putCustom(UsersMetadata.TYPE, new UsersMetadata(UserDefinitions.DUMMY_USERS))
            .build();
        List<String> unknownUserNames = TransportPrivilegesAction.validateUserNames(metadata, List.of("Ford", "Arthur"));
        assertThat(unknownUserNames.size(), is(0));
    }

}
