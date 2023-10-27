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


package io.crate.user.metadata;

import static io.crate.testing.Asserts.assertThat;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import io.crate.common.collections.MapBuilder;
import io.crate.user.Privilege;

public class PrivilegesMetadataUpgraderTest extends ESTestCase {

    private static final PrivilegesMetadataUpgrader UPGRADER = new PrivilegesMetadataUpgrader();

    @Test
    public void testNoUsersNothingChanged() throws Exception {
        Map<String, Metadata.Custom> customMap = new HashMap<>(1);
        customMap.put(UsersMetadata.TYPE, new UsersMetadata(Map.of()));
        Map<String, Metadata.Custom> oldCustomMap = new HashMap<>(customMap);
        Map<String, Metadata.Custom> newCustomMap = UPGRADER.apply(Settings.EMPTY, customMap);
        assertThat(newCustomMap).isEqualTo(oldCustomMap);
    }

    @Test
    public void testExistingUserWithoutAnyPrivilegeGetsAllPrivileges() throws Exception {
        Map<String, Metadata.Custom> customMap = new HashMap<>(1);
        customMap.put(UsersMetadata.TYPE, new UsersMetadata(UserDefinitions.SINGLE_USER_ONLY));
        Map<String, Metadata.Custom> oldCustomMap = new HashMap<>(customMap);

        Map<String, Metadata.Custom> newCustomMap = UPGRADER.apply(Settings.EMPTY, customMap);
        assertThat(newCustomMap).isNotEqualTo(oldCustomMap);

        UsersPrivilegesMetadata privilegesMetadata = (UsersPrivilegesMetadata) newCustomMap.get(UsersPrivilegesMetadata.TYPE);
        assertThat(privilegesMetadata).isNotNull();
        Set<Privilege> userPrivileges = privilegesMetadata.getUserPrivileges("Arthur");
        assertThat(userPrivileges).isNotNull();
        Set<Privilege.Type> privilegeTypes = userPrivileges.stream().map(p -> p.ident().type()).collect(Collectors.toSet());
        assertThat(privilegeTypes).containsExactlyInAnyOrder(Privilege.Type.values());
    }

    @Test
    public void testExistingUserWithPrivilegesDoesntGetMore() throws Exception {
        Map<String, Metadata.Custom> customMap = new HashMap<>(1);
        customMap.put(UsersMetadata.TYPE, new UsersMetadata(UserDefinitions.SINGLE_USER_ONLY));
        customMap.put(UsersPrivilegesMetadata.TYPE, new UsersPrivilegesMetadata(
            MapBuilder.<String, Set<Privilege>>newMapBuilder()
                .put("Arthur", Set.of(
                    new Privilege(Privilege.State.GRANT, Privilege.Type.DQL, Privilege.Clazz.CLUSTER, null, "crate")))
                .map()));
        Map<String, Metadata.Custom> oldCustomMap = new HashMap<>(customMap);

        Map<String, Metadata.Custom> newCustomMap = UPGRADER.apply(Settings.EMPTY, customMap);
        assertThat(newCustomMap).isEqualTo(oldCustomMap);
    }

    @Test
    public void testExistingUserWithEmptyPrivilegesDoesntGetMore() throws Exception {
        Map<String, Metadata.Custom> customMap = new HashMap<>(1);
        customMap.put(UsersMetadata.TYPE, new UsersMetadata(UserDefinitions.SINGLE_USER_ONLY));
        customMap.put(UsersPrivilegesMetadata.TYPE, new UsersPrivilegesMetadata(
            MapBuilder.<String, Set<Privilege>>newMapBuilder()
                .put("Arthur", Collections.emptySet())
                .map()));
        Map<String, Metadata.Custom> oldCustomMap = new HashMap<>(customMap);

        Map<String, Metadata.Custom> newCustomMap = UPGRADER.apply(Settings.EMPTY, customMap);
        assertThat(newCustomMap).isEqualTo(oldCustomMap);
    }
}
