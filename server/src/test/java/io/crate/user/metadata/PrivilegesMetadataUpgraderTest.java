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

import com.google.common.collect.ImmutableMap;
import io.crate.user.Privilege;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.cluster.metadata.Metadata;
import io.crate.common.collections.MapBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

public class PrivilegesMetadataUpgraderTest extends ESTestCase {

    private static final PrivilegesMetadataUpgrader UPGRADER = new PrivilegesMetadataUpgrader();

    @Test
    public void testNoUsersNothingChanged() throws Exception {
        Map<String, Metadata.Custom> customMap = new HashMap<>(1);
        customMap.put(UsersMetadata.TYPE, new UsersMetadata(ImmutableMap.of()));
        Map<String, Metadata.Custom> oldCustomMap = new HashMap<>(customMap);
        Map<String, Metadata.Custom> newCustomMap = UPGRADER.apply(Settings.EMPTY, customMap);
        assertThat(newCustomMap, is(oldCustomMap));
    }

    @Test
    public void testExistingUserWithoutAnyPrivilegeGetsAllPrivileges() throws Exception {
        Map<String, Metadata.Custom> customMap = new HashMap<>(1);
        customMap.put(UsersMetadata.TYPE, new UsersMetadata(UserDefinitions.SINGLE_USER_ONLY));
        Map<String, Metadata.Custom> oldCustomMap = new HashMap<>(customMap);

        Map<String, Metadata.Custom> newCustomMap = UPGRADER.apply(Settings.EMPTY, customMap);
        assertThat(newCustomMap, not(is(oldCustomMap)));

        UsersPrivilegesMetadata privilegesMetadata = (UsersPrivilegesMetadata) newCustomMap.get(UsersPrivilegesMetadata.TYPE);
        assertThat(privilegesMetadata, notNullValue());
        Set<Privilege> userPrivileges = privilegesMetadata.getUserPrivileges("Arthur");
        assertThat(userPrivileges, notNullValue());
        Set<Privilege.Type> privilegeTypes = userPrivileges.stream().map(p -> p.ident().type()).collect(Collectors.toSet());
        assertThat(privilegeTypes, containsInAnyOrder(Privilege.Type.values()));
    }

    @Test
    public void testExistingUserWithPrivilegesDoesntGetMore() throws Exception {
        Map<String, Metadata.Custom> customMap = new HashMap<>(1);
        customMap.put(UsersMetadata.TYPE, new UsersMetadata(UserDefinitions.SINGLE_USER_ONLY));
        customMap.put(UsersPrivilegesMetadata.TYPE, new UsersPrivilegesMetadata(
            MapBuilder.<String, Set<Privilege>>newMapBuilder()
                .put("Arthur", Sets.newHashSet(
                    new Privilege(Privilege.State.GRANT, Privilege.Type.DQL, Privilege.Clazz.CLUSTER, null, "crate")))
                .map()));
        Map<String, Metadata.Custom> oldCustomMap = new HashMap<>(customMap);

        Map<String, Metadata.Custom> newCustomMap = UPGRADER.apply(Settings.EMPTY, customMap);
        assertThat(newCustomMap, is(oldCustomMap));
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
        assertThat(newCustomMap, is(oldCustomMap));
    }
}
