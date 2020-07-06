/*
 * This file is part of a module with proprietary Enterprise Features.
 *
 * Licensed to Crate.io Inc. ("Crate.io") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 *
 * To use this file, Crate.io must have given you permission to enable and
 * use such Enterprise Features and you must have a valid Enterprise or
 * Subscription Agreement with Crate.io.  If you enable or use the Enterprise
 * Features, you represent and warrant that you have a valid Enterprise or
 * Subscription Agreement with Crate.io.  Your use of the Enterprise Features
 * if governed by the terms and conditions of your Enterprise or Subscription
 * Agreement with Crate.io.
 */


package io.crate.metadata;

import com.google.common.collect.ImmutableMap;
import io.crate.analyze.user.Privilege;
import io.crate.test.integration.CrateUnitTest;
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

public class PrivilegesMetadataUpgraderTest extends CrateUnitTest {

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
