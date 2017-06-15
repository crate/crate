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

import io.crate.analyze.user.Privilege;
import io.crate.settings.SharedSettings;
import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

public class PrivilegesMetaDataUpgraderTest extends CrateUnitTest {

    private static final PrivilegesMetaDataUpgrader UPGRADER = new PrivilegesMetaDataUpgrader();

    @Test
    public void testEnterpriseDisabledNothingChanged() throws Exception {
        Map<String, MetaData.Custom> customMap = new HashMap<>(1);
        customMap.put(UsersMetaData.TYPE, new UsersMetaData(Collections.singletonList("Arthur")));
        Map<String, MetaData.Custom> oldCustomMap = new HashMap<>(customMap);
        Settings settings = Settings.builder()
            .put(SharedSettings.ENTERPRISE_LICENSE_SETTING.getKey(), false)
            .build();
        Map<String, MetaData.Custom> newCustomMap = UPGRADER.apply(settings, customMap);
        assertThat(newCustomMap, is(oldCustomMap));
    }

    @Test
    public void testNoUsersNothingChanged() throws Exception {
        Map<String, MetaData.Custom> customMap = new HashMap<>(1);
        customMap.put(UsersMetaData.TYPE, new UsersMetaData(Collections.emptyList()));
        Map<String, MetaData.Custom> oldCustomMap = new HashMap<>(customMap);
        Map<String, MetaData.Custom> newCustomMap = UPGRADER.apply(Settings.EMPTY, customMap);
        assertThat(newCustomMap, is(oldCustomMap));
    }

    @Test
    public void testExistingUserWithoutAnyPrivilegeGetsAllPrivileges() throws Exception {
        Map<String, MetaData.Custom> customMap = new HashMap<>(1);
        customMap.put(UsersMetaData.TYPE, new UsersMetaData(Collections.singletonList("Arthur")));
        Map<String, MetaData.Custom> oldCustomMap = new HashMap<>(customMap);

        Map<String, MetaData.Custom> newCustomMap = UPGRADER.apply(Settings.EMPTY, customMap);
        assertThat(newCustomMap, not(is(oldCustomMap)));

        UsersPrivilegesMetaData privilegesMetaData = (UsersPrivilegesMetaData) newCustomMap.get(UsersPrivilegesMetaData.TYPE);
        assertThat(privilegesMetaData, notNullValue());
        Set<Privilege> userPrivileges = privilegesMetaData.getUserPrivileges("Arthur");
        assertThat(userPrivileges, notNullValue());
        Set<Privilege.Type> privilegeTypes = userPrivileges.stream().map(Privilege::type).collect(Collectors.toSet());
        assertThat(privilegeTypes, is(Privilege.GRANTABLE_TYPES));
    }

    @Test
    public void testExistingUserWithPrivilegesDoesntGetMore() throws Exception {
        Map<String, MetaData.Custom> customMap = new HashMap<>(1);
        customMap.put(UsersMetaData.TYPE, new UsersMetaData(Collections.singletonList("Arthur")));
        customMap.put(UsersPrivilegesMetaData.TYPE, new UsersPrivilegesMetaData(
            MapBuilder.<String, Set<Privilege>>newMapBuilder()
                .put("Arthur", Sets.newHashSet(
                    new Privilege(Privilege.State.GRANT, Privilege.Type.DQL, Privilege.Clazz.CLUSTER, null, "crate")))
                .map()));
        Map<String, MetaData.Custom> oldCustomMap = new HashMap<>(customMap);

        Map<String, MetaData.Custom> newCustomMap = UPGRADER.apply(Settings.EMPTY, customMap);
        assertThat(newCustomMap, is(oldCustomMap));
    }

    @Test
    public void testExistingUserWithEmptyPrivilegesDoesntGetMore() throws Exception {
        Map<String, MetaData.Custom> customMap = new HashMap<>(1);
        customMap.put(UsersMetaData.TYPE, new UsersMetaData(Collections.singletonList("Arthur")));
        customMap.put(UsersPrivilegesMetaData.TYPE, new UsersPrivilegesMetaData(
            MapBuilder.<String, Set<Privilege>>newMapBuilder()
                .put("Arthur", Collections.emptySet())
                .map()));
        Map<String, MetaData.Custom> oldCustomMap = new HashMap<>(customMap);

        Map<String, MetaData.Custom> newCustomMap = UPGRADER.apply(Settings.EMPTY, customMap);
        assertThat(newCustomMap, is(oldCustomMap));
    }
}
