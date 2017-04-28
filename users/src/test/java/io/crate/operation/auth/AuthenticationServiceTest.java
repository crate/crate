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

package io.crate.operation.auth;

import com.google.common.collect.ImmutableMap;
import io.crate.action.sql.SessionContext;
import io.crate.plugin.SQLPlugin;
import io.crate.test.integration.CrateUnitTest;
import org.apache.commons.lang3.RandomStringUtils;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.jboss.netty.channel.Channel;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static io.crate.operation.auth.AuthenticationService.Matchers.isValidAddress;
import static io.crate.operation.auth.AuthenticationService.Matchers.isValidUser;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class AuthenticationServiceTest extends CrateUnitTest {

    private static final ImmutableMap<String, String> HBA_1 = ImmutableMap.<String, String>builder()
        .put("user", "crate")
        .put("address", "127.0.0.1")
        .put("method", "trust")
        .build();

    private static final ImmutableMap<String, String> HBA_2 = ImmutableMap.<String, String>builder()
        .put("user", "crate")
        .put("address", "0.0.0.0/0")
        .put("method", "fake")
        .build();

    private static final ImmutableMap<String, String> HBA_3 = ImmutableMap.<String, String>builder()
        .put("address", "127.0.0.1")
        .put("method", "md5")
        .build();

    private ClusterService clusterService;
    private AuthenticationService authService;

    private Settings settings;

    @Before
    private void setUpTest(){
        settings = Settings.EMPTY;
        clusterService = createClusterService(settings);
        authService = new AuthenticationService(clusterService, settings);
    }

    private ClusterService createClusterService(Settings settings){
        SQLPlugin sqlPlugin = new SQLPlugin(Settings.EMPTY);
        Set<Setting<?>> cSettings = new HashSet<>();
        cSettings.addAll(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        cSettings.addAll(sqlPlugin.getSettings());
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, cSettings);

        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getSettings()).thenReturn(settings);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);

        return clusterService;
    }

    @SafeVarargs
    private static Map<String, Map<String, String>> createHbaConf(Map<String, String>... entries) {
        ImmutableMap.Builder<String, Map<String, String>> builder = ImmutableMap.builder();
        int idx = 0;
        for (Map<String, String> entry : entries) {
            builder.put(String.valueOf(idx), entry);
            idx++;
        }
        return builder.build();
    }

    @Test
    public void testMissingHbaConf() throws Exception {
        assertFalse(authService.enabled());
    }

    @Test
    public void testMissingUserOrAddress() throws Exception {
        authService.updateHbaConfig(Collections.emptyMap());
        AuthenticationMethod method;
        method = authService.resolveAuthenticationType(null, "127.0.0.1");
        assertNull(method);
        method = authService.resolveAuthenticationType("crate", null);
        assertNull(method);
    }

    @Test
    public void testEmptyHbaConf() throws Exception {
        authService.updateHbaConfig(Collections.emptyMap());
        AuthenticationMethod method = authService.resolveAuthenticationType("crate", "127.0.0.1");
        assertNull(method);
    }

    @Test
    public void testResolveAuthMethod() throws Exception {
        AuthenticationMethod noopAuthMethod = new AuthenticationMethod() {
            @Override
            public void pgAuthenticate(Channel channel, SessionContext session, Settings settings) {
            }

            @Override
            public String name() {
                return "trust";
            }
        };

        authService.registerAuthMethod(noopAuthMethod);
        authService.updateHbaConfig(createHbaConf(HBA_1));
        AuthenticationMethod method = authService.resolveAuthenticationType("crate", "127.0.0.1");
        assertThat(method, is(noopAuthMethod));
    }

    @Test
    public void testFilterEntriesSimple() throws Exception {
        authService.updateHbaConfig(createHbaConf(HBA_1));
        Optional entry;

        entry = authService.getEntry("crate", "127.0.0.1");
        assertThat(entry.isPresent(), is(true));

        entry = authService.getEntry("cr8", "127.0.0.1");
        assertThat(entry.isPresent(), is(false));

        entry = authService.getEntry("crate", "127.0.0.2");
        assertThat(entry.isPresent(), is(false));
    }

    @Test
    public void testFilterEntriesCIDR() throws Exception {
        authService.updateHbaConfig(createHbaConf(HBA_2, HBA_3));
        Optional<Map.Entry<String, Map<String, String>>> entry;

        entry = authService.getEntry("crate", "123.45.67.89");
        assertTrue(entry.isPresent());
        assertThat(entry.get().getValue().get("method"), is("fake"));

        entry = authService.getEntry("cr8", "127.0.0.1");
        assertTrue(entry.isPresent());
        assertThat(entry.get().getValue().get("method"), is("md5"));

        entry = authService.getEntry("cr8", "123.45.67.89");
        assertThat(entry.isPresent(), is(false));
    }

    @Test
    public void testMatchUser() throws Exception {
        // only "crate" matches
        Map.Entry<String, Map<String, String>> entry = new HashMap.SimpleEntry<>(
            "0", Collections.singletonMap("user", "crate")
        );
        assertTrue(isValidUser(entry, "crate"));
        assertFalse(isValidUser(entry, "postgres"));

        // any user matches
        entry = new HashMap.SimpleEntry<>(
            "0", Collections.emptyMap() // key "user" not present in map
        );
        assertTrue(isValidUser(entry, RandomStringUtils.random(8)));
    }

    @Test
    public void testMatchAddress() throws Exception {
        // matches
        Map.Entry<String, Map<String, String>> entry = new HashMap.SimpleEntry<>(
            "0", Collections.singletonMap("address", "10.0.1.100")
        );
        assertTrue(isValidAddress(entry, "10.0.1.100"));
        assertFalse(isValidAddress(entry, "10.0.1.99"));
        assertFalse(isValidAddress(entry, "10.0.1.101"));

        entry = new HashMap.SimpleEntry<>(
            "0", Collections.singletonMap("address", "10.0.1.0/24") // 10.0.1.0 -- 10.0.1.255
        );
        assertTrue(isValidAddress(entry, "10.0.1.0"));
        assertTrue(isValidAddress(entry, "10.0.1.255"));
        assertFalse(isValidAddress(entry, "10.0.0.255"));
        assertFalse(isValidAddress(entry, "10.0.2.0"));

        entry = new HashMap.SimpleEntry<>(
            "0", Collections.emptyMap() // key "address" is missing
        );
        assertTrue(isValidAddress(entry,
            String.format("%s.%s.%s.%s", randomInt(255), randomInt(255), randomInt(255), randomInt(255))));
    }

    @Test
    public void testConvertHBASetting() throws Exception {
        Settings settings = Settings.builder()
            .put("auth.host_based.0.user", "crate")
            .put("auth.host_based.0.address", "127.0.0.1")
            .put("auth.host_based.0.method", "trust")
            .put("auth.host_based.1.user", "crate")
            .put("auth.host_based.1.address", "0.0.0.0/0")
            .put("auth.host_based.1.method", "fake")
            .build();

        AuthenticationService authService = new AuthenticationService(createClusterService(settings), settings);
        assertThat(authService.hbaConf(), is(createHbaConf(HBA_1, HBA_2)));
    }
}
