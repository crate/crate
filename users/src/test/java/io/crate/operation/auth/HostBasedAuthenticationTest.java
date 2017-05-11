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
import io.crate.operation.user.User;
import io.crate.plugin.SQLPlugin;
import io.crate.test.integration.CrateUnitTest;
import org.apache.commons.lang3.RandomStringUtils;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.jboss.netty.channel.Channel;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.CompletableFuture;

import static io.crate.operation.auth.HostBasedAuthentication.Matchers.*;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class HostBasedAuthenticationTest extends CrateUnitTest {

    private static final ImmutableMap<String, String> HBA_1 = ImmutableMap.<String, String>builder()
        .put("user", "crate")
        .put("address", "127.0.0.1")
        .put("method", "trust")
        .put("protocol", "pg")
        .build();

    private static final ImmutableMap<String, String> HBA_2 = ImmutableMap.<String, String>builder()
        .put("user", "crate")
        .put("address", "0.0.0.0/0")
        .put("method", "fake")
        .put("protocol", "pg")
        .build();

    private static final ImmutableMap<String, String> HBA_3 = ImmutableMap.<String, String>builder()
        .put("address", "127.0.0.1")
        .put("method", "md5")
        .put("protocol", "pg")
        .build();

    private static final InetAddress LOCALHOST = InetAddresses.forString("127.0.0.1");
    private HostBasedAuthentication authService;

    @Before
    private void setUpTest() {
        Settings settings = Settings.builder()
            .put(AuthenticationProvider.AUTH_HOST_BASED_ENABLED_SETTING.getKey(), true)
            .build();
        authService = new HostBasedAuthentication(settings, null);
    }

    private static ClusterService createClusterService(Settings settings){
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
    public void testEnableAuthentication() throws Exception {
        Authentication authService = new HostBasedAuthentication(Settings.EMPTY, null);
        assertFalse(authService.enabled());

        Settings settings = Settings.builder()
            .put(AuthenticationProvider.AUTH_HOST_BASED_ENABLED_SETTING.getKey(), true)
            .build();
        authService = new HostBasedAuthentication(settings, null);
        assertTrue(authService.enabled());
    }

    @Test
    public void testMissingUserOrAddress() throws Exception {
        authService.updateHbaConfig(Collections.emptyMap());
        AuthenticationMethod method;
        method = authService.resolveAuthenticationType(null, LOCALHOST, HbaProtocol.POSTGRES);
        assertNull(method);
        method = authService.resolveAuthenticationType("crate", null, HbaProtocol.POSTGRES);
        assertNull(method);
    }

    @Test
    public void testEmptyHbaConf() throws Exception {
        authService.updateHbaConfig(Collections.emptyMap());
        AuthenticationMethod method = authService.resolveAuthenticationType("crate", LOCALHOST, HbaProtocol.POSTGRES);
        assertNull(method);
    }

    @Test
    public void testResolveAuthMethod() throws Exception {
        AuthenticationMethod noopAuthMethod = new AuthenticationMethod() {
            @Override
            public CompletableFuture<User> pgAuthenticate(Channel channel, String userName) {
                return null;
            }

            @Override
            public String name() {
                return "trust";
            }
        };
        authService.registerAuthMethod(noopAuthMethod.name(), () -> noopAuthMethod);
        authService.updateHbaConfig(createHbaConf(HBA_1));
        AuthenticationMethod method = authService.resolveAuthenticationType("crate", LOCALHOST, HbaProtocol.POSTGRES);
        assertThat(method, is(noopAuthMethod));
    }

    @Test
    public void testFilterEntriesSimple() throws Exception {
        authService.updateHbaConfig(createHbaConf(HBA_1));
        Optional entry;

        entry = authService.getEntry("crate", LOCALHOST, HbaProtocol.POSTGRES);
        assertThat(entry.isPresent(), is(true));

        entry = authService.getEntry("cr8", LOCALHOST, HbaProtocol.POSTGRES);
        assertThat(entry.isPresent(), is(false));

        entry = authService.getEntry("crate", InetAddresses.forString("10.0.0.1"), HbaProtocol.POSTGRES);
        assertThat(entry.isPresent(), is(false));
    }

    @Test
    public void testFilterEntriesCIDR() throws Exception {
        authService.updateHbaConfig(createHbaConf(HBA_2, HBA_3));
        Optional<Map.Entry<String, Map<String, String>>> entry;

        entry = authService.getEntry("crate", InetAddresses.forString("123.45.67.89"), HbaProtocol.POSTGRES);
        assertTrue(entry.isPresent());
        assertThat(entry.get().getValue().get("method"), is("fake"));

        entry = authService.getEntry("cr8", InetAddresses.forString("127.0.0.1"), HbaProtocol.POSTGRES);
        assertTrue(entry.isPresent());
        assertThat(entry.get().getValue().get("method"), is("md5"));

        entry = authService.getEntry("cr8", InetAddresses.forString("123.45.67.89"), HbaProtocol.POSTGRES);
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
    public void testMatchProtocol() throws Exception {
        assertTrue(isValidProtocol("pg", HbaProtocol.POSTGRES));
        assertFalse(isValidProtocol("http", HbaProtocol.POSTGRES));
        assertTrue(isValidProtocol(null, HbaProtocol.POSTGRES));
    }

    @Test
    public void testMatchAddress() throws Exception {
        // matches
        Map.Entry<String, Map<String, String>> entry = new HashMap.SimpleEntry<>(
            "0", Collections.singletonMap("address", "10.0.1.100")
        );
        assertTrue(isValidAddress(entry, InetAddresses.forString("10.0.1.100")));
        assertFalse(isValidAddress(entry, InetAddresses.forString("10.0.1.99")));
        assertFalse(isValidAddress(entry, InetAddresses.forString("10.0.1.101")));

        entry = new HashMap.SimpleEntry<>(
            "0", Collections.singletonMap("address", "10.0.1.0/24") // 10.0.1.0 -- 10.0.1.255
        );
        assertTrue(isValidAddress(entry, InetAddresses.forString("10.0.1.0")));
        assertTrue(isValidAddress(entry, InetAddresses.forString("10.0.1.255")));
        assertFalse(isValidAddress(entry, InetAddresses.forString("10.0.0.255")));
        assertFalse(isValidAddress(entry, InetAddresses.forString("10.0.2.0")));

        entry = new HashMap.SimpleEntry<>(
            "0", Collections.emptyMap() // key "address" is missing
        );
        assertTrue(isValidAddress(entry,
            InetAddresses.forString(
                String.format("%s.%s.%s.%s", randomInt(255), randomInt(255), randomInt(255), randomInt(255)))
            )
        );
    }

    @Test
    public void testConvertSettingsToConf() throws Exception {
        Settings settings = Settings.builder()
            .put("auth.host_based.enabled", true)
            .put("auth.host_based.config",
                "0", new String[]{"user", "address", "method", "protocol"}, new String[]{"crate", "127.0.0.1", "trust", "pg"})
            .put("auth.host_based.config",
                "1", new String[]{"user", "address", "method", "protocol"}, new String[]{"crate", "0.0.0.0/0", "fake", "pg"})
            .put("auth.host_based.config",
                "2", new String[]{}, new String[]{}) // ignored because empty
            .build();

        HostBasedAuthentication authService = new HostBasedAuthentication(settings, null);
        assertThat(authService.hbaConf(), is(createHbaConf(HBA_1, HBA_2)));
    }
}
