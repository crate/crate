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
import io.crate.protocols.postgres.ConnectionProperties;
import io.crate.test.integration.CrateUnitTest;
import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import org.apache.commons.lang3.RandomStringUtils;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.settings.Settings;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import javax.net.ssl.SSLSession;
import java.net.InetAddress;
import java.security.cert.Certificate;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static io.crate.operation.auth.HostBasedAuthentication.Matchers.isValidAddress;
import static io.crate.operation.auth.HostBasedAuthentication.Matchers.isValidProtocol;
import static io.crate.operation.auth.HostBasedAuthentication.Matchers.isValidUser;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.core.Is.is;
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

    private static final ImmutableMap<String, String> HBA_4 = ImmutableMap.<String, String>builder()
        .put("address", "_local_")
        .build();

    private static final InetAddress LOCALHOST = InetAddresses.forString("127.0.0.1");

    private HostBasedAuthentication authService;
    private SSLSession sslSession;

    @Before
    private void setUpTest() throws Exception {
        Settings settings = Settings.builder()
            .put(AuthSettings.AUTH_HOST_BASED_ENABLED_SETTING.getKey(), true)
            .build();
        authService = new HostBasedAuthentication(settings, null);

        SelfSignedCertificate ssc = new SelfSignedCertificate();
        SslHandler sslHandler = SslContextBuilder
            .forServer(ssc.certificate(), ssc.privateKey())
            .trustManager(InsecureTrustManagerFactory.INSTANCE)
            .startTls(false)
            .build().newHandler(ByteBufAllocator.DEFAULT);
        sslSession = sslHandler.engine().getSession();
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
    public void testMissingUserOrAddress() throws Exception {
        authService.updateHbaConfig(Collections.emptyMap());
        AuthenticationMethod method;
        method = authService.resolveAuthenticationType(null, new ConnectionProperties(LOCALHOST, Protocol.POSTGRES, null));
        assertNull(method);
        method = authService.resolveAuthenticationType("crate", new ConnectionProperties(null, Protocol.POSTGRES, null));
        assertNull(method);
    }

    @Test
    public void testEmptyHbaConf() throws Exception {
        authService.updateHbaConfig(Collections.emptyMap());
        AuthenticationMethod method =
            authService.resolveAuthenticationType("crate", new ConnectionProperties(LOCALHOST, Protocol.POSTGRES, null));
        assertNull(method);
    }

    @Test
    public void testResolveAuthMethod() throws Exception {
        authService.updateHbaConfig(createHbaConf(HBA_1));
        AuthenticationMethod method =
            authService.resolveAuthenticationType("crate", new ConnectionProperties(LOCALHOST, Protocol.POSTGRES, null));
        assertThat(method, instanceOf(TrustAuthenticationMethod.class));
    }

    @Test
    public void testFilterEntriesSimple() throws Exception {
        authService.updateHbaConfig(createHbaConf(HBA_1));
        Optional entry;

        entry = authService.getEntry("crate", new ConnectionProperties(LOCALHOST, Protocol.POSTGRES, null));
        assertThat(entry.isPresent(), is(true));

        entry = authService.getEntry("cr8", new ConnectionProperties(LOCALHOST, Protocol.POSTGRES, null));
        assertThat(entry.isPresent(), is(false));

        entry = authService.getEntry("crate",
            new ConnectionProperties(InetAddresses.forString("10.0.0.1"), Protocol.POSTGRES, null));
        assertThat(entry.isPresent(), is(false));
    }

    @Test
    public void testFilterEntriesCIDR() throws Exception {
        authService.updateHbaConfig(createHbaConf(HBA_2, HBA_3));
        Optional<Map.Entry<String, Map<String, String>>> entry;

        entry = authService.getEntry("crate",
            new ConnectionProperties(InetAddresses.forString("123.45.67.89"), Protocol.POSTGRES, null));
        assertTrue(entry.isPresent());
        assertThat(entry.get().getValue().get("method"), is("fake"));

        entry = authService.getEntry("cr8",
            new ConnectionProperties(InetAddresses.forString("127.0.0.1"), Protocol.POSTGRES, null));
        assertTrue(entry.isPresent());
        assertThat(entry.get().getValue().get("method"), is("md5"));

        entry = authService.getEntry("cr8",
            new ConnectionProperties(InetAddresses.forString("123.45.67.89"), Protocol.POSTGRES, null));
        assertThat(entry.isPresent(), is(false));
    }

    @Test
    public void testLocalhostMatchesBothIpv4AndIpv6() {
        authService.updateHbaConfig(createHbaConf(HBA_4));
        Optional<Map.Entry<String, Map<String, String>>> entry;
        entry = authService.getEntry("crate",
            new ConnectionProperties(InetAddresses.forString("127.0.0.1"), Protocol.POSTGRES, null));
        assertTrue(entry.isPresent());
        entry = authService.getEntry("crate",
            new ConnectionProperties(InetAddresses.forString("::1"), Protocol.POSTGRES, null));
        assertTrue(entry.isPresent());
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
        assertTrue(isValidProtocol("pg", Protocol.POSTGRES));
        assertFalse(isValidProtocol("http", Protocol.POSTGRES));
        assertTrue(isValidProtocol(null, Protocol.POSTGRES));
    }

    @Test
    public void testMatchAddress() throws Exception {
        String hbaAddress = "10.0.1.100";
        assertTrue(isValidAddress(hbaAddress, InetAddresses.forString("10.0.1.100")));
        assertFalse(isValidAddress(hbaAddress, InetAddresses.forString("10.0.1.99")));
        assertFalse(isValidAddress(hbaAddress, InetAddresses.forString("10.0.1.101")));

        hbaAddress = "10.0.1.0/24";  // 10.0.1.0 -- 10.0.1.255
        assertTrue(isValidAddress(hbaAddress, InetAddresses.forString("10.0.1.0")));
        assertTrue(isValidAddress(hbaAddress, InetAddresses.forString("10.0.1.255")));
        assertFalse(isValidAddress(hbaAddress, InetAddresses.forString("10.0.0.255")));
        assertFalse(isValidAddress(hbaAddress, InetAddresses.forString("10.0.2.0")));

        assertTrue(isValidAddress(null,
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

    @Test
    public void testPSQLSslOption() {
        Map<String, String> sslConfig;

        sslConfig = ImmutableMap.<String, String>builder().putAll(HBA_1)
            .put(HostBasedAuthentication.SSL.KEY, HostBasedAuthentication.SSL.OPTIONAL.VALUE).build();
        authService.updateHbaConfig(createHbaConf(sslConfig));
        assertThat(
            authService.getEntry("crate", new ConnectionProperties(LOCALHOST, Protocol.POSTGRES, null)),
            not(Optional.empty()));
        assertThat(
            authService.getEntry("crate", new ConnectionProperties(LOCALHOST, Protocol.POSTGRES, sslSession)),
            not(Optional.empty()));

        sslConfig = ImmutableMap.<String, String>builder().putAll(HBA_1)
            .put(HostBasedAuthentication.SSL.KEY, HostBasedAuthentication.SSL.REQUIRED.VALUE).build();
        authService.updateHbaConfig(createHbaConf(sslConfig));
        assertThat(
            authService.getEntry("crate", new ConnectionProperties(LOCALHOST, Protocol.POSTGRES, null)),
            is(Optional.empty()));
        assertThat(
            authService.getEntry("crate", new ConnectionProperties(LOCALHOST, Protocol.POSTGRES, sslSession)),
                not(Optional.empty()));

        sslConfig = ImmutableMap.<String, String>builder().putAll(HBA_1)
            .put(HostBasedAuthentication.SSL.KEY, HostBasedAuthentication.SSL.NEVER.VALUE).build();
        authService.updateHbaConfig(createHbaConf(sslConfig));
        assertThat(
            authService.getEntry("crate", new ConnectionProperties(LOCALHOST, Protocol.POSTGRES, null)),
            not(Optional.empty()));
        assertThat(
            authService.getEntry("crate", new ConnectionProperties(LOCALHOST, Protocol.POSTGRES, sslSession)),
            is(Optional.empty()));
    }

    @Test
    public void testHttpSSLOption() throws Exception {
        Map<String, String> baseConfig = new HashMap<>();
        baseConfig.putAll(HBA_1);
        baseConfig.put(HostBasedAuthentication.KEY_PROTOCOL, "http");

        SSLSession sslSession = Mockito.mock(SSLSession.class);
        when(sslSession.getPeerCertificates()).thenReturn(new Certificate[0]);
        ConnectionProperties sslConnProperties =
            new ConnectionProperties(LOCALHOST, Protocol.HTTP, sslSession);
        ConnectionProperties noSslConnProperties =
            new ConnectionProperties(LOCALHOST, Protocol.HTTP, null);

        Map<String, String> sslConfig;

        sslConfig = ImmutableMap.<String, String>builder().putAll(baseConfig)
            .put(HostBasedAuthentication.SSL.KEY, HostBasedAuthentication.SSL.OPTIONAL.VALUE).build();
        authService.updateHbaConfig(createHbaConf(sslConfig));
        assertThat(authService.getEntry("crate", noSslConnProperties), not(Optional.empty()));
        assertThat(authService.getEntry("crate", sslConnProperties), not(Optional.empty()));

        sslConfig = ImmutableMap.<String, String>builder().putAll(baseConfig)
            .put(HostBasedAuthentication.SSL.KEY, HostBasedAuthentication.SSL.REQUIRED.VALUE).build();
        authService.updateHbaConfig(createHbaConf(sslConfig));
        assertThat(authService.getEntry("crate", noSslConnProperties), is(Optional.empty()));
        assertThat(authService.getEntry("crate", sslConnProperties), not(Optional.empty()));

        sslConfig = ImmutableMap.<String, String>builder().putAll(baseConfig)
            .put(HostBasedAuthentication.SSL.KEY, HostBasedAuthentication.SSL.NEVER.VALUE).build();
        authService.updateHbaConfig(createHbaConf(sslConfig));
        assertThat(authService.getEntry("crate", noSslConnProperties), not(Optional.empty()));
        assertThat(authService.getEntry("crate", sslConnProperties), is(Optional.empty()));
    }
}
