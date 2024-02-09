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

package io.crate.auth;

import static io.crate.auth.AuthenticationWithSSLIntegrationTest.getAbsoluteFilePathFromClassPath;
import static io.crate.auth.HostBasedAuthentication.Matchers.isValidAddress;
import static io.crate.auth.HostBasedAuthentication.Matchers.isValidProtocol;
import static io.crate.auth.HostBasedAuthentication.Matchers.isValidUser;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.InetAddress;
import java.nio.file.Path;
import java.security.cert.Certificate;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import javax.net.ssl.SSLSession;

import org.apache.http.impl.conn.InMemoryDnsResolver;
import org.apache.http.impl.conn.SystemDefaultDnsResolver;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.InternalSettingsPreparer;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import io.crate.protocols.postgres.ConnectionProperties;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.handler.ssl.util.SelfSignedCertificate;


public class HostBasedAuthenticationTest extends ESTestCase {

    private static final String TEST_DNS_HOSTNAME = "node1.test-cluster.crate.io";
    private static final String TEST_DNS_IP = "192.168.10.20";

    private static final String TEST_DOMAIN_HOSTNAME = "b.crate.io";
    private static final String TEST_DOMAIN_IP = "192.168.10.40";
    private static final String TEST_SUBDOMAIN_HOSTNAME = "a.b.crate.io";
    private static final String TEST_SUBDOMAIN_IP = "192.168.10.30";

    private static final Settings HBA_1 = Settings.builder()
        .put("auth.host_based.config.1.user", "crate")
        .put("auth.host_based.config.1.address", "127.0.0.1")
        .put("auth.host_based.config.1.method", "trust")
        .put("auth.host_based.config.1.protocol", "pg")
        .build();

    private static final Settings HBA_2 = Settings.builder()
        .put("auth.host_based.config.2.user", "crate")
        .put("auth.host_based.config.2.address", "0.0.0.0/0")
        .put("auth.host_based.config.2.method", "fake")
        .put("auth.host_based.config.2.protocol", "pg")
        .build();

    private static final Settings HBA_3 = Settings.builder()
        .put("auth.host_based.config.3.address", "127.0.0.1")
        .put("auth.host_based.config.3.method", "md5")
        .put("auth.host_based.config.3.protocol", "pg")
        .build();

    private static final Settings HBA_4 = Settings.builder()
        .put("auth.host_based.config.4.address", "_local_")
        .build();

    private static final Settings HBA_5 = Settings.builder()
        .put("auth.host_based.config.1.user", "crate")
        .put("auth.host_based.config.1.address", TEST_DNS_HOSTNAME)
        .put("auth.host_based.config.1.method", "trust")
        .put("auth.host_based.config.1.protocol", "pg")
        .build();

    private static final Settings HBA_6 = Settings.builder()
        .put("auth.host_based.config.1.user", "jwt_user")
        .put("auth.host_based.config.1.method", "jwt")
        .put("auth.host_based.config.1.protocol", "http")
        .build();

    private static final InetAddress LOCALHOST = InetAddresses.forString("127.0.0.1");
    private static final InMemoryDnsResolver IN_MEMORY_RESOLVER = new InMemoryDnsResolver();

    private SSLSession sslSession;

    @BeforeClass
    public static void ensureEnglishLocale() {
        // BouncyCastle is parsing date objects with the system locale while creating self-signed SSL certs
        // This fails for certain locales, e.g. 'ks'.
        // Until this is fixed, we force the english locale.
        // See also https://github.com/bcgit/bc-java/issues/405 (different topic, but same root cause)
        Locale.setDefault(Locale.ENGLISH);
        IN_MEMORY_RESOLVER.add(TEST_DNS_HOSTNAME, InetAddresses.forString(TEST_DNS_IP));
        IN_MEMORY_RESOLVER.add(TEST_SUBDOMAIN_HOSTNAME, InetAddresses.forString(TEST_SUBDOMAIN_IP));
        IN_MEMORY_RESOLVER.add(TEST_DOMAIN_HOSTNAME, InetAddresses.forString(TEST_DOMAIN_IP));
    }

    @Before
    private void setUpTest() throws Exception {
        SelfSignedCertificate ssc = new SelfSignedCertificate();
        SslHandler sslHandler = SslContextBuilder
            .forServer(ssc.certificate(), ssc.privateKey())
            .trustManager(InsecureTrustManagerFactory.INSTANCE)
            .startTls(false)
            .build().newHandler(UnpooledByteBufAllocator.DEFAULT);
        sslSession = sslHandler.engine().getSession();

    }

    @Test
    public void testMissingUserOrAddress() {
        HostBasedAuthentication authService = new HostBasedAuthentication(Settings.EMPTY, null, SystemDefaultDnsResolver.INSTANCE);
        AuthenticationMethod method;
        method = authService.resolveAuthenticationType(null, new ConnectionProperties(LOCALHOST, Protocol.POSTGRES, null));
        assertNull(method);
        method = authService.resolveAuthenticationType("crate", new ConnectionProperties(null, Protocol.POSTGRES, null));
        assertNull(method);
    }

    @Test
    public void testEmptyHbaConf() {
        HostBasedAuthentication authService = new HostBasedAuthentication(Settings.EMPTY, null, SystemDefaultDnsResolver.INSTANCE);
        AuthenticationMethod method =
            authService.resolveAuthenticationType("crate", new ConnectionProperties(LOCALHOST, Protocol.POSTGRES, null));
        assertNull(method);
    }

    @Test
    public void testResolveAuthMethod() {
        HostBasedAuthentication authService = new HostBasedAuthentication(HBA_1, null, SystemDefaultDnsResolver.INSTANCE);
        AuthenticationMethod method =
            authService.resolveAuthenticationType("crate", new ConnectionProperties(LOCALHOST, Protocol.POSTGRES, null));
        assertThat(method, instanceOf(TrustAuthenticationMethod.class));
    }

    @Test
    public void test_resolve_jwt_method() {
        HostBasedAuthentication authService = new HostBasedAuthentication(HBA_6, null, SystemDefaultDnsResolver.INSTANCE);
        AuthenticationMethod method =
            authService.resolveAuthenticationType("jwt_user", new ConnectionProperties(LOCALHOST, Protocol.HTTP, null));
        assertThat(method, instanceOf(JWTAuthenticationMethod.class));
    }

    @Test
    public void test_invalid_jwt_config_throws_error() {
        // No protocol specified with jwt method ==> cannot enable jwt for all protocols, it's supported only for http.
        Settings noProtocolSettings = Settings.builder()
            .put("auth.host_based.config.1.user", "debug")
            .put("auth.host_based.config.1.method", "jwt")
            .build();
        assertThatThrownBy(
            () -> new HostBasedAuthentication(noProtocolSettings, null, SystemDefaultDnsResolver.INSTANCE)
        ).isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessage("protocol must be set to http when using jwt auth method");

        // Non-http protocol specified with jwt method ==> it's supported only for http.
        Settings unsupportedProtocolSettings = Settings.builder()
            .put("auth.host_based.config.1.user", "debug")
            .put("auth.host_based.config.1.method", "jwt")
            .put("auth.host_based.config.1.protocol", "pg")
            .build();
        assertThatThrownBy(
            () -> new HostBasedAuthentication(unsupportedProtocolSettings, null, SystemDefaultDnsResolver.INSTANCE)
        ).isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessage("protocol must be set to http when using jwt auth method");
    }

    @Test
    public void testFilterEntriesSimple() {
        HostBasedAuthentication authService = new HostBasedAuthentication(HBA_1, null, SystemDefaultDnsResolver.INSTANCE);
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
    public void testFilterEntriesCIDR() {
        Settings settings = Settings.builder().put(HBA_2).put(HBA_3).build();
        HostBasedAuthentication authService = new HostBasedAuthentication(settings, null, SystemDefaultDnsResolver.INSTANCE);

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
        HostBasedAuthentication authService = new HostBasedAuthentication(HBA_4, null, SystemDefaultDnsResolver.INSTANCE);

        Optional<Map.Entry<String, Map<String, String>>> entry;
        entry = authService.getEntry("crate",
            new ConnectionProperties(InetAddresses.forString("127.0.0.1"), Protocol.POSTGRES, null));
        assertTrue(entry.isPresent());
        entry = authService.getEntry("crate",
            new ConnectionProperties(InetAddresses.forString("::1"), Protocol.POSTGRES, null));
        assertTrue(entry.isPresent());
    }

    @Test
    public void test_filter_entries_hostname() {
        HostBasedAuthentication authService = new HostBasedAuthentication(HBA_5, null, IN_MEMORY_RESOLVER);

        Optional entry = authService.getEntry("crate",
            new ConnectionProperties(InetAddresses.forString(TEST_DNS_IP), Protocol.POSTGRES, null));

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
        assertTrue(isValidUser(entry, randomAlphaOfLength(8)));
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
        assertTrue(isValidAddress(hbaAddress, InetAddresses.forString("10.0.1.100"), SystemDefaultDnsResolver.INSTANCE));
        assertFalse(isValidAddress(hbaAddress, InetAddresses.forString("10.0.1.99"), SystemDefaultDnsResolver.INSTANCE));
        assertFalse(isValidAddress(hbaAddress, InetAddresses.forString("10.0.1.101"), SystemDefaultDnsResolver.INSTANCE));

        hbaAddress = "10.0.1.0/24";  // 10.0.1.0 -- 10.0.1.255
        assertTrue(isValidAddress(hbaAddress, InetAddresses.forString("10.0.1.0"), SystemDefaultDnsResolver.INSTANCE));
        assertTrue(isValidAddress(hbaAddress, InetAddresses.forString("10.0.1.255"), SystemDefaultDnsResolver.INSTANCE));
        assertFalse(isValidAddress(hbaAddress, InetAddresses.forString("10.0.0.255"), SystemDefaultDnsResolver.INSTANCE));
        assertFalse(isValidAddress(hbaAddress, InetAddresses.forString("10.0.2.0"), SystemDefaultDnsResolver.INSTANCE));

        hbaAddress = ".b.crate.io";

        InetAddress randomAddress = InetAddresses.forString(
              String.format("%s.%s.%s.%s",
                            randomInt(255),
                            randomInt(255),
                            randomInt(255),
                            randomInt(255)));
        long randomAddressAsLong = HostBasedAuthentication.Matchers.inetAddressToInt(randomAddress);

        assertTrue(isValidAddress(hbaAddress, randomAddressAsLong, () -> TEST_SUBDOMAIN_HOSTNAME, IN_MEMORY_RESOLVER));
        assertFalse(isValidAddress(hbaAddress, randomAddressAsLong, () -> TEST_DOMAIN_HOSTNAME, IN_MEMORY_RESOLVER));

        assertTrue(isValidAddress(null, randomAddress, SystemDefaultDnsResolver.INSTANCE));
    }

    @Test
    public void test_cidr_check_with_ip_requiring_all_bits() throws Exception {
        String hbaAddress = "192.168.0.0/16";
        assertTrue(isValidAddress(hbaAddress, InetAddresses.forString("192.168.101.92"), SystemDefaultDnsResolver.INSTANCE));
    }

    @Test
    public void testConvertSettingsToConf() throws Exception {
        Settings settings = Settings.builder()
            .put("auth.host_based.enabled", true)
            .put(HBA_1)
            .put(HBA_2)
            .put("auth.host_based.config",
                "3", new String[]{}, new String[]{}) // ignored because empty
            .build();

        HostBasedAuthentication authService = new HostBasedAuthentication(settings, null, SystemDefaultDnsResolver.INSTANCE);
        Settings confirmSettings = Settings.builder().put(HBA_1).put(HBA_2).build();
        assertThat(authService.hbaConf(), is(authService.convertHbaSettingsToHbaConf(confirmSettings)));
    }

    @Test
    public void testPSQLSslOption() {
        Settings sslConfig;
        HostBasedAuthentication authService;

        sslConfig = Settings.builder().put(HBA_1)
            .put("auth.host_based.config.1." + HostBasedAuthentication.SSL.KEY, HostBasedAuthentication.SSL.OPTIONAL.VALUE)
            .build();
        authService = new HostBasedAuthentication(sslConfig, null, SystemDefaultDnsResolver.INSTANCE);
        assertThat(
            authService.getEntry("crate", new ConnectionProperties(LOCALHOST, Protocol.POSTGRES, null)),
            not(Optional.empty()));
        assertThat(
            authService.getEntry("crate", new ConnectionProperties(LOCALHOST, Protocol.POSTGRES, sslSession)),
            not(Optional.empty()));

        sslConfig = Settings.builder().put(HBA_1)
            .put("auth.host_based.config.1." + HostBasedAuthentication.SSL.KEY, HostBasedAuthentication.SSL.REQUIRED.VALUE)
            .build();
        authService = new HostBasedAuthentication(sslConfig, null, SystemDefaultDnsResolver.INSTANCE);
        assertThat(
            authService.getEntry("crate", new ConnectionProperties(LOCALHOST, Protocol.POSTGRES, null)),
            is(Optional.empty()));
        assertThat(
            authService.getEntry("crate", new ConnectionProperties(LOCALHOST, Protocol.POSTGRES, sslSession)),
                not(Optional.empty()));

        sslConfig = Settings.builder().put(HBA_1)
            .put("auth.host_based.config.1." + HostBasedAuthentication.SSL.KEY, HostBasedAuthentication.SSL.NEVER.VALUE)
            .build();
        authService = new HostBasedAuthentication(sslConfig, null, SystemDefaultDnsResolver.INSTANCE);
        assertThat(
            authService.getEntry("crate", new ConnectionProperties(LOCALHOST, Protocol.POSTGRES, null)),
            not(Optional.empty()));
        assertThat(
            authService.getEntry("crate", new ConnectionProperties(LOCALHOST, Protocol.POSTGRES, sslSession)),
            is(Optional.empty()));
    }

    @Test
    public void testHttpSSLOption() throws Exception {
        Settings baseConfig = Settings.builder().put(HBA_1)
            .put("auth.host_based.config.1.protocol", "http")
            .build();

        SSLSession sslSession = mock(SSLSession.class);
        when(sslSession.getPeerCertificates()).thenReturn(new Certificate[0]);
        ConnectionProperties sslConnProperties =
            new ConnectionProperties(LOCALHOST, Protocol.HTTP, sslSession);
        ConnectionProperties noSslConnProperties =
            new ConnectionProperties(LOCALHOST, Protocol.HTTP, null);

        Settings sslConfig;
        HostBasedAuthentication authService;

        sslConfig = Settings.builder().put(baseConfig)
            .put("auth.host_based.config.1." + HostBasedAuthentication.SSL.KEY, HostBasedAuthentication.SSL.OPTIONAL.VALUE)
            .build();
        authService = new HostBasedAuthentication(sslConfig, null, SystemDefaultDnsResolver.INSTANCE);
        assertThat(authService.getEntry("crate", noSslConnProperties), not(Optional.empty()));
        assertThat(authService.getEntry("crate", sslConnProperties), not(Optional.empty()));

        sslConfig = Settings.builder().put(baseConfig)
            .put("auth.host_based.config.1." + HostBasedAuthentication.SSL.KEY, HostBasedAuthentication.SSL.REQUIRED.VALUE)
            .build();
        authService = new HostBasedAuthentication(sslConfig, null, SystemDefaultDnsResolver.INSTANCE);
        assertThat(authService.getEntry("crate", noSslConnProperties), is(Optional.empty()));
        assertThat(authService.getEntry("crate", sslConnProperties), not(Optional.empty()));

        sslConfig = Settings.builder().put(baseConfig)
            .put("auth.host_based.config.1." + HostBasedAuthentication.SSL.KEY, HostBasedAuthentication.SSL.NEVER.VALUE)
            .build();
        authService = new HostBasedAuthentication(sslConfig, null, SystemDefaultDnsResolver.INSTANCE);
        assertThat(authService.getEntry("crate", noSslConnProperties), not(Optional.empty()));
        assertThat(authService.getEntry("crate", sslConnProperties), is(Optional.empty()));
    }

    public void testKeyOrderIsRespectedInHbaConfig() {
        Settings first = Settings.builder()
            .put("auth.host_based.config.1.method", "trust")
            .put("auth.host_based.config.1.protocol", "pg")
            .build();
        Settings second = Settings.builder()
            .put("auth.host_based.config.2.method", "cert")
            .put("auth.host_based.config.2.protocol", "http")
            .build();

        // add in reverse order to test natural order of keys in config
        Settings settings = Settings.builder().put(second).put(first).build();
        HostBasedAuthentication hba = new HostBasedAuthentication(settings, null, SystemDefaultDnsResolver.INSTANCE);

        AuthenticationMethod authMethod = hba.resolveAuthenticationType("crate",
            new ConnectionProperties(InetAddresses.forString("1.2.3.4"), Protocol.POSTGRES, null));
        assertThat(authMethod, instanceOf(TrustAuthenticationMethod.class));

        AuthenticationMethod authMethod2 = hba.resolveAuthenticationType("crate",
            new ConnectionProperties(InetAddresses.forString("1.2.3.4"), Protocol.HTTP, null));
        assertThat(authMethod2, instanceOf(ClientCertAuth.class));
    }


    @Test
    public void cert_method_resolved_when_ssl_on_and_keystore_configured() throws Exception {
        // This test makes sure that "ssl: on" from crate.yml in test resources
        // is correctly mapped to the corresponding enum value despite on yml treats "on" as "true".

        Path config = getAbsoluteFilePathFromClassPath("org/elasticsearch/node/config").toPath();

        HashMap<String, String> settings = new HashMap<>();
        settings.put("path.home", ".");
        settings.put("path.conf", config.toAbsolutePath().toString());
        settings.put("stats.enabled", "false");

        // Settings are intentionally created not by directly putting properties
        // but by using InternalSettingsPreparer.prepareEnvironment to trigger yml parsing.
        Settings finalSettings = InternalSettingsPreparer
            .prepareEnvironment(Settings.EMPTY, settings, config, () -> "node1").settings();

        // 'on' becomes 'true' -
        assertThat(finalSettings.get("auth.host_based.config.0.ssl"), is("true"));

        HostBasedAuthentication hba = new HostBasedAuthentication(finalSettings, null, SystemDefaultDnsResolver.INSTANCE);
        AuthenticationMethod authMethod = hba.resolveAuthenticationType("crate",
            new ConnectionProperties(InetAddresses.forString("1.2.3.4"), Protocol.TRANSPORT, mock(SSLSession.class)));
        assertThat(authMethod, instanceOf(ClientCertAuth.class));

        AuthenticationMethod authMethod2 = hba.resolveAuthenticationType("crate",
            new ConnectionProperties(InetAddresses.forString("1.2.3.4"), Protocol.TRANSPORT, mock(SSLSession.class)));
        assertThat(authMethod2, instanceOf(ClientCertAuth.class));
    }
}
