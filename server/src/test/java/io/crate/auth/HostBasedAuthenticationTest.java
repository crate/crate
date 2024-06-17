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
import static io.crate.role.metadata.RolesHelper.JWT_TOKEN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.security.cert.Certificate;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import javax.net.ssl.SSLSession;

import org.elasticsearch.common.network.DnsResolver;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.InternalSettingsPreparer;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import io.crate.auth.HostBasedAuthentication.HBAConf;
import io.crate.auth.HostBasedAuthentication.SSL;
import io.crate.protocols.postgres.ConnectionProperties;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.handler.ssl.util.SelfSignedCertificate;


public class HostBasedAuthenticationTest extends ESTestCase {

    private static final Credentials CRATE_USER_CREDENTIALS = new Credentials("crate", null);

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
        .put("auth.host_based.config.2.method", "password")
        .put("auth.host_based.config.2.protocol", "pg")
        .build();

    private static final Settings HBA_3 = Settings.builder()
        .put("auth.host_based.config.3.address", "127.0.0.1")
        .put("auth.host_based.config.3.method", "password")
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
    private static final DnsResolver IN_MEMORY_RESOLVER = new DnsResolver() {

        @Override
        public InetAddress[] resolve(String host) throws UnknownHostException {
            return switch (host) {
                case TEST_DNS_HOSTNAME -> new InetAddress[] { InetAddresses.forString(TEST_DNS_IP) };
                case TEST_SUBDOMAIN_HOSTNAME -> new InetAddress[] { InetAddresses.forString(TEST_SUBDOMAIN_IP) };
                case TEST_DOMAIN_HOSTNAME -> new InetAddress[] { InetAddresses.forString(TEST_DOMAIN_IP) };
                default -> new InetAddress[0];
            };
        }
    };

    private SSLSession sslSession;

    @BeforeClass
    public static void ensureEnglishLocale() {
        // BouncyCastle is parsing date objects with the system locale while creating self-signed SSL certs
        // This fails for certain locales, e.g. 'ks'.
        // Until this is fixed, we force the english locale.
        // See also https://github.com/bcgit/bc-java/issues/405 (different topic, but same root cause)
        Locale.setDefault(Locale.ENGLISH);
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
        HostBasedAuthentication authService = new HostBasedAuthentication(
            Settings.EMPTY,
            null,
            DnsResolver.SYSTEM,
            () -> "dummy"
        );
        AuthenticationMethod method;
        Credentials nullCredentials = new Credentials(null, null);
        method = authService.resolveAuthenticationType(null, new ConnectionProperties(nullCredentials, LOCALHOST, Protocol.POSTGRES, null));
        assertNull(method);
        method = authService.resolveAuthenticationType("crate", new ConnectionProperties(CRATE_USER_CREDENTIALS, null, Protocol.POSTGRES, null));
        assertNull(method);
    }

    @Test
    public void testEmptyHbaConf() {
        HostBasedAuthentication authService = new HostBasedAuthentication(
            Settings.EMPTY,
            null,
            DnsResolver.SYSTEM,
            () -> "dummy"
        );
        AuthenticationMethod method =
            authService.resolveAuthenticationType("crate", new ConnectionProperties(CRATE_USER_CREDENTIALS, LOCALHOST, Protocol.POSTGRES, null));
        assertNull(method);
    }

    @Test
    public void testResolveAuthMethod() {
        HostBasedAuthentication authService = new HostBasedAuthentication(
            HBA_1,
            null,
            DnsResolver.SYSTEM,
            () -> "dummy"
        );
        AuthenticationMethod method =
            authService.resolveAuthenticationType("crate", new ConnectionProperties(CRATE_USER_CREDENTIALS, LOCALHOST, Protocol.POSTGRES, null));
        assertThat(method).isExactlyInstanceOf(TrustAuthenticationMethod.class);
    }

    @Test
    public void test_resolve_jwt_method() {
        HostBasedAuthentication authService = new HostBasedAuthentication(
            HBA_6,
            null,
            DnsResolver.SYSTEM,
            () -> "dummy"
        );
        Credentials credentials = new Credentials(JWT_TOKEN);
        AuthenticationMethod method =
            authService.resolveAuthenticationType("jwt_user", new ConnectionProperties(credentials, LOCALHOST, Protocol.HTTP, null));
        assertThat(method).isExactlyInstanceOf(JWTAuthenticationMethod.class);
    }

    @Test
    public void test_invalid_jwt_config_throws_error() {
        // No protocol specified with jwt method ==> cannot enable jwt for all protocols, it's supported only for http.
        Settings noProtocolSettings = Settings.builder()
            .put("auth.host_based.config.1.user", "debug")
            .put("auth.host_based.config.1.method", "jwt")
            .build();
        assertThatThrownBy(
            () -> new HostBasedAuthentication(
                noProtocolSettings,
                null,
                DnsResolver.SYSTEM,
                () -> "dummy"
            )
        ).isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessage("protocol must be set to http when using jwt auth method");

        // Non-http protocol specified with jwt method ==> it's supported only for http.
        Settings unsupportedProtocolSettings = Settings.builder()
            .put("auth.host_based.config.1.user", "debug")
            .put("auth.host_based.config.1.method", "jwt")
            .put("auth.host_based.config.1.protocol", "pg")
            .build();
        assertThatThrownBy(
            () -> new HostBasedAuthentication(
                unsupportedProtocolSettings,
                null,
                DnsResolver.SYSTEM,
                () -> "dummy"
            )
        ).isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessage("protocol must be set to http when using jwt auth method");
    }

    @Test
    public void testFilterEntriesSimple() {
        HostBasedAuthentication authService = new HostBasedAuthentication(
            HBA_1,
            null,
            DnsResolver.SYSTEM,
            () -> "dummy"
        );
        Optional<Map.Entry<String, HBAConf>> entry;

        entry = authService.getEntry("crate", new ConnectionProperties(CRATE_USER_CREDENTIALS, LOCALHOST, Protocol.POSTGRES, null));
        assertThat(entry.isPresent()).isTrue();

        entry = authService.getEntry(
            "cr8",
            new ConnectionProperties(new Credentials("cr8", null), LOCALHOST, Protocol.POSTGRES, null)
        );
        assertThat(entry.isPresent()).isFalse();

        entry = authService.getEntry(
            "crate",
            new ConnectionProperties(CRATE_USER_CREDENTIALS, InetAddresses.forString("10.0.0.1"), Protocol.POSTGRES, null)
        );
        assertThat(entry.isPresent()).isFalse();
    }

    @Test
    public void testFilterEntriesCIDR() {
        Settings settings = Settings.builder().put(HBA_2).put(HBA_3).build();
        HostBasedAuthentication authService = new HostBasedAuthentication(
            settings,
            null,
            DnsResolver.SYSTEM,
            () -> "dummy"
        );

        Optional<Map.Entry<String, HBAConf>> entry;

        entry = authService.getEntry("crate",
            new ConnectionProperties(CRATE_USER_CREDENTIALS, InetAddresses.forString("123.45.67.89"), Protocol.POSTGRES, null));
        assertThat(entry.isPresent()).isTrue();
        assertThat(entry.get().getValue().method()).isEqualTo("password");

        entry = authService.getEntry(
            "cr8",
            new ConnectionProperties(new Credentials("cr8", null), InetAddresses.forString("127.0.0.1"), Protocol.POSTGRES, null)
        );
        assertThat(entry.isPresent()).isTrue();
        assertThat(entry.get().getValue().method()).isEqualTo("password");

        entry = authService.getEntry(
            "cr8",
            new ConnectionProperties(new Credentials("cr8", null), InetAddresses.forString("123.45.67.89"), Protocol.POSTGRES, null)
        );
        assertThat(entry.isPresent()).isFalse();
    }

    @Test
    public void testLocalhostMatchesBothIpv4AndIpv6() {
        HostBasedAuthentication authService = new HostBasedAuthentication(
            HBA_4,
            null,
            DnsResolver.SYSTEM,
            () -> "dummy"
        );

        Optional<Map.Entry<String, HBAConf>> entry;
        entry = authService.getEntry("crate",
            new ConnectionProperties(CRATE_USER_CREDENTIALS, InetAddresses.forString("127.0.0.1"), Protocol.POSTGRES, null));
        assertThat(entry.isPresent()).isTrue();
        entry = authService.getEntry("crate",
            new ConnectionProperties(CRATE_USER_CREDENTIALS, InetAddresses.forString("::1"), Protocol.POSTGRES, null));
        assertThat(entry.isPresent()).isTrue();
    }

    @Test
    public void test_filter_entries_hostname() {
        HostBasedAuthentication authService = new HostBasedAuthentication(
            HBA_5,
            null,
            IN_MEMORY_RESOLVER,
            () -> "dummy"
        );

        Optional<?> entry = authService.getEntry("crate",
            new ConnectionProperties(CRATE_USER_CREDENTIALS, InetAddresses.forString(TEST_DNS_IP), Protocol.POSTGRES, null));

        assertThat(entry.isPresent()).isTrue();
    }

    @Test
    public void testMatchUser() throws Exception {
        // only "crate" matches
        Map.Entry<String, HBAConf> entry = Map.entry("0", new HBAConf("trust", SSL.OPTIONAL, "crate", null, null));
        assertThat(isValidUser(entry, "crate")).isTrue();
        assertThat(isValidUser(entry, "postgres")).isFalse();

        // any user matches
        entry = Map.entry("0", new HBAConf("trust", SSL.OPTIONAL, null, null, null));
        assertThat(isValidUser(entry, randomAlphaOfLength(8))).isTrue();
    }

    @Test
    public void testMatchProtocol() throws Exception {
        assertThat(isValidProtocol("pg", Protocol.POSTGRES)).isTrue();
        assertThat(isValidProtocol("http", Protocol.POSTGRES)).isFalse();
        assertThat(isValidProtocol(null, Protocol.POSTGRES)).isTrue();
    }

    @Test
    public void testMatchAddress() throws Exception {
        String hbaAddress = "10.0.1.100";
        assertThat(isValidAddress(hbaAddress, InetAddresses.forString("10.0.1.100"), DnsResolver.SYSTEM)).isTrue();
        assertThat(isValidAddress(hbaAddress, InetAddresses.forString("10.0.1.99"), DnsResolver.SYSTEM)).isFalse();
        assertThat(isValidAddress(hbaAddress, InetAddresses.forString("10.0.1.101"), DnsResolver.SYSTEM)).isFalse();

        hbaAddress = "10.0.1.0/24";  // 10.0.1.0 -- 10.0.1.255
        assertThat(isValidAddress(hbaAddress, InetAddresses.forString("10.0.1.0"), DnsResolver.SYSTEM)).isTrue();
        assertThat(isValidAddress(hbaAddress, InetAddresses.forString("10.0.1.255"), DnsResolver.SYSTEM)).isTrue();
        assertThat(isValidAddress(hbaAddress, InetAddresses.forString("10.0.0.255"), DnsResolver.SYSTEM)).isFalse();
        assertThat(isValidAddress(hbaAddress, InetAddresses.forString("10.0.2.0"), DnsResolver.SYSTEM)).isFalse();

        hbaAddress = ".b.crate.io";

        InetAddress randomAddress = InetAddresses.forString(
              String.format("%s.%s.%s.%s",
                            randomInt(255),
                            randomInt(255),
                            randomInt(255),
                            randomInt(255)));
        long randomAddressAsLong = HostBasedAuthentication.Matchers.inetAddressToInt(randomAddress);

        assertThat(isValidAddress(hbaAddress, randomAddressAsLong, () -> TEST_SUBDOMAIN_HOSTNAME, IN_MEMORY_RESOLVER)).isTrue();
        assertThat(isValidAddress(hbaAddress, randomAddressAsLong, () -> TEST_DOMAIN_HOSTNAME, IN_MEMORY_RESOLVER)).isFalse();

        assertThat(isValidAddress(null, randomAddress, DnsResolver.SYSTEM)).isTrue();
    }

    @Test
    public void test_cidr_check_with_ip_requiring_all_bits() throws Exception {
        String hbaAddress = "192.168.0.0/16";
        assertThat(isValidAddress(hbaAddress, InetAddresses.forString("192.168.101.92"), DnsResolver.SYSTEM)).isTrue();
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

        HostBasedAuthentication authService = new HostBasedAuthentication(
            settings,
            null,
            DnsResolver.SYSTEM,
            () -> "dummy"
        );
        Settings confirmSettings = Settings.builder().put(HBA_1).put(HBA_2).build();
        assertThat(authService.hbaConf()).isEqualTo(authService.convertHbaSettingsToHbaConf(confirmSettings));
    }

    @Test
    public void testPSQLSslOption() {
        Settings sslConfig;
        HostBasedAuthentication authService;

        sslConfig = Settings.builder().put(HBA_1)
            .put("auth.host_based.config.1." + HostBasedAuthentication.SSL.KEY, HostBasedAuthentication.SSL.OPTIONAL.VALUE)
            .build();
        authService = new HostBasedAuthentication(
            sslConfig,
            null,
            DnsResolver.SYSTEM,
            () -> "dummy"
        );
        assertThat(
            authService.getEntry("crate", new ConnectionProperties(CRATE_USER_CREDENTIALS, LOCALHOST, Protocol.POSTGRES, null)),
            not(Optional.empty()));
        assertThat(
            authService.getEntry("crate", new ConnectionProperties(CRATE_USER_CREDENTIALS, LOCALHOST, Protocol.POSTGRES, sslSession)),
            not(Optional.empty()));

        sslConfig = Settings.builder().put(HBA_1)
            .put("auth.host_based.config.1." + HostBasedAuthentication.SSL.KEY, HostBasedAuthentication.SSL.REQUIRED.VALUE)
            .build();
        authService = new HostBasedAuthentication(
            sslConfig,
            null,
            DnsResolver.SYSTEM,
            () -> "dummy"
        );
        assertThat(
            authService.getEntry("crate", new ConnectionProperties(CRATE_USER_CREDENTIALS, LOCALHOST, Protocol.POSTGRES, null))).isEqualTo(Optional.empty());
        assertThat(
            authService.getEntry("crate", new ConnectionProperties(CRATE_USER_CREDENTIALS, LOCALHOST, Protocol.POSTGRES, sslSession)),
                not(Optional.empty()));

        sslConfig = Settings.builder().put(HBA_1)
            .put("auth.host_based.config.1." + HostBasedAuthentication.SSL.KEY, HostBasedAuthentication.SSL.NEVER.VALUE)
            .build();
        authService = new HostBasedAuthentication(
            sslConfig,
            null,
            DnsResolver.SYSTEM,
            () -> "dummy"
        );
        assertThat(
            authService.getEntry("crate", new ConnectionProperties(CRATE_USER_CREDENTIALS, LOCALHOST, Protocol.POSTGRES, null)),
            not(Optional.empty()));
        assertThat(
            authService.getEntry("crate", new ConnectionProperties(CRATE_USER_CREDENTIALS, LOCALHOST, Protocol.POSTGRES, sslSession))).isEqualTo(Optional.empty());
    }

    @Test
    public void testHttpSSLOption() throws Exception {
        Settings baseConfig = Settings.builder().put(HBA_1)
            .put("auth.host_based.config.1.protocol", "http")
            .build();

        SSLSession sslSession = mock(SSLSession.class);
        when(sslSession.getPeerCertificates()).thenReturn(new Certificate[0]);
        ConnectionProperties sslConnProperties =
            new ConnectionProperties(CRATE_USER_CREDENTIALS, LOCALHOST, Protocol.HTTP, sslSession);
        ConnectionProperties noSslConnProperties =
            new ConnectionProperties(CRATE_USER_CREDENTIALS, LOCALHOST, Protocol.HTTP, null);

        Settings sslConfig;
        HostBasedAuthentication authService;

        sslConfig = Settings.builder().put(baseConfig)
            .put("auth.host_based.config.1." + HostBasedAuthentication.SSL.KEY, HostBasedAuthentication.SSL.OPTIONAL.VALUE)
            .build();
        authService = new HostBasedAuthentication(
            sslConfig,
            null,
            DnsResolver.SYSTEM,
            () -> "dummy"
        );
        assertThat(authService.getEntry("crate", noSslConnProperties), not(Optional.empty()));
        assertThat(authService.getEntry("crate", sslConnProperties), not(Optional.empty()));

        sslConfig = Settings.builder().put(baseConfig)
            .put("auth.host_based.config.1." + HostBasedAuthentication.SSL.KEY, HostBasedAuthentication.SSL.REQUIRED.VALUE)
            .build();
        authService = new HostBasedAuthentication(
            sslConfig,
            null,
            DnsResolver.SYSTEM,
            () -> "dummy"
        );
        assertThat(authService.getEntry("crate", noSslConnProperties)).isEqualTo(Optional.empty());
        assertThat(authService.getEntry("crate", sslConnProperties), not(Optional.empty()));

        sslConfig = Settings.builder().put(baseConfig)
            .put("auth.host_based.config.1." + HostBasedAuthentication.SSL.KEY, HostBasedAuthentication.SSL.NEVER.VALUE)
            .build();
        authService = new HostBasedAuthentication(
            sslConfig,
            null,
            DnsResolver.SYSTEM,
            () -> "dummy"
        );
        assertThat(authService.getEntry("crate", noSslConnProperties), not(Optional.empty()));
        assertThat(authService.getEntry("crate", sslConnProperties)).isEqualTo(Optional.empty());
    }

    public void testKeyOrderIsRespectedInHbaConfig() throws Exception {
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
        HostBasedAuthentication hba = new HostBasedAuthentication(
            settings,
            null,
            DnsResolver.SYSTEM,
            () -> "dummy"
        );

        SSLSession sslSession = mock(SSLSession.class);
        when(sslSession.getPeerCertificates()).thenReturn(new Certificate[]{mock(Certificate.class)});

        AuthenticationMethod authMethod = hba.resolveAuthenticationType("crate",
            new ConnectionProperties(CRATE_USER_CREDENTIALS, InetAddresses.forString("1.2.3.4"), Protocol.POSTGRES, sslSession));
        assertThat(authMethod).isExactlyInstanceOf(TrustAuthenticationMethod.class);

        AuthenticationMethod authMethod2 = hba.resolveAuthenticationType("crate",
            new ConnectionProperties(CRATE_USER_CREDENTIALS, InetAddresses.forString("1.2.3.4"), Protocol.HTTP, sslSession));
        assertThat(authMethod2).isExactlyInstanceOf(ClientCertAuth.class);
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
        assertThat(finalSettings.get("auth.host_based.config.0.ssl")).isEqualTo("true");

        HostBasedAuthentication hba = new HostBasedAuthentication(
            finalSettings,
            null,
            DnsResolver.SYSTEM,
            () -> "dummy"
        );
        SSLSession sslSession = mock(SSLSession.class);
        when(sslSession.getPeerCertificates()).thenReturn(new Certificate[]{mock(Certificate.class)});

        AuthenticationMethod authMethod = hba.resolveAuthenticationType("crate",
            new ConnectionProperties(CRATE_USER_CREDENTIALS, InetAddresses.forString("1.2.3.4"), Protocol.TRANSPORT, sslSession));
        assertThat(authMethod).isExactlyInstanceOf(ClientCertAuth.class);

        AuthenticationMethod authMethod2 = hba.resolveAuthenticationType("crate",
            new ConnectionProperties(CRATE_USER_CREDENTIALS, InetAddresses.forString("1.2.3.4"), Protocol.TRANSPORT, sslSession));
        assertThat(authMethod2).isExactlyInstanceOf(ClientCertAuth.class);
    }

    @Test
    public void test_multiple_matches_method_uniquely_identified_by_properties() {
        // Validating different declaration order as auth used to fail depending on order.
        Settings pwdFirst = Settings.builder()
            .put("auth.host_based.config.1.method", "password")
            .put("auth.host_based.config.2.method", "jwt")
            .put("auth.host_based.config.2.protocol", "http")
            .build();
        Settings jwtFirst = Settings.builder()
            .put("auth.host_based.config.1.method", "jwt")
            .put("auth.host_based.config.1.protocol", "http")
            .put("auth.host_based.config.2.method", "password")
            .build();
        HostBasedAuthentication pwdFirstService = new HostBasedAuthentication(
            pwdFirst,
            null,
            DnsResolver.SYSTEM,
            () -> "dummy"
        );
        HostBasedAuthentication jwtFirstService = new HostBasedAuthentication(
            jwtFirst,
            null,
            DnsResolver.SYSTEM,
            () -> "dummy"
        );
        AuthenticationMethod authMethod;
        ConnectionProperties jwtConnProps = new ConnectionProperties(new Credentials(JWT_TOKEN), LOCALHOST, Protocol.HTTP, sslSession);
        ConnectionProperties pwdConnProps = new ConnectionProperties(new Credentials("dummy", new char[]{'a'}), LOCALHOST, Protocol.HTTP, sslSession);

        // Password first config can handle connection with jwt property
        authMethod = pwdFirstService.resolveAuthenticationType("dummy", jwtConnProps);
        assertThat(authMethod).isExactlyInstanceOf(JWTAuthenticationMethod.class);

        // Password first config can handle connection with password property
        authMethod = pwdFirstService.resolveAuthenticationType("dummy", pwdConnProps);
        assertThat(authMethod).isExactlyInstanceOf(PasswordAuthenticationMethod.class);

        // JWT first config can handle connection with jwt property
        authMethod = jwtFirstService.resolveAuthenticationType("dummy", jwtConnProps);
        assertThat(authMethod).isExactlyInstanceOf(JWTAuthenticationMethod.class);

        // JWT first config can handle connection with password property
        authMethod = jwtFirstService.resolveAuthenticationType("dummy", pwdConnProps);
        assertThat(authMethod).isExactlyInstanceOf(PasswordAuthenticationMethod.class);
    }

    @Test
    public void test_multiple_matches_after_properties_check_pick_up_first() throws Exception {
        // This test covers scenario when multiple client auth methods are available.
        // First matching HBA entry wins in this case as well.
        Settings certFirstSettings = Settings.builder()
            .put("auth.host_based.config.1.method", "cert")
            .put("auth.host_based.config.1.ssl", "on")
            .put("auth.host_based.config.2.method", "jwt")
            .put("auth.host_based.config.2.protocol", "http")
            .build();
        HostBasedAuthentication certFirstService = new HostBasedAuthentication(
            certFirstSettings,
            null,
            DnsResolver.SYSTEM,
            () -> "dummy"
        );

        Settings jwtFirstSettings = Settings.builder()
            .put("auth.host_based.config.1.method", "jwt")
            .put("auth.host_based.config.1.protocol", "http")
            .put("auth.host_based.config.2.method", "cert")
            .put("auth.host_based.config.2.ssl", "on")
            .build();
        HostBasedAuthentication jwtFirstService = new HostBasedAuthentication(
            jwtFirstSettings,
            null,
            DnsResolver.SYSTEM,
            () -> "dummy"
        );

        AuthenticationMethod authMethod;
        SSLSession sslSession = mock(SSLSession.class);
        when(sslSession.getPeerCertificates()).thenReturn(new Certificate[]{mock(Certificate.class)});
        ConnectionProperties connProps = new ConnectionProperties(new Credentials(JWT_TOKEN), LOCALHOST, Protocol.HTTP, sslSession);

        authMethod = certFirstService.resolveAuthenticationType("dummy", connProps);
        assertThat(authMethod).isExactlyInstanceOf(ClientCertAuth.class);

        authMethod = jwtFirstService.resolveAuthenticationType("dummy", connProps);
        assertThat(authMethod).isExactlyInstanceOf(JWTAuthenticationMethod.class);
    }

    @Test
    public void test_entry_without_method_resolves_to_trust() {
        // Settings without method to ensure that method check doesn't throw an NPE.
        Settings settings = Settings.builder()
            .put("auth.host_based.config.1.protocol", "http")
            .build();
        HostBasedAuthentication authService = new HostBasedAuthentication(
            settings,
            null,
            DnsResolver.SYSTEM,
            () -> "dummy"
        );
        AuthenticationMethod authMethod = authService.resolveAuthenticationType(
            "dummy",
            new ConnectionProperties(new Credentials("dummy", null), LOCALHOST, Protocol.HTTP, null)
        );
        assertThat(authMethod).isExactlyInstanceOf(TrustAuthenticationMethod.class);
    }
}
