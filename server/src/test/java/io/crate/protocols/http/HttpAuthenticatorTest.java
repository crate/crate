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

package io.crate.protocols.http;

import static io.crate.protocols.http.HttpAuthenticator.WWW_AUTHENTICATE_REALM_MESSAGE;
import static io.crate.role.metadata.RolesHelper.DUMMY_USERS;
import static io.crate.role.metadata.RolesHelper.JWT_TOKEN;
import static io.crate.role.metadata.RolesHelper.JWT_USER;
import static io.crate.role.metadata.RolesHelper.getSecureHash;
import static io.crate.role.metadata.RolesHelper.userOf;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.cert.Certificate;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import javax.net.ssl.SSLSession;

import org.elasticsearch.common.network.DnsResolver;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;
import org.mockito.Mockito;

import io.crate.auth.AlwaysOKAuthentication;
import io.crate.auth.AuthSettings;
import io.crate.auth.Authentication;
import io.crate.auth.AuthenticationMethod;
import io.crate.auth.AuthenticationMethod.AuthToken;
import io.crate.auth.Credentials;
import io.crate.auth.HostBasedAuthentication;
import io.crate.auth.JWTAuthenticationMethod;
import io.crate.auth.PasswordAuthenticationMethod;
import io.crate.protocols.postgres.ConnectionProperties;
import io.crate.role.Role;
import io.crate.role.Roles;
import io.crate.role.StubRoleManager;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.pkitesting.CertificateBuilder;

public class HttpAuthenticatorTest extends ESTestCase {

    private final Settings hbaEnabled = Settings.builder()
        .put("auth.host_based.enabled", true)
        .put("auth.host_based.config.0.user", "crate")
        .build();

    // Roles always returns null, so there are no users (even no default crate superuser)
    private final Authentication authService = new HostBasedAuthentication(
        hbaEnabled,
        List::of,
        DnsResolver.SYSTEM,
        () -> "dummy"
    );
    private final HttpAuthenticator authenticatorWithHBA =
        new HttpAuthenticator(Settings.EMPTY, authService, new StubRoleManager());

    private static HttpRequest sqlRequest() {
        return new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/_sql");
    }

    private static void assertUnauthorized(DefaultFullHttpResponse resp, String expectedBody) {
        assertThat(resp.status()).isEqualTo(HttpResponseStatus.UNAUTHORIZED);
        assertThat(resp.content().toString(StandardCharsets.UTF_8)).isEqualTo(expectedBody);
        assertThat(resp.headers().get(HttpHeaderNames.WWW_AUTHENTICATE)).isEqualTo(WWW_AUTHENTICATE_REALM_MESSAGE);
    }

    @Test
    public void testChannelClosedWhenUnauthorized() throws Exception {
        EmbeddedChannel ch = new EmbeddedChannel();
        HttpAuthenticator.sendUnauthorized(ch, null);
        ch.releaseInbound();

        HttpResponse resp = ch.readOutbound();
        assertThat(resp.status()).isEqualTo(HttpResponseStatus.UNAUTHORIZED);
        assertThat(ch.isOpen()).isFalse();
    }

    @Test
    public void testSendUnauthorizedWithoutBody() throws Exception {
        EmbeddedChannel ch = new EmbeddedChannel();
        HttpAuthenticator.sendUnauthorized(ch, null);
        ch.releaseInbound();

        DefaultFullHttpResponse resp = ch.readOutbound();
        assertThat(resp.content()).isEqualTo(Unpooled.EMPTY_BUFFER);
    }

    @Test
    public void testSendUnauthorizedWithBody() throws Exception {
        EmbeddedChannel ch = new EmbeddedChannel();
        HttpAuthenticator.sendUnauthorized(ch, "not allowed\n");
        ch.releaseInbound();

        DefaultFullHttpResponse resp = ch.readOutbound();
        assertThat(resp.content().toString(StandardCharsets.UTF_8)).isEqualTo("not allowed\n");
    }

    @Test
    public void testSendUnauthorizedWithBodyNoNewline() throws Exception {
        EmbeddedChannel ch = new EmbeddedChannel();
        HttpAuthenticator.sendUnauthorized(ch, "not allowed");
        ch.releaseInbound();

        DefaultFullHttpResponse resp = ch.readOutbound();
        assertThat(resp.content().toString(StandardCharsets.UTF_8)).isEqualTo("not allowed\n");
    }

    @Test
    public void testAuthorized() throws Exception {
        HttpAuthenticator authenticator = new HttpAuthenticator(
            Settings.EMPTY, new AlwaysOKAuthentication(() -> List.of(Role.CRATE_USER)), new StubRoleManager());

        assertThat(authenticator.authenticate(sqlRequest(), new EmbeddedChannel())).isEqualTo(Role.CRATE_USER);
    }

    @Test
    public void testNoHbaConfig() throws Exception {
        EmbeddedChannel ch = new EmbeddedChannel();
        HttpRequest request = sqlRequest();
        request.headers().add(HttpHeaderNames.AUTHORIZATION, "Basic QWxhZGRpbjpPcGVuU2VzYW1l");

        assertThat(authenticatorWithHBA.authenticate(request, ch)).isNull();
        assertUnauthorized(
            ch.readOutbound(),
            "No valid auth.host_based.config entry found for host \"127.0.0.1\", user \"Aladdin\", protocol \"http\". Did you enable TLS in your client?\n");
    }

    /**
     * Ensure that the {@code X-Real-IP} header is ignored by default as this allows to by-pass HBA rules.
     * See https://github.com/crate/crate/issues/15231.
     */
    @Test
    public void test_real_ip_header_is_ignored_by_default() {
        EmbeddedChannel ch = new EmbeddedChannel();
        HttpRequest request = sqlRequest();
        request.headers().add(HttpHeaderNames.AUTHORIZATION, "Basic QWxhZGRpbjpPcGVuU2VzYW1l");
        request.headers().add("X-Real-IP", "10.1.0.100");

        assertThat(authenticatorWithHBA.authenticate(request, ch)).isNull();
        assertUnauthorized(
            ch.readOutbound(),
            "No valid auth.host_based.config entry found for host \"127.0.0.1\", user \"Aladdin\", protocol \"http\". Did you enable TLS in your client?\n");
    }

    @Test
    public void test_real_ip_header_is_used_if_enabled() {
        var settings = Settings.builder()
            .put(AuthSettings.AUTH_TRUST_HTTP_SUPPORT_X_REAL_IP.getKey(), true)
            .build();
        HttpAuthenticator authenticator = new HttpAuthenticator(settings, authService, new StubRoleManager());
        EmbeddedChannel ch = new EmbeddedChannel();
        HttpRequest request = sqlRequest();
        request.headers().add(HttpHeaderNames.AUTHORIZATION, "Basic QWxhZGRpbjpPcGVuU2VzYW1l");
        request.headers().add("X-Real-IP", "10.1.0.100");

        assertThat(authenticator.authenticate(request, ch)).isNull();
        assertUnauthorized(
            ch.readOutbound(),
            "No valid auth.host_based.config entry found for host \"10.1.0.100\", user \"Aladdin\", protocol \"http\". Did you enable TLS in your client?\n");
    }

    @Test
    public void test_real_ip_header_blacklist() {
        var settings = Settings.builder()
            .put(AuthSettings.AUTH_TRUST_HTTP_SUPPORT_X_REAL_IP.getKey(), true)
            .build();
        HttpAuthenticator authenticator = new HttpAuthenticator(settings, authService, new StubRoleManager());
        EmbeddedChannel ch = new EmbeddedChannel();
        HttpRequest request = sqlRequest();
        request.headers().add(HttpHeaderNames.AUTHORIZATION, "Basic QWxhZGRpbjpPcGVuU2VzYW1l");
        request.headers().add("X-Real-IP", "::1");

        assertThat(authenticator.authenticate(request, ch)).isNull();
        assertUnauthorized(
            ch.readOutbound(),
            "No valid auth.host_based.config entry found for host \"127.0.0.1\", user \"Aladdin\", protocol \"http\". Did you enable TLS in your client?\n");
    }

    @Test
    public void test_real_ip_header_non_canonical_loopback() {
        var settings = Settings.builder()
            .put(AuthSettings.AUTH_HOST_BASED_ENABLED_SETTING.getKey(), true)
            .put("auth.host_based.config.0.user", "crate")
            .put("auth.host_based.config.0.address", "_local_")
            .put("auth.host_based.config.0.method", "trust")
            .put("auth.host_based.config.99.method", "password")
            .put(AuthSettings.AUTH_TRUST_HTTP_SUPPORT_X_REAL_IP.getKey(), true)
            .build();

        StubRoleManager roles = new StubRoleManager();
        var hbaAuth = new HostBasedAuthentication(
            settings,
            roles,
            DnsResolver.SYSTEM,
            () -> "dummy"
        );
        HttpAuthenticator authenticator = new HttpAuthenticator(settings, hbaAuth, roles);
        EmbeddedChannel ch = new EmbeddedChannel() {
            @Override
            public SocketAddress remoteAddress() {
                return new InetSocketAddress(InetAddress.ofLiteral("192.168.0.100"), 0);
            }
        };

        HttpRequest request = sqlRequest();
        request.headers().add("X-Real-IP", "::ffff:127.0.0.1");

        assertThat(authenticator.authenticate(request, ch)).isNull();
        assertUnauthorized(
            ch.readOutbound(),
            "No valid auth.host_based.config entry found for host \"192.168.0.100\", user \"crate\", protocol \"http\". Did you enable TLS in your client?\n");
    }

    @Test
    public void testUnauthorizedUser() throws Exception {
        EmbeddedChannel ch = new EmbeddedChannel();

        assertThat(authenticatorWithHBA.authenticate(sqlRequest(), ch)).isNull();
        assertUnauthorized(ch.readOutbound(), "trust authentication failed for user \"crate\"\n");
    }

    @Test
    public void testClientCertUserHasPreferenceOverTrustAuthDefault() throws Exception {
        var ssc = new CertificateBuilder()
            .subject("CN=localhost")
            .setIsCertificateAuthority(true)
            .buildSelfSigned();
        SSLSession session = mock(SSLSession.class);
        when(session.getPeerCertificates()).thenReturn(new Certificate[] { ssc.getCertificate() });

        HttpRequest request = sqlRequest();
        String userName = HttpAuthenticator.credentialsFromRequest(request, session, "default-user").username();

        assertThat(userName).isEqualTo("localhost");
    }

    @Test
    public void testUserAuthenticationWithDisabledHBA() throws Exception {
        Authentication authServiceNoHBA = new AlwaysOKAuthentication(() -> List.of(Role.CRATE_USER));
        HttpAuthenticator authenticator = new HttpAuthenticator(Settings.EMPTY, authServiceNoHBA, new StubRoleManager());

        HttpRequest request = sqlRequest();
        request.headers().add(HttpHeaderNames.AUTHORIZATION, "Basic Y3JhdGU6");

        assertThat(authenticator.authenticate(request, new EmbeddedChannel())).isEqualTo(Role.CRATE_USER);
    }

    @Test
    public void testUnauthorizedUserWithDisabledHBA() throws Exception {
        Authentication authServiceNoHBA = new AlwaysOKAuthentication(List::of);
        HttpAuthenticator authenticator = new HttpAuthenticator(Settings.EMPTY, authServiceNoHBA, new StubRoleManager());
        EmbeddedChannel ch = new EmbeddedChannel();

        HttpRequest request = sqlRequest();
        request.headers().add(HttpHeaderNames.AUTHORIZATION, "Basic QWxhZGRpbjpPcGVuU2VzYW1l");

        assertThat(authenticator.authenticate(request, ch)).isNull();
        assertUnauthorized(ch.readOutbound(), "trust authentication failed for user \"Aladdin\"\n");
    }

    @Test
    public void test_user_authentication_with_jwt_token() throws Exception {
        Roles roles = () -> List.of(JWT_USER);
        Authentication authentication = mock(Authentication.class);
        AuthenticationMethod jwtAuth = mock(JWTAuthenticationMethod.class);
        when(authentication.resolveAuthenticationType(eq(JWT_USER.name()), any(ConnectionProperties.class)))
            .thenReturn(jwtAuth);
        when(jwtAuth.authenticate(any(Credentials.class), any(ConnectionProperties.class)))
            .thenReturn(AuthToken.of(JWT_USER));

        HttpAuthenticator authenticator = new HttpAuthenticator(Settings.EMPTY, authentication, roles);
        HttpRequest request = sqlRequest();
        request.headers().add(HttpHeaderNames.AUTHORIZATION, "Bearer " + JWT_TOKEN);

        assertThat(authenticator.authenticate(request, new EmbeddedChannel())).isEqualTo(JWT_USER);
    }

    @Test
    public void test_user_auth_with_invalid_jwt_token_returns_401() throws Exception {
        String brokenToken = "Bearer eyJhbGciOiJSUzI1NiIsImtpZCI6IlJVSFJ5QjFrd1FNaVRxRnVmWDF1T25tRlBTZDN3Z0lSMlBoYXUzZzVsNVkiLCJ1c2UiOiJzaWcifQS05OTgzLTQzODUtYTI1Yi1lMjk5OWI1YWMzNTIiLCJjbHVzdGVyX2lkIjoiNzMzODBhNzktOTk4My00Mzg1LWEyNWItZTI5OTliNWFjMzUyIiwiZXhwIjoxNzg4OTQ1NDY3LCJpc3MiOiJodHRwczovL2NvbnNvbGUuY3JhdGVkYi1kZXYuY2xvdWQvYXBpL3YyL21ldGEvandrLyIsImp0aSI6Ijk3MDZiMDc0LTRkNGYtNDZkZi1hODI5LWMwNmY0MzllNjgxZSIsInN1YiI6ImpvbkBjcmF0ZS5pbyIsInR5cGUiOiJhY2Nlc3MiLCJ1c2VybmFtZSI6ImFkbWluIn0.HxI6IKg3_tbcpndEimZLwwtHtHJuWtkOn6wSTTaKU8JaKlls4dIIqKH1OSBD1DGKa1urKmDOqs_co-aP8i2vbkP7z09rNvGy-NGvjx9RdlkS_shQZdsRQIRXoG7MKH3z7fT9U1-31OMjfNuqpFGIxrtKDHhZt8Dagz7E7Z_uPDHus1HsBlJL3YxH9HB81U8BvmMxFe6puIzDB5Y5y83q5s7CjFF_61Srcl5kGL1fQjtOLelF6L25Zv1gVpPYw5TDU5Xb9S5r1eXV9vLF0wN0izFjOrNgJIOYEhP51ZjBof0tt3GWqTzMxW14P8CDKtz1VZxlcFCg27KQKOSDv8fNA51SiuijDSyii3NZ-UiEsAC7ukd_6ixkmtgGiCvYkhCJCNfv6P9kv059KfuMkS24YyzRv0x1dvWRDMDF-dMavmAKXz-IQagY3tudwTem23Yo5NVDB0qnlZAmyBzMEE-q7ApaCXwgI9TnwDocNCOxnx2gBk40j_MRc4oWY5qICb3MICA8oKeZm4GwpSIG5cqJNq3zSJ6sdudX3tyhEeECzPMLSQsIlIXEiSAcPuObHc48vdCo5qOnbTVBTOQS--aZWStU8LtKX17ZZ887alskrB0b7iT5qmfmjGNMSv3sXWHyM-vdHOh88-5R67F6mCx0mDLVC-XSM5nsCfTKfUJFUak";

        Roles roles = () -> List.of(JWT_USER);
        Authentication authentication = mock(Authentication.class);
        HttpAuthenticator authenticator = new HttpAuthenticator(Settings.EMPTY, authentication, roles);
        EmbeddedChannel ch = new EmbeddedChannel();

        HttpRequest request = sqlRequest();
        request.headers().add(HttpHeaderNames.AUTHORIZATION, brokenToken);

        assertThat(authenticator.authenticate(request, ch)).isNull();
        assertUnauthorized(ch.readOutbound(), "The token was expected to have 3 parts, but got 2.\n");
    }

    @Test
    public void test_user_authentication_with_jwt_token_user_not_found() throws Exception {
        EmbeddedChannel ch = new EmbeddedChannel();
        HttpRequest request = sqlRequest();
        request.headers().add(HttpHeaderNames.AUTHORIZATION, "Bearer " + JWT_TOKEN);

        assertThat(authenticatorWithHBA.authenticate(request, ch)).isNull();
        HttpResponse resp = ch.readOutbound();
        assertThat(resp.status()).isEqualTo(HttpResponseStatus.UNAUTHORIZED);
    }

    @Test
    public void test_user_authentication_with_jwt_token_verified_per_request() throws Exception {
        Roles roles = () -> List.of(JWT_USER);
        Authentication authentication = mock(Authentication.class);
        AuthenticationMethod jwtAuth = mock(JWTAuthenticationMethod.class);
        when(authentication.resolveAuthenticationType(eq(JWT_USER.name()), any(ConnectionProperties.class)))
            .thenReturn(jwtAuth);
        when(jwtAuth.authenticate(any(Credentials.class), any(ConnectionProperties.class)))
            .thenReturn(AuthToken.of(JWT_USER));

        HttpAuthenticator authenticator = new HttpAuthenticator(Settings.EMPTY, authentication, roles);
        EmbeddedChannel ch = new EmbeddedChannel();

        HttpRequest request = sqlRequest();
        request.headers().add(HttpHeaderNames.AUTHORIZATION, "Bearer " + JWT_TOKEN);
        authenticator.authenticate(request, ch);

        HttpRequest request2 = sqlRequest();
        request2.headers().add(HttpHeaderNames.AUTHORIZATION, "Bearer " + JWT_TOKEN);
        authenticator.authenticate(request2, ch);

        // The authenticator does not cache; every request is authenticated afresh.
        verify(jwtAuth, times(2)).authenticate(any(Credentials.class), any(ConnectionProperties.class));
    }

    @Test
    public void test_auth_header_verified_once_per_connection() throws Exception {
        Authentication authentication = mock(Authentication.class);
        Role user = DUMMY_USERS.get("Ford");
        StubRoleManager roles = new StubRoleManager(List.of(user), false);
        AuthenticationMethod authMethod = mock(AuthenticationMethod.class);
        when(authMethod.authenticate(any(Credentials.class), any(ConnectionProperties.class)))
            .thenReturn(AuthToken.of(user));
        when(authMethod.renew(any(AuthToken.class), any(Credentials.class)))
            .thenAnswer(invocation -> invocation.getArgument(0));
        when(authentication.resolveAuthenticationType(Mockito.anyString(), any(ConnectionProperties.class)))
            .thenReturn(authMethod);
        when(authMethod.name()).thenReturn(PasswordAuthenticationMethod.NAME);
        HttpAuthenticator authenticator = new HttpAuthenticator(Settings.EMPTY, authentication, roles);
        EmbeddedChannel ch = new EmbeddedChannel();

        HttpRequest request = sqlRequest();
        String header = "Basic " + Base64.getEncoder().encodeToString("Ford:fords-password".getBytes(StandardCharsets.UTF_8));

        request.headers().add(HttpHeaderNames.AUTHORIZATION, header);
        authenticator.authenticate(request, ch);

        HttpRequest request2 = sqlRequest();
        request2.headers().add(HttpHeaderNames.AUTHORIZATION, header);
        authenticator.authenticate(request2, ch);

        verify(authMethod, times(1)).authenticate(any(Credentials.class), any(ConnectionProperties.class));
    }

    @Test
    public void test_same_user_different_provided_credentials_not_authenticated() throws Exception {
        Role user = DUMMY_USERS.get("Ford");
        List<Role> users = new ArrayList<>(List.of(user));
        Roles roles = () -> users;
        Settings hba = Settings.builder()
            .put("auth.host_based.enabled", true)
            .put("auth.host_based.config.0.user", "Ford")
            .put("auth.host_based.config.0.method", "password").build();
        var authenticator = new HttpAuthenticator(
            Settings.EMPTY,
            new HostBasedAuthentication(hba, roles, DnsResolver.SYSTEM, () -> "dummy"),
            roles
        );
        EmbeddedChannel ch = new EmbeddedChannel();

        HttpRequest request = sqlRequest();
        String header = "Basic " + Base64.getEncoder().encodeToString("Ford:fords-password".getBytes(StandardCharsets.UTF_8));
        request.headers().add(HttpHeaderNames.AUTHORIZATION, header);
        assertThat(authenticator.authenticate(request, ch)).isEqualTo(user);

        // Someone claims to be Ford with incorrect credentials.
        // Cache must not be used and user must not be authenticated.
        request = sqlRequest();
        header = "Basic " + Base64.getEncoder().encodeToString("Ford:wrong-password".getBytes(StandardCharsets.UTF_8));
        request.headers().add(HttpHeaderNames.AUTHORIZATION, header);
        assertThat(authenticator.authenticate(request, ch)).isNull();
    }

    @Test
    public void test_renew_auth_token_with_stale_password_fails() throws Exception {
        Role user = DUMMY_USERS.get("Ford");
        List<Role> users = new ArrayList<>(List.of(user));
        Roles roles = () -> users;
        Settings hba = Settings.builder()
            .put("auth.host_based.enabled", true)
            .put("auth.host_based.config.0.user", "Ford")
            .put("auth.host_based.config.0.method", "password").build();
        var authenticator = new HttpAuthenticator(
            Settings.EMPTY,
            new HostBasedAuthentication(hba, roles, DnsResolver.SYSTEM, () -> "dummy"),
            roles
        );
        EmbeddedChannel ch = new EmbeddedChannel();

        String oldHeader = "Basic " +
            Base64.getEncoder().encodeToString("Ford:fords-password".getBytes(StandardCharsets.UTF_8));
        String newHeader = "Basic " +
            Base64.getEncoder().encodeToString("Ford:new-password".getBytes(StandardCharsets.UTF_8));

        // First auth, add a cache entry.
        HttpRequest request = sqlRequest();
        request.headers().add(HttpHeaderNames.AUTHORIZATION, oldHeader);
        assertThat(authenticator.authenticate(request, ch)).isEqualTo(user);

        Role updatedUser = userOf("Ford", getSecureHash("new-password"));
        users.set(0, updatedUser);
        request = sqlRequest();
        request.headers().add(HttpHeaderNames.AUTHORIZATION, oldHeader);
        assertThat(authenticator.authenticate(request, ch)).isNull();

        request = sqlRequest();
        request.headers().add(HttpHeaderNames.AUTHORIZATION, newHeader);
        assertThat(authenticator.authenticate(request, ch)).isEqualTo(updatedUser);
    }

    @Test
    public void test_auth_token_not_renewed_if_user_was_dropped() throws Exception {
        Role user = DUMMY_USERS.get("Ford");
        List<Role> users = new ArrayList<>(List.of(user));
        Roles roles = () -> users;
        Settings hba = Settings.builder()
            .put("auth.host_based.enabled", true)
            .put("auth.host_based.config.0.user", "Ford")
            .put("auth.host_based.config.0.method", "password").build();
        var authenticator = new HttpAuthenticator(
            Settings.EMPTY,
            new HostBasedAuthentication(hba, roles, DnsResolver.SYSTEM, () -> "dummy"),
            roles
        );
        EmbeddedChannel ch = new EmbeddedChannel();

        String oldHeader = "Basic " +
            Base64.getEncoder().encodeToString("Ford:fords-password".getBytes(StandardCharsets.UTF_8));
        String newHeader = "Basic " +
            Base64.getEncoder().encodeToString("Ford:new-password".getBytes(StandardCharsets.UTF_8));

        // First auth, add a cache entry.
        HttpRequest request = sqlRequest();
        request.headers().add(HttpHeaderNames.AUTHORIZATION, oldHeader);
        assertThat(authenticator.authenticate(request, ch)).isEqualTo(user);

        // Previous auth added an auth token, after dropping users the renew/auth must fail.
        users.clear();
        request = sqlRequest();
        request.headers().add(HttpHeaderNames.AUTHORIZATION, oldHeader);
        assertThat(authenticator.authenticate(request, ch)).isNull();
    }

}
