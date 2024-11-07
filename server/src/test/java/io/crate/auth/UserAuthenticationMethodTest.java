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

import static io.crate.role.metadata.RolesHelper.JWT_TOKEN;
import static io.crate.role.metadata.RolesHelper.JWT_USER;
import static io.crate.role.metadata.RolesHelper.getSecureHash;
import static io.crate.role.metadata.RolesHelper.userOf;
import static io.crate.testing.auth.RsaKeys.PRIVATE_KEY_256;
import static io.crate.testing.auth.RsaKeys.PUBLIC_KEY_256;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.security.interfaces.RSAPrivateKey;
import java.security.spec.EncodedKeySpec;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Base64;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import com.auth0.jwk.Jwk;
import com.auth0.jwk.JwkProvider;
import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;

import io.crate.role.JwtProperties;
import io.crate.role.Role;
import io.crate.role.Roles;
import io.crate.role.SecureHash;

public class UserAuthenticationMethodTest extends ESTestCase {

    private static final String KID = "1";


    private static class CrateOrNullRoles implements Roles {

        @Override
        public Collection<Role> roles() {
            SecureHash pwHash;
            try {
                pwHash = SecureHash.of(new SecureString("pw".toCharArray()));
            } catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
                throw new RuntimeException(e);
            }
            return List.of(userOf("crate", pwHash));
        }
    }

    @Test
    public void testTrustAuthentication() throws Exception {
        TrustAuthenticationMethod trustAuth = new TrustAuthenticationMethod(new CrateOrNullRoles());
        assertThat(trustAuth.name()).isEqualTo("trust");
        assertThat(trustAuth.authenticate(new Credentials("crate", null), null).name()).isEqualTo("crate");

        assertThatThrownBy(() -> trustAuth.authenticate(new Credentials("cr8", null), null))
            .hasMessage("trust authentication failed for user \"cr8\"");
    }

    @Test
    public void testAlwaysOKAuthentication() throws Exception {
        AlwaysOKAuthentication alwaysOkAuth = new AlwaysOKAuthentication(new CrateOrNullRoles());
        AuthenticationMethod alwaysOkAuthMethod = alwaysOkAuth.resolveAuthenticationType("crate", null);

        assertThat(alwaysOkAuthMethod.name()).isEqualTo("trust");
        assertThat(alwaysOkAuthMethod.authenticate(new Credentials("crate", null), null).name()).isEqualTo("crate");

        assertThatThrownBy(() -> alwaysOkAuthMethod.authenticate(new Credentials("cr8", null), null))
            .hasMessage("trust authentication failed for user \"cr8\"");
    }

    public void testPasswordAuthentication() throws Exception {
        PasswordAuthenticationMethod pwAuth = new PasswordAuthenticationMethod(new CrateOrNullRoles());
        assertThat(pwAuth.name()).isEqualTo("password");

        assertThat(pwAuth.authenticate(new Credentials("crate", "pw".toCharArray()), null).name()).isEqualTo("crate");
    }

    @Test
    public void testPasswordAuthenticationWrongPassword() throws Exception {
        PasswordAuthenticationMethod pwAuth = new PasswordAuthenticationMethod(new CrateOrNullRoles());
        assertThat(pwAuth.name()).isEqualTo("password");

        assertThatThrownBy(() -> pwAuth.authenticate(new Credentials("crate", "wrong".toCharArray()), null))
            .hasMessage("password authentication failed for user \"crate\"");

    }

    @Test
    public void testPasswordAuthenticationForNonExistingUser() throws Exception {
        PasswordAuthenticationMethod pwAuth = new PasswordAuthenticationMethod(new CrateOrNullRoles());
        assertThatThrownBy(() -> pwAuth.authenticate(new Credentials("cr8", "pw".toCharArray()), null))
            .hasMessage("password authentication failed for user \"cr8\"");
    }

    @Test
    public void test_jwt_authentication() throws Exception {
        Roles roles = () -> List.of(JWT_USER);
        JWTAuthenticationMethod jwtAuth = new JWTAuthenticationMethod(
            roles,
            Settings.EMPTY,
            () -> "dummy",
            jwkProviderFunction(null)
        );
        assertThat(jwtAuth.name()).isEqualTo("jwt");

        Credentials credentials = new Credentials(JWT_TOKEN);
        assertThat(credentials.username()).isNull();
        credentials.setUsername(JWT_USER.name());

        Role authenticatedRole = jwtAuth.authenticate(credentials, null);
        assertThat(authenticatedRole).isNotNull();
        assertThat(authenticatedRole.name()).isEqualTo(JWT_USER.name());
    }

    @Test
    public void test_jwt_authentication_default_aud_same_as_token() throws Exception {
        // JWT_TOKEN has aud = "test_cluster_id", imitate that cluster id is same.
        String clusterId = "test_cluster_id";
        Roles roles = () -> List.of(
            userOf(
                "John",
                Set.of(),
                new HashSet<>(),
                getSecureHash("johns-pwd"),
                // User doesn't have "aud" JWT property, cluster id will be used as aud.
                new JwtProperties("https://console.cratedb-dev.cloud/api/v2/meta/jwk/", "cloud_user", null)
            )
        );
        JWTAuthenticationMethod jwtAuth = new JWTAuthenticationMethod(
            roles,
            Settings.EMPTY,
            () -> clusterId,
            jwkProviderFunction(null)
        );

        Credentials credentials = new Credentials(JWT_TOKEN);
        credentials.setUsername(JWT_USER.name());

        Role authenticatedRole = jwtAuth.authenticate(credentials, null);
        assertThat(authenticatedRole.name()).isEqualTo(JWT_USER.name());
    }

    @Test
    public void test_jwt_authentication_default_aud_different_as_token() throws Exception {
        // JWT_TOKEN has aud = "test_cluster_id", imitate that cluster id is different.
        String clusterId = "not_same_as_user_aud";
        Roles roles = () -> List.of(
            userOf(
                "John",
                Set.of(),
                new HashSet<>(),
                getSecureHash("johns-pwd"),
                // User doesn't have "aud" JWT property, cluster id will be used as aud.
                new JwtProperties("https://console.cratedb-dev.cloud/api/v2/meta/jwk/", "cloud_user", null)
            )
        );
        JWTAuthenticationMethod jwtAuth = new JWTAuthenticationMethod(
            roles,
            Settings.EMPTY,
            () -> clusterId,
            jwkProviderFunction(null)
        );

        Credentials credentials = new Credentials(JWT_TOKEN);
        credentials.setUsername(JWT_USER.name());

        assertThatThrownBy(
            () -> jwtAuth.authenticate(credentials, null))
            .isExactlyInstanceOf(RuntimeException.class)
            .hasMessageContaining("jwt authentication failed for user John. Reason: The Claim 'aud' value doesn't contain the required audience.");
    }


    @Test
    @SuppressWarnings("resource")
    public void test_jwt_authentication_token_expired() throws Exception {
        PKCS8EncodedKeySpec privateKeySpec = new PKCS8EncodedKeySpec(Base64.getDecoder().decode(PRIVATE_KEY_256));
        KeyFactory kf = KeyFactory.getInstance("RSA");
        RSAPrivateKey privateKey = (RSAPrivateKey) kf.generatePrivate(privateKeySpec);

        var jwtProperties = JWT_USER.jwtProperties();
        String jwt = JWT.create()
            .withHeader(Map.of("typ", "JWT", "alg", "RS256", "kid", KID))
            .withIssuer(jwtProperties.iss())
            .withClaim("username", jwtProperties.username())
            .withAudience(jwtProperties.aud())
            .withExpiresAt(LocalDateTime.now(ZoneOffset.UTC).minusDays(1).toInstant(ZoneOffset.UTC))
            .sign(Algorithm.RSA256(null, privateKey));

        Roles roles = () -> List.of(JWT_USER);

        JWTAuthenticationMethod jwtAuth = new JWTAuthenticationMethod(
            roles,
            Settings.EMPTY,
            () -> "dummy",
            jwkProviderFunction(null)
        );

        Credentials credentials = new Credentials(jwt);
        credentials.setUsername(JWT_USER.name());

        assertThatThrownBy(
                () -> jwtAuth.authenticate(credentials, null))
            .isExactlyInstanceOf(RuntimeException.class)
            .hasMessageContaining("jwt authentication failed for user John. Reason: The Token has expired");
    }

    @Test
    @SuppressWarnings("resource")
    public void test_jwt_authentication_token_aud_not_provided() throws Exception {
        PKCS8EncodedKeySpec privateKeySpec = new PKCS8EncodedKeySpec(Base64.getDecoder().decode(PRIVATE_KEY_256));
        KeyFactory kf = KeyFactory.getInstance("RSA");
        RSAPrivateKey privateKey = (RSAPrivateKey) kf.generatePrivate(privateKeySpec);

        var jwtProperties = JWT_USER.jwtProperties();
        String jwt = JWT.create()
            .withHeader(Map.of("typ", "JWT", "alg", "RS256", "kid", KID))
            .withIssuer(jwtProperties.iss())
            .withClaim("username", jwtProperties.username())
            .sign(Algorithm.RSA256(null, privateKey));

        Roles roles = () -> List.of(JWT_USER);

        JWTAuthenticationMethod jwtAuth = new JWTAuthenticationMethod(
            roles,
            Settings.EMPTY,
            () -> "dummy",
            jwkProviderFunction(null)
        );

        Credentials credentials = new Credentials(jwt);
        credentials.setUsername(JWT_USER.name());

        assertThatThrownBy(
            () -> jwtAuth.authenticate(credentials, null))
            .isExactlyInstanceOf(RuntimeException.class)
            .hasMessageContaining("jwt authentication failed for user John. Reason: The Claim 'aud' is not present in the JWT.");
    }

    @Test
    @SuppressWarnings("resource")
    public void test_token_algo_and_jwk_algo_mistmatch_throws_error() throws Exception {
        Roles roles = () -> List.of(JWT_USER);
        JWTAuthenticationMethod jwtAuth = new JWTAuthenticationMethod(
            roles,
            Settings.EMPTY,
            () -> "dummy",
            jwkProviderFunction("RS384")
        );

        Credentials credentials = new Credentials(JWT_TOKEN);
        assertThat(credentials.username()).isNull();
        credentials.setUsername(JWT_USER.name());

        assertThatThrownBy(
            () -> jwtAuth.authenticate(credentials, null))
            .isExactlyInstanceOf(RuntimeException.class)
            .hasMessage("jwt authentication failed for user John. Reason: Jwt token has algorithm not matching with the algorithm of the public key.");
    }

    @Test
    public void test_token_issuer_and_resolved_issuer_mistmatch_throws_error() throws Exception {
        PKCS8EncodedKeySpec privateKeySpec = new PKCS8EncodedKeySpec(Base64.getDecoder().decode(PRIVATE_KEY_256));
        KeyFactory kf = KeyFactory.getInstance("RSA");
        RSAPrivateKey privateKey = (RSAPrivateKey) kf.generatePrivate(privateKeySpec);
        String jwt = JWT.create()
            .withHeader(
                Map.of(
                    "typ","JWT",
                    "alg", "RS256",
                    "kid", "1",
                    "aud", "test_cluster_id" // Aligned with JWT_USER's properties.
                )
            )
            .withIssuer("https://malicious.server") // Different from JWT_USER's properties.
            .withClaim("username", "cloud_user") // Aligned with JWT_USER's properties.
            .sign(Algorithm.RSA256(null, privateKey));

        Credentials credentials = new Credentials(jwt);
        credentials.setUsername(JWT_USER.name());

        JWTAuthenticationMethod jwtAuth = new JWTAuthenticationMethod(
            () -> List.of(JWT_USER),
            Settings.EMPTY,
            () -> null,
            jwkProviderFunction(null)
        );

        assertThatThrownBy(
            () -> jwtAuth.authenticate(credentials, null))
            .isExactlyInstanceOf(RuntimeException.class)
            .hasMessage("jwt authentication failed for user John. Reason: The Claim 'iss' value doesn't match the required issuer.");
    }

    @Test
    @SuppressWarnings("resource")
    public void test_jwt_authentication_user_not_found_throws_error() throws Exception {
        // Testing a scenario when user is looked up by iss/username, name is set to Credentials
        // but during authentication user cannot be found by name (for example, could be dropped in a meantime).
        JWTAuthenticationMethod jwtAuth = new JWTAuthenticationMethod(
            List::of,
            Settings.EMPTY,
            () -> "dummy"
        );

        Credentials credentials = new Credentials(JWT_TOKEN);
        credentials.setUsername(JWT_USER.name());

        assertThatThrownBy(
            () -> jwtAuth.authenticate(credentials, null))
            .isExactlyInstanceOf(RuntimeException.class)
            .hasMessage("jwt authentication failed for user \"John\"");
    }

    @Test
    @SuppressWarnings("resource")
    public void test_jwt_authentication_claim_mismatch_throws_error() throws Exception {
        Role userWithModifiedJwtProperty = new Role(
            JWT_USER.name(),
            true,
            Set.of(),
            Set.of(),
            null,
            new JwtProperties("dummy", "dummy", null),
            Map.of()
        );
        Roles roles = () -> List.of(userWithModifiedJwtProperty);
        JWTAuthenticationMethod jwtAuth = new JWTAuthenticationMethod(
            roles,
            Settings.EMPTY,
            () -> "dummy",
            jwkProviderFunction(null)
        );

        Credentials credentials = new Credentials(JWT_TOKEN);
        credentials.setUsername(JWT_USER.name());

        assertThatThrownBy(
            () -> jwtAuth.authenticate(credentials, null))
            .isExactlyInstanceOf(RuntimeException.class)
            .hasMessage("jwt authentication failed for user John. Reason: The Claim 'iss' value doesn't match the required issuer.");
    }

    @Test
    public void test_jwt_authentication_user_has_no_jwt_properties_default_is_used() throws Exception {
        String tokenUsername = "cloud_user"; // Token payload dictates CrateDB username.
        Roles roles = () -> List.of(
            userOf(
                tokenUsername,
                Set.of(),
                new HashSet<>(),
                getSecureHash("pwd")
            )
        );
        var settings = Settings.builder()
            // Matches JWT_TOKEN payload.
            .put(AuthSettings.AUTH_HOST_BASED_JWT_ISS_SETTING.getKey(), "https://console.cratedb-dev.cloud/api/v2/meta/jwk/")
            // Matches JWT_TOKEN payload.
            .put(AuthSettings.AUTH_HOST_BASED_JWT_AUD_SETTING.getKey(), "test_cluster_id")
            .build();
        JWTAuthenticationMethod jwtAuth = new JWTAuthenticationMethod(
            roles,
            settings,
            () -> null, // ClusterId supplier is not used, aud is taken from defaults
            jwkProviderFunction(null)
        );

        Credentials credentials = new Credentials(JWT_TOKEN);
        credentials.setUsername(tokenUsername);

        Role authenticatedRole = jwtAuth.authenticate(credentials, null);
        assertThat(authenticatedRole).isNotNull();
        assertThat(authenticatedRole.name()).isEqualTo(tokenUsername);
    }

    private static Function<String, JwkProvider> jwkProviderFunction(String algorithm) throws Exception {
        EncodedKeySpec publicKeySpec = new X509EncodedKeySpec(Base64.getDecoder().decode(PUBLIC_KEY_256));
        KeyFactory keyFactory = KeyFactory.getInstance("RSA");
        PublicKey publicKey = keyFactory.generatePublic(publicKeySpec);

        JwkProvider mockJwkProvider = mock(JwkProvider.class);

        Jwk mockJwk = mock(Jwk.class);

        when(mockJwkProvider.get(KID)).thenReturn(mockJwk);
        when(mockJwk.getPublicKey()).thenReturn(publicKey);
        when(mockJwk.getAlgorithm()).thenReturn(algorithm);
        return _ -> mockJwkProvider;
    }
}
