/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import static io.crate.auth.AuthSettings.AUTH_HOST_BASED_JWT_AUD_SETTING;
import static io.crate.auth.AuthSettings.AUTH_HOST_BASED_JWT_ISS_SETTING;

import java.security.PublicKey;
import java.security.interfaces.RSAPublicKey;
import java.util.Locale;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Supplier;

import org.elasticsearch.common.settings.Settings;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;

import com.auth0.jwk.Jwk;
import com.auth0.jwk.JwkException;
import com.auth0.jwk.JwkProvider;
import com.auth0.jwt.JWT;
import com.auth0.jwt.JWTVerifier;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.interfaces.DecodedJWT;
import com.auth0.jwt.interfaces.Verification;

import io.crate.protocols.postgres.ConnectionProperties;
import io.crate.role.Role;
import io.crate.role.Roles;

public class JWTAuthenticationMethod implements AuthenticationMethod {

    public static final String NAME = "jwt";

    private final Roles roles;

    private final Function<String, JwkProvider> createProvider;
    private final ConcurrentHashMap<String, JwkProvider> cachedJwkProviders = new ConcurrentHashMap<>();

    private final Supplier<String> clusterId;

    private final Settings settings;

    public JWTAuthenticationMethod(Roles roles, Settings settings, Supplier<String> clusterId) {
        this(roles, settings, clusterId, CachingJwkProvider::new);
    }

    @VisibleForTesting
    JWTAuthenticationMethod(Roles roles,
                            Settings settings,
                            Supplier<String> clusterId,
                            Function<String, JwkProvider> createProvider) {
        this.roles = roles;
        this.settings = settings;
        this.clusterId = clusterId;
        this.createProvider = createProvider;
    }

    @Override
    public @Nullable Role authenticate(Credentials credentials, ConnectionProperties connProperties) {
        var username = credentials.username();
        assert username != null : "User name must be resolved before authentication attempt";
        var decodedJWT = credentials.decodedToken();
        assert decodedJWT != null : "Token must be not null on jwt auth";

        Role user = roles.findUser(username);
        if (user == null) {
            throw new RuntimeException("jwt authentication failed for user \"" + username + "\"");
        }

        try {
            String issuer = settings.get(AUTH_HOST_BASED_JWT_ISS_SETTING.getKey());
            String audience = settings.get(AUTH_HOST_BASED_JWT_AUD_SETTING.getKey(), clusterId.get());
            String name;
            if (issuer != null) {
                // Ignore user specific configs if default 'iss' is defined.
                name = username; // Token dictates CrateDB user.
            } else {
                var jwtProperties = user.jwtProperties();
                assert jwtProperties != null : "credentials.username was matched using jwt properties, properties cannot be null.";
                issuer = jwtProperties.iss();
                name = jwtProperties.username();
                audience = jwtProperties.aud() != null ? jwtProperties.aud() : audience;
            }
            JwkProvider jwkProvider = cachedJwkProviders.computeIfAbsent(issuer, this.createProvider::apply);
            // Expiration date is checked by default(if provided in token)
            Algorithm algorithm = resolveAlgorithm(jwkProvider, decodedJWT);
            Verification verification = JWT.require(algorithm)
                // withers below are not needed for payload signature check.
                // It's an extra step on top of signature verification to double check
                // that user metadata (or default values) match token payload.
                .withIssuer(issuer)
                .withClaim("username", name)
                .withAudience(audience);

            JWTVerifier verifier = verification.build();
            verifier.verify(decodedJWT);

        } catch (Exception e) {
            throw new RuntimeException(
                String.format(
                    Locale.ENGLISH,
                    "jwt authentication failed for user %s. Reason: %s",
                    username,
                    e.getMessage()
                )
            );
        }

        return user;
    }

    @Override
    public String name() {
        return NAME;
    }

    /**
     * Resolved algorithm based on the endpoint info.
     * Cannot resolve algorithm based on the token header as it's optional and unsafe.
     * If token has "alg" claim, it's checked against JWK info and in case of mismatch, error is thrown.
     * If JWK info doesn't have "alg" (it's optional for endpoint response as well), we fall back to RSA256.
     */
    private static Algorithm resolveAlgorithm(JwkProvider jwkProvider,
                                              @NotNull DecodedJWT decodedJWT) throws JwkException {
        Jwk jwk = jwkProvider.get(decodedJWT.getKeyId());
        PublicKey publicKey = jwk.getPublicKey();
        if (publicKey instanceof RSAPublicKey == false) {
            throw new UnsupportedOperationException("Only RSA algorithm is supported for JWT");
        }
        RSAPublicKey rsaPublicKey = (RSAPublicKey) publicKey;

        // Default.
        if (jwk.getAlgorithm() == null) {
            return Algorithm.RSA256(new LoadedRSAKeyProvider(rsaPublicKey));
        }

        // Try to validate resolved info with token (if both JWK and token are not null).
        if (decodedJWT.getAlgorithm() != null && decodedJWT.equals(jwk.getAlgorithm()) == false) {
            throw new IllegalArgumentException("Jwt token has algorithm not matching with the algorithm of the public key.");
        }

        return switch (jwk.getAlgorithm()) {
            case "RS256" -> Algorithm.RSA256(new LoadedRSAKeyProvider(rsaPublicKey));
            case "RS384" -> Algorithm.RSA384(new LoadedRSAKeyProvider(rsaPublicKey));
            case "RS512" -> Algorithm.RSA512(new LoadedRSAKeyProvider(rsaPublicKey));
            default ->
                throw new RuntimeException(
                    String.format(
                        Locale.ENGLISH,
                        "Unsupported algorithm %s",
                        jwk.getAlgorithm()
                    )
                );
        };

    }
}
