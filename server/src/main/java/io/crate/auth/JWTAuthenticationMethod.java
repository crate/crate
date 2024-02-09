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

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.security.PublicKey;
import java.security.interfaces.RSAPublicKey;
import java.util.Locale;
import java.util.function.Function;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import com.auth0.jwk.Jwk;
import com.auth0.jwk.JwkException;
import com.auth0.jwk.JwkProvider;
import com.auth0.jwk.JwkProviderBuilder;
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

    private final Function<String, JwkProvider> urlToJwkProvider;


    public JWTAuthenticationMethod(Roles roles, Function<String, JwkProvider> urlToJwkProvider) {
        this.roles = roles;
        this.urlToJwkProvider = urlToJwkProvider;
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
            JwkProvider jwkProvider = urlToJwkProvider.apply(decodedJWT.getIssuer());
            Algorithm algorithm = resolveAlgorithm(jwkProvider, decodedJWT);

            var jwtProperties = user.jwtProperties();
            assert jwtProperties != null : "Resolved user must have jwt properties";
            // Expiration date is checked by default(if provided in token)
            Verification verification = JWT.require(algorithm)
                // withers below are not needed for payload signature check.
                // It's an extra step on top of signature verification to double check that user metadata matches token payload.
                .withIssuer(jwtProperties.iss())
                .withClaim("username", jwtProperties.username());

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


    public static JwkProvider jwkProvider(@NotNull String issuer) {
        URL jwkUrl;
        try {
            /*
             We cannot use JwkProviderBuilder constructor with String.
             It adds hard coded WELL_KNOWN_JWKS_PATH (/.well-known/jwks.json) to the url.
             JWK URL not necessarily ends with that suffix, for example:
             Microsoft: "jwks_uri":" https://login.microsoftonline.com/common/discovery/v2.0/keys"
             Google: "jwks_uri": "https://www.googleapis.com/oauth2/v3/certs",
             Taken from https://login.microsoftonline.com/common/v2.0/.well-known/openid-configuration
             and https://accounts.google.com/.well-known/openid-configuration correspondingly
            */

            URI uri = new URI(issuer).normalize();
            jwkUrl = uri.toURL();
        } catch (MalformedURLException | URISyntaxException e) {
            throw new IllegalArgumentException("Invalid jwks uri", e);
        }

        return new JwkProviderBuilder(jwkUrl).build();
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
