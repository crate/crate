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
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.util.concurrent.TimeUnit;

import org.jetbrains.annotations.Nullable;

import com.auth0.jwk.JwkException;
import com.auth0.jwk.JwkProvider;
import com.auth0.jwk.JwkProviderBuilder;
import com.auth0.jwt.JWT;
import com.auth0.jwt.JWTVerifier;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.interfaces.DecodedJWT;
import com.auth0.jwt.interfaces.RSAKeyProvider;

import io.crate.common.annotations.VisibleForTesting;
import io.crate.protocols.postgres.ConnectionProperties;
import io.crate.role.Role;
import io.crate.role.Roles;

public class JWTAuthenticationMethod implements AuthenticationMethod {

    public static final String NAME = "jwt";

    // Reusable, as internally uses reusable ObjectMapper.
    private static final JWT JWT = new JWT();
    private final Roles roles;

    JWTAuthenticationMethod(Roles roles) {
        this.roles = roles;
    }

    @Override
    public @Nullable Role authenticate(Credentials credentials, ConnectionProperties connProperties) {
        DecodedJWT decodedJWT = null;
        try {
            // TODO: Maybe we can store token as DecodedJWT in Credentials to avoid double decoding from String?
            decodedJWT = JWT.decodeJwt(credentials.jwtToken());

            JwkProvider provider = new JwkProviderBuilder(buildJwkUrl(decodedJWT.getIssuer()))
                // up to 10 JWKs will be cached for up to 24 hours
                .cached(10, 24, TimeUnit.HOURS)
                // up to 10 JWKs can be retrieved within one minute
                .rateLimited(10, 1, TimeUnit.MINUTES)
                .build();
            RSAKeyProvider keyProvider = new RSAKeyProvider() {
                @Override
                public RSAPublicKey getPublicKeyById(String kid) {
                    try {
                        return (RSAPublicKey) provider.get(kid).getPublicKey();
                    } catch (JwkException e) {
                        throw new RuntimeException("jwt authentication failed for user \"" + credentials.username() + "\"", e);
                    }
                }

                @Override
                public RSAPrivateKey getPrivateKey() {
                    return null;
                }

                @Override
                public String getPrivateKeyId() {
                    return null;
                }
            };

            // TODO : Specify which algos we support and add add header validation to reject unsupported algos.
            Algorithm algorithm = Algorithm.RSA256(keyProvider);

            // TODO: Add more claims and expiration date to verifier.
            JWTVerifier verifier = JWT.require(algorithm).build();

            verifier.verify(decodedJWT);

        } catch (Exception e) {
            throw new RuntimeException("jwt authentication failed for user \"" + credentials.username() + "\"", e);
        }

        Role user = roles.findUser(credentials.username());
        if (user == null) {
            throw new RuntimeException("jwt authentication failed for user \"" + credentials.username() + "\"");
        }
        validateJwtProperties(user, decodedJWT);
        return user;
    }

    @VisibleForTesting
    static void validateJwtProperties(Role user, DecodedJWT decodedJWT) {

    }

    /**
     * We cannot use JwkProviderBuilder constructor with String.
     * It adds hard coded WELL_KNOWN_JWKS_PATH (/.well-known/jwks.json) to the url.
     * JWK URL not necessarily ends with that suffix, for example:
     * Microsoft: "jwks_uri":" https://login.microsoftonline.com/common/discovery/v2.0/keys"
     * Google: "jwks_uri": "https://www.googleapis.com/oauth2/v3/certs",
     * Taken from https://login.microsoftonline.com/common/v2.0/.well-known/openid-configuration
     * and https://accounts.google.com/.well-known/openid-configuration correspondingly.
     */
     @VisibleForTesting
     static URL buildJwkUrl(String url)  {
        final URI uri;
        try {
            uri = new URI(url).normalize();
            return uri.toURL();
        } catch (MalformedURLException | URISyntaxException e) {
            throw new IllegalArgumentException("Invalid jwks uri", e);
        }
    }

    @Override
    public String name() {
        return NAME;
    }
}
