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

import java.io.Closeable;
import java.util.function.Predicate;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import com.auth0.jwt.JWT;
import com.auth0.jwt.interfaces.DecodedJWT;

import io.crate.role.Role;

/**
 * Holder for all authentication methods.
 * username is CrateDB user. Set up directly on password, cert and trust methods and resolved later on jwt.
 * password is used only for password method and null otherwise.
 * jwtToken is used only for jwt method and null otherwise.
 */
public class Credentials implements Closeable {

    // Reusable, as internally uses reusable ObjectMapper.
    private static final JWT JWT = new JWT();

    // Non-final as we set it up one we resolve CrateDB user.
    private String username;

    // Non-final as Postgres protocol might inject password later after creation.
    private SecureString password;

    private final DecodedJWT decodedToken;

    private Credentials(@Nullable String username, @Nullable char[] password, @Nullable String jwtToken) {
        this.username = username;
        this.password = password != null ? new SecureString(password) : null;
        if (jwtToken != null) {
            this.decodedToken = JWT.decodeJwt(jwtToken);
            validateToken(decodedToken);
        } else {
            this.decodedToken = null;
        }
    }

    public Credentials(String username, @Nullable char[] password) {
        this(username, password, null);
    }

    public Credentials(@NotNull String jwtToken) {
        this(null, null, jwtToken);
    }

    /**
     * Only for PG protocol
     */
    public void setPassword(@NotNull char[] password) {
        this.password = new SecureString(password);
    }

    /**
     * @param username is CrateDB username.
     * Resolved from iss/username of the JWT token.
     */
    public void setUsername(@NotNull String username) {
        this.username = username;
    }

    @Nullable
    public String username() {
        return username;
    }

    @Nullable
    public SecureString password() {
        return password;
    }

    @Nullable
    public DecodedJWT decodedToken() {
        return decodedToken;
    }

    @Override
    public void close() {
        if (password != null) {
            password.close();
        }
    }

    private static void validateToken(@NotNull DecodedJWT decodedToken) {
        if (Strings.isNullOrEmpty(decodedToken.getKeyId())) {
            // kid is optional: https://datatracker.ietf.org/doc/html/rfc7517#section-4.5
            // but for JWK it's required (JwkProvider uses it to fetch correct public key from JWK endpoint).
            throw new IllegalArgumentException("The JWT token must contain a public key id (kid)");
        }
        if (Strings.isNullOrEmpty(decodedToken.getIssuer())) {
            // Optional: https://www.rfc-editor.org/rfc/rfc7519#section-4.1.1
            // but required for CrateDB user lookup
            throw new IllegalArgumentException("The JWT token must contain an issuer (iss)");
        }
        if (Strings.isNullOrEmpty(decodedToken.getClaim("username").asString())) {
            // custom claim, required for CrateDB user lookup
            throw new IllegalArgumentException("The JWT token must contain a 'username' claim");
        }
    }

    /**
     * Matches only on jwt properites.
     * @return NULL if no lookup is needed (Basic auth).
     */
    @Nullable
    public Predicate<Role> jwtPropertyMatch() {
        if (decodedToken != null) {
            return role -> {
                var jwtProperties = role.jwtProperties();
                if (role.isUser() && jwtProperties != null) {
                    assert jwtProperties.iss() != null && jwtProperties.username() != null :
                        "If user has jwt properties, 'iss' and 'username' must be not null";
                    return jwtProperties.match(decodedToken.getIssuer(), decodedToken.getClaim("username").asString());
                }
                return false;
            };
        }
        return null;
    }
}
