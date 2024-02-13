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
import java.util.function.BiFunction;

import org.elasticsearch.common.settings.SecureString;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import com.auth0.jwt.JWT;
import com.auth0.jwt.interfaces.DecodedJWT;

import io.crate.role.Role;

/**
 * Holder for all authentication methods.
 * username is CrateDB user.
 * password is used only for password method and null otherwise.
 * jwtToken is used only for jwt method and null otherwise.
 */
public class Credentials implements Closeable {

    // Reusable, as internally uses reusable ObjectMapper.
    private static final JWT JWT = new JWT();

    private final String username;

    // Non-final as Postgres protocol might inject password later after creation.
    private SecureString password;

    private final String jwtToken;

    /**
     * @param roleLookup is used to resolve CrateDB user from JWT token.
     */
    private Credentials(@Nullable String username,
                        @Nullable char[] password,
                        @Nullable String jwtToken,
                        @Nullable BiFunction<String, String, Role> roleLookup) {
        this.password = password != null ? new SecureString(password) : null;
        this.jwtToken = jwtToken;
        if (username != null) {
            // Basic auth, username is provided or empty in case of null/empty header but not null.
            this.username = username;
        } else {
            assert jwtToken != null && roleLookup != null
                : "If user is not provided, it looked up using JWT token, token and lookup function must be not null.";
            DecodedJWT decodedJWT = JWT.decodeJwt(jwtToken);
            // validate(decodedJWT) TODO: Check that it contains all required fields, throw an error with hints otherwise.
            // kid must be there - for endpoint lookup, see getPublicKeyById(String kid) in JWTAuthenticationMethod
            Role role = roleLookup.apply(decodedJWT.getIssuer(), decodedJWT.getClaim("username").asString());
            if (role != null) {
                this.username = role.name();
            } else {
                this.username = null;
            }
        }
    }

    public static Credentials fromNameAndPassword(@NotNull String username, @Nullable char[] password) {
        return new Credentials(username, password, null, null);
    }

    public static Credentials fromToken(@NotNull String jwtToken, @NotNull BiFunction<String, String, Role> roleLookup) {
        return new Credentials(null, null, jwtToken, roleLookup);
    }

    /**
     * Only for PG protocol
     */
    public void setPassword(@NotNull char[] password) {
        this.password = new SecureString(password);
    }

    /**
     * @return one of:
     * - Empty string in case of empty/null auth header
     * - Basic header's username "as is"
     * - CrateDB user, resolved from JWT token
     */
    @NotNull
    public String username() {
        return username;
    }

    @Nullable
    public SecureString password() {
        return password;
    }

    @Nullable
    public String jwtToken() {
        return jwtToken;
    }

    @Override
    public void close() {
        if (password != null) {
            password.close();
        }
    }
}
