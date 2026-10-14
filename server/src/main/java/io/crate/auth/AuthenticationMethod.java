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

import org.jspecify.annotations.Nullable;

import io.crate.protocols.postgres.ConnectionProperties;
import io.crate.role.Role;

public interface AuthenticationMethod {

    interface AuthToken {

        Role role();

        static AuthToken of(Role role) {
            return () -> role;
        }
    }

    /**
     * @param credentials contains username, password or token - depending on the used method.
     * @return token holding the user.
     * @throws RuntimeException if the authentication failed.
     */
    AuthToken authenticate(Credentials credentials, ConnectionProperties connProperties);

    /**
     * @return token as is if it's still valid or return null to enforce authentication.
     *
     * All implementations must check that incoming token is instance of the same class:
     * We can get previous token from a proxied connection, and it can have a different type.
     */
    @Nullable
    default AuthToken renew(AuthToken token, Credentials credentials) {
        return null;
    }

    /**
     * @return unique name of the authentication method
     */
    String name();
}
