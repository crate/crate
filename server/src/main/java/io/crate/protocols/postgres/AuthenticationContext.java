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

package io.crate.protocols.postgres;

import java.io.Closeable;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.SecureString;
import org.jetbrains.annotations.Nullable;

import io.crate.auth.AuthenticationMethod;
import io.crate.auth.Credentials;
import io.crate.common.annotations.VisibleForTesting;
import io.crate.role.Role;

class AuthenticationContext implements Closeable {

    private final Credentials credentials;
    private final Logger logger;
    private final AuthenticationMethod authMethod;
    private final ConnectionProperties connProperties;

    /**
     * Create a context that holds information for authenticating a user using a certain authentication method.
     * The context instance is created after reading the startup body of the newly established connection.
     * The password is optional and can be provided as a char[] when the message handler of the protocol implementation
     * obtains the password from the client.
     *
     * @param authMethod The method that is used for authentication. {@link AuthenticationMethod}
     * @param connProperties Additional connection properties
     * @param credentials credentials of the user to authenticate.
     * @param logger The logger instance from {@link PostgresWireProtocol}
     */
    AuthenticationContext(AuthenticationMethod authMethod,
                          ConnectionProperties connProperties,
                          Credentials credentials,
                          Logger logger) {
        this.authMethod = authMethod;
        this.connProperties = connProperties;
        this.credentials = credentials;
        this.logger = logger;
    }

    @Nullable
    Role authenticate() {
        Role user = authMethod.authenticate(credentials, connProperties);
        if (user != null && logger.isTraceEnabled()) {
            logger.trace("Authentication succeeded user \"{}\" and method \"{}\".", user.name(), authMethod.name());
        }
        return user;
    }

    void setSecurePassword(char[] secureString) {
        credentials.setPassword(secureString);
    }

    @Nullable
    @VisibleForTesting
    SecureString password() {
        return credentials.password();
    }

    /**
     * Close method should be called as soon as possible in order to clear out the password char[].
     * Once close was called, {@link #authenticate()} would fail due to empty password.
     */
    @Override
    public void close() {
        if (credentials != null) {
            credentials.close();
        }
    }
}
