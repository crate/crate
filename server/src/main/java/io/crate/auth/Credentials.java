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

import org.elasticsearch.common.settings.SecureString;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Holder for all authentication methods.
 * username is used for password, cert and trust methods and null otherwise.
 * password is used for password method and null otherwise.
 * jwtToken is used for jwt method and null otherwise.
 */
public class Credentials {

    private final String username;

    // Non-final as Postgres protocol might inject password later after creation.
    private SecureString password;

    private final String jwtToken;

    public static Credentials of(@NotNull String username, @Nullable SecureString password) {
        return new Credentials(username, password, null);
    }

    public static Credentials of(@NotNull String jwtToken) {
        return new Credentials(null, null, jwtToken);
    }

    private Credentials(@Nullable String username, @Nullable SecureString password, @Nullable String jwtToken) {
        this.username = username;
        this.password = password;
        this.jwtToken = jwtToken;
    }

    public void setPassword(@NotNull SecureString password) {
        this.password = password;
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
    public String jwtToken() {
        return jwtToken;
    }
}
