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
import org.elasticsearch.common.settings.SecureString;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class Credentials implements Closeable {

    private final String username;

    // Non-final as Postgres protocol might inject password later after creation.
    private SecureString passwordOrToken;

    public Credentials(@Nullable String username, @Nullable char[] passwordOrToken) {
        this.username = username;
        this.passwordOrToken = passwordOrToken != null ? new SecureString(passwordOrToken) : null;
    }

    /**
     * Only for PG protocol
     */
    public void setPassword(@NotNull char[] password) {
        this.passwordOrToken = new SecureString(password);
    }

    @Nullable
    public String username() {
        return username;
    }

    @Nullable
    public SecureString passwordOrToken() {
        return passwordOrToken;
    }

    @Override
    public void close() {
        if (passwordOrToken != null) {
            passwordOrToken.close();
        }
    }
}
