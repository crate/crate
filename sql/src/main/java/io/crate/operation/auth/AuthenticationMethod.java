/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.operation.auth;

import io.crate.operation.user.User;
import io.netty.channel.Channel;

import java.util.concurrent.CompletableFuture;

/**
 * Common interface for Authentication methods.
 *
 * An auth method must provide a unique name which is exposed via the {@link #name()} method.
 *
 * It is also responsible for authentication for the Postgres Wire Protocol,
 * {@link #pgAuthenticate(Channel channel, String userName)},
 */
public interface AuthenticationMethod {
    /**
     * Authenticates the Postgres Wire Protocol client,
     * sends AuthenticationOK if authentication is successful
     * If authentication fails a throwable is passed to the future
     * @param channel request channel
     * @param userName the userName sent with the startup message
     * @return Future with authenticated user or null
     *
     */
    CompletableFuture<User> pgAuthenticate(Channel channel, String userName);

    /**
     * @return name of the authentication method
     */
    String name();

    CompletableFuture<User> httpAuthentication(String userName);
}
