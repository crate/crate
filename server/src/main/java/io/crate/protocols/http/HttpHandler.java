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

package io.crate.protocols.http;

import static io.crate.auth.AuthSettings.AUTH_HOST_BASED_JWT_ISS_SETTING;

import java.util.function.Predicate;

import org.elasticsearch.common.settings.Settings;
import org.jspecify.annotations.Nullable;

import io.crate.auth.AuthSettings;
import io.crate.auth.Credentials;
import io.crate.auth.HttpAuthUpstreamHandler;
import io.crate.common.annotations.VisibleForTesting;
import io.crate.protocols.postgres.ConnectionProperties;
import io.crate.role.Role;
import io.crate.role.Roles;
import io.crate.session.Session;
import io.crate.session.Sessions;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMessage;

public abstract class HttpHandler<T> extends SimpleChannelInboundHandler<T> {

    private static final String REQUEST_HEADER_SCHEMA = "Default-Schema";

    private final Settings settings;
    private final Sessions sessions;
    private final Roles roles;
    private final boolean checkJwtProperties;

    @VisibleForTesting
    Session session;

    public HttpHandler(Settings settings,
                       Sessions sessions,
                       Roles roles) {
        super(false);
        this.settings = settings;
        this.sessions = sessions;
        this.roles = roles;
        this.checkJwtProperties = settings.get(AUTH_HOST_BASED_JWT_ISS_SETTING.getKey()) == null;
    }

    protected Roles roles() {
        return roles;
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        if (session != null) {
            session.close();
            session = null;
        }
        super.channelUnregistered(ctx);
    }

    @VisibleForTesting
    public Session ensureSession(ConnectionProperties connectionProperties, HttpMessage request) {
        String defaultSchema = request.headers().get(REQUEST_HEADER_SCHEMA);
        Role authenticatedUser = userFromAuthHeader(request.headers().get(HttpHeaderNames.AUTHORIZATION));
        Session session = this.session;
        if (session == null) {
            session = sessions.newSession(connectionProperties, defaultSchema, authenticatedUser);
        } else if (session.sessionSettings().authenticatedUser().equals(authenticatedUser) == false) {
            session.close();
            session = sessions.newSession(connectionProperties, defaultSchema, authenticatedUser);
        }
        this.session = session;
        return session;
    }

    /**
     * Doesn't do authentication as it's already done
     * in {@link HttpAuthUpstreamHandler} which is registered before this handler
     * Checks user existence and if not possible to resolve from header (basic or jwt),
     * returns trusted user from configuration.
     */
    @VisibleForTesting
    public Role userFromAuthHeader(@Nullable String authHeaderValue) {
        try (Credentials credentials = Headers.extractCredentialsFromHttpAuthHeader(authHeaderValue)) {
            Predicate<Role> rolePredicate = credentials.matchByToken(checkJwtProperties);
            if (rolePredicate != null) {
                Role role = roles.findUser(rolePredicate);
                if (role != null) {
                    credentials.setUsername(role.name());
                }
            }
            String username = credentials.username();
            // Fallback to trusted user from configuration
            if (username == null || username.isEmpty()) {
                username = AuthSettings.AUTH_TRUST_HTTP_DEFAULT_HEADER.get(settings);
            }
            return roles.findUser(username);
        }
    }
}
