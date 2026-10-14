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

import org.jspecify.annotations.Nullable;

import io.crate.common.annotations.VisibleForTesting;
import io.crate.protocols.postgres.ConnectionProperties;
import io.crate.role.Role;
import io.crate.role.Roles;
import io.crate.session.Session;
import io.crate.session.Sessions;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.HttpMessage;
import io.netty.handler.codec.http.HttpRequest;

public abstract class HttpHandler<T> extends SimpleChannelInboundHandler<T> {

    private static final String REQUEST_HEADER_SCHEMA = "Default-Schema";

    private final Sessions sessions;
    private final Roles roles;
    private final HttpAuthenticator authenticator;

    @VisibleForTesting
    Session session;

    public HttpHandler(Sessions sessions, Roles roles, HttpAuthenticator authenticator) {
        super(false);
        this.sessions = sessions;
        this.roles = roles;
        this.authenticator = authenticator;
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

    /**
     * On failure a {@code 401} has already been sent and {@code null} is returned;
     * the caller must stop processing and release the request.
     */
    @Nullable
    protected Role authenticate(ChannelHandlerContext ctx, HttpRequest request) {
        return authenticator.authenticate(request, ctx.channel());
    }

    @VisibleForTesting
    public Session ensureSession(Role authenticatedUser, ConnectionProperties connectionProperties, HttpMessage request) {
        String defaultSchema = request.headers().get(REQUEST_HEADER_SCHEMA);
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
}
