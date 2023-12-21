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

import static io.crate.protocols.SSL.getSession;
import static io.netty.buffer.Unpooled.copiedBuffer;

import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.security.cert.Certificate;
import java.util.List;
import java.util.Locale;

import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.http.netty4.Netty4HttpServerTransport;
import org.jetbrains.annotations.Nullable;

import io.crate.common.annotations.VisibleForTesting;
import io.crate.common.collections.Tuple;
import io.crate.protocols.SSL;
import io.crate.protocols.http.Headers;
import io.crate.protocols.postgres.ConnectionProperties;
import io.crate.role.Role;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;


public class HttpAuthUpstreamHandler extends SimpleChannelInboundHandler<Object> {

    private static final Logger LOGGER = LogManager.getLogger(HttpAuthUpstreamHandler.class);
    @VisibleForTesting
    // realm-value should not contain any special characters
    static final String WWW_AUTHENTICATE_REALM_MESSAGE = "Basic realm=\"CrateDB Authenticator\"";

    private static List<String> REAL_IP_HEADER_BLACKLIST = List.of("127.0.0.1", "::1");

    private final Authentication authService;
    private final Settings settings;
    private String authorizedUser = null;

    public HttpAuthUpstreamHandler(Settings settings, Authentication authService) {
        // do not auto-release reference counted messages which are just in transit here
        super(false);
        this.settings = settings;
        this.authService = authService;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof HttpRequest) {
            handleHttpRequest(ctx, (HttpRequest) msg);
        } else if (msg instanceof HttpContent) {
            handleHttpChunk(ctx, ((HttpContent) msg));
        } else {
            // neither http request nor http chunk - send upstream and see ...
            ctx.fireChannelRead(msg);
        }
    }


    private void handleHttpRequest(ChannelHandlerContext ctx, HttpRequest request) {
        SSLSession session = getSession(ctx.channel());
        Tuple<String, SecureString> credentials = credentialsFromRequest(request, session, settings);
        String username = credentials.v1();
        SecureString password = credentials.v2();
        if (username.equals(authorizedUser)) {
            ctx.fireChannelRead(request);
            return;
        }

        InetAddress address = addressFromRequestOrChannel(request, ctx.channel());
        ConnectionProperties connectionProperties = new ConnectionProperties(address, Protocol.HTTP, session);
        AuthenticationMethod authMethod = authService.resolveAuthenticationType(username, connectionProperties);
        if (authMethod == null) {
            String errorMessage = String.format(
                Locale.ENGLISH,
                "No valid auth.host_based.config entry found for host \"%s\", user \"%s\", protocol \"%s\". Did you enable TLS in your client?",
                address.getHostAddress(), username, Protocol.HTTP.toString());
            sendUnauthorized(ctx.channel(), errorMessage);
        } else {
            try {
                Role user = authMethod.authenticate(username, password, connectionProperties);
                if (user != null && LOGGER.isTraceEnabled()) {
                    LOGGER.trace("Authentication succeeded user \"{}\" and method \"{}\".", username, authMethod.name());
                }
                authorizedUser = username;
                ctx.fireChannelRead(request);
            } catch (Exception e) {
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("{} authentication failed for user={} from connection={}",
                                authMethod.name(), username, connectionProperties.address());
                }
                sendUnauthorized(ctx.channel(), e.getMessage());
            }
        }
    }

    private void handleHttpChunk(ChannelHandlerContext ctx, HttpContent msg) {
        if (authorizedUser == null) {
            // We won't forward the message downstream, thus we have to release
            msg.release();
            sendUnauthorized(ctx.channel(), null);
        } else {
            ctx.fireChannelRead(msg);
        }
    }

    @VisibleForTesting
    static void sendUnauthorized(Channel channel, @Nullable String body) {
        HttpResponse response;
        if (body != null) {
            if (!body.endsWith("\n")) {
                body += "\n";
            }
            response = new DefaultFullHttpResponse(
                HttpVersion.HTTP_1_1, HttpResponseStatus.UNAUTHORIZED, copiedBuffer(body, StandardCharsets.UTF_8));
            HttpUtil.setContentLength(response, body.length());
        } else {
            response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.UNAUTHORIZED);
        }
        // "Tell" the browser to open the credentials popup
        // It helps to avoid custom login page in AdminUI
        response.headers().set(HttpHeaderNames.WWW_AUTHENTICATE, WWW_AUTHENTICATE_REALM_MESSAGE);
        channel.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
    }

    @VisibleForTesting
    boolean authorized() {
        return authorizedUser != null;
    }

    @VisibleForTesting
    static Tuple<String, SecureString> credentialsFromRequest(HttpRequest request, @Nullable SSLSession session, Settings settings) {
        String username = null;
        if (request.headers().contains(HttpHeaderNames.AUTHORIZATION.toString())) {
            // Prefer Http Basic Auth
            return Headers.extractCredentialsFromHttpBasicAuthHeader(
                request.headers().get(HttpHeaderNames.AUTHORIZATION.toString()));
        } else {
            // prefer commonName as userName over AUTH_TRUST_HTTP_DEFAULT_HEADER user
            if (session != null) {
                try {
                    Certificate certificate = session.getPeerCertificates()[0];
                    username = SSL.extractCN(certificate);
                } catch (ArrayIndexOutOfBoundsException | SSLPeerUnverifiedException ignored) {
                    // client cert is optional
                }
            }
            if (username == null) {
                username = AuthSettings.AUTH_TRUST_HTTP_DEFAULT_HEADER.get(settings);
            }
        }
        return new Tuple<>(username, null);
    }

    private InetAddress addressFromRequestOrChannel(HttpRequest request, Channel channel) {
        boolean supportXRealIp = AuthSettings.AUTH_TRUST_HTTP_SUPPORT_X_REAL_IP.get(settings);
        var realIP = request.headers().get(AuthSettings.HTTP_HEADER_REAL_IP);
        if (supportXRealIp && realIP != null && !REAL_IP_HEADER_BLACKLIST.contains(realIP)) {
            return InetAddresses.forString(realIP);
        } else {
            return Netty4HttpServerTransport.getRemoteAddress(channel);
        }
    }
}

