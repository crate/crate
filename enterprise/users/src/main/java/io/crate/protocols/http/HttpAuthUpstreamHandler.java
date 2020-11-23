/*
 * This file is part of a module with proprietary Enterprise Features.
 *
 * Licensed to Crate.io Inc. ("Crate.io") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 *
 * To use this file, Crate.io must have given you permission to enable and
 * use such Enterprise Features and you must have a valid Enterprise or
 * Subscription Agreement with Crate.io.  If you enable or use the Enterprise
 * Features, you represent and warrant that you have a valid Enterprise or
 * Subscription Agreement with Crate.io.  Your use of the Enterprise Features
 * if governed by the terms and conditions of your Enterprise or Subscription
 * Agreement with Crate.io.
 */

package io.crate.protocols.http;

import com.google.common.annotations.VisibleForTesting;
import io.crate.auth.AuthSettings;
import io.crate.auth.Authentication;
import io.crate.auth.AuthenticationMethod;
import io.crate.auth.Protocol;
import io.crate.auth.user.User;
import io.crate.protocols.SSL;
import io.crate.protocols.postgres.ConnectionProperties;
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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.crate.common.collections.Lists2;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.http.netty4.Netty4HttpServerTransport;

import javax.annotation.Nullable;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.security.cert.Certificate;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static io.crate.protocols.SSL.getSession;
import static io.netty.buffer.Unpooled.copiedBuffer;


public class HttpAuthUpstreamHandler extends SimpleChannelInboundHandler<Object> {

    private static final Logger LOGGER = LogManager.getLogger(HttpAuthUpstreamHandler.class);
    @VisibleForTesting

    // realm-value should not contain any special characters
    static final String WWW_AUTHENTICATE_REALM_MESSAGE = "Basic realm=\"CrateDB Authenticator\"";
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
        List<UserWithPassword> credentials = credentialsFromRequest(request, session, settings);
        for (var userWithPassword : credentials) {
            if (userWithPassword.userName().equals(authorizedUser)) {
                ctx.fireChannelRead(request);
                return;
            }
        }

        InetAddress address = addressFromRequestOrChannel(request, ctx.channel());
        ConnectionProperties connectionProperties = new ConnectionProperties(address, Protocol.HTTP, session);

        boolean foundAuthMethod = false;
        for (var userWithPassword : credentials) {
            String username = userWithPassword.userName();
            AuthenticationMethod authMethod = authService.resolveAuthenticationType(username, connectionProperties);
            if (authMethod == null) {
                continue;
            } else {
                foundAuthMethod = true;
                try {
                    User user = authMethod.authenticate(username, userWithPassword.password(), connectionProperties);
                    if (user != null && LOGGER.isTraceEnabled()) {
                        LOGGER.trace("Authentication succeeded user \"{}\" and method \"{}\".", username, authMethod.name());
                    }
                    authorizedUser = username;
                    ctx.fireChannelRead(request);
                    return;
                } catch (Exception e) {
                    continue;
                }
            }
        }
        if (foundAuthMethod) {
            String message = String.format(
                Locale.ENGLISH,
                "Authentication failed from connection=%s, attempted users=%s",
                connectionProperties.address(),
                Lists2.joinOn(", ", credentials, UserWithPassword::userName)
            );
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info(message);
            }
            sendUnauthorized(ctx.channel(), message);
        } else {
            String errorMessage = String.format(
                Locale.ENGLISH,
                "No valid auth.host_based.config entry found for host \"%s\", protocol \"%s\". Tried users: %s",
                address.getHostAddress(),
                Protocol.HTTP.toString(),
                Lists2.joinOn(", ", credentials, UserWithPassword::userName)
            );
            sendUnauthorized(ctx.channel(), errorMessage);
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
    static List<UserWithPassword> credentialsFromRequest(HttpRequest request, @Nullable SSLSession session, Settings settings) {
        ArrayList<UserWithPassword> result = new ArrayList<>();
        String authorizationHeader = HttpHeaderNames.AUTHORIZATION.toString();
        if (request.headers().contains(authorizationHeader)) {
            var userWithPassword = Headers.extractCredentialsFromHttpBasicAuthHeader(request.headers().get(authorizationHeader));
            if (userWithPassword != null) {
                result.add(userWithPassword);
            }
        }
        if (session != null) {
            try {
                Certificate certificate = session.getPeerCertificates()[0];
                result.add(new UserWithPassword(SSL.extractCN(certificate), null));
            } catch (ArrayIndexOutOfBoundsException | SSLPeerUnverifiedException ignored) {
                // client cert is optional
            }
        }
        if (result.isEmpty()) {
            result.add(new UserWithPassword(AuthSettings.AUTH_TRUST_HTTP_DEFAULT_HEADER.setting().get(settings), null));
        }
        return result;
    }

    private InetAddress addressFromRequestOrChannel(HttpRequest request, Channel channel) {
        if (request.headers().contains(AuthSettings.HTTP_HEADER_REAL_IP)) {
            return InetAddresses.forString(request.headers().get(AuthSettings.HTTP_HEADER_REAL_IP));
        } else {
            return Netty4HttpServerTransport.getRemoteAddress(channel);
        }
    }
}

