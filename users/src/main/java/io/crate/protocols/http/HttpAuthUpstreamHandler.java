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
import io.crate.operation.auth.Authentication;
import io.crate.operation.auth.AuthenticationMethod;
import io.crate.operation.auth.AuthenticationProvider;
import io.crate.operation.auth.Protocol;
import io.crate.operation.user.User;
import io.crate.protocols.postgres.ConnectionProperties;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.settings.Settings;

import javax.annotation.Nullable;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.Locale;

import static io.crate.protocols.SSL.getSession;
import static io.netty.buffer.Unpooled.copiedBuffer;


public class HttpAuthUpstreamHandler extends SimpleChannelInboundHandler<Object> {

    private static final Logger LOGGER = Loggers.getLogger(HttpAuthUpstreamHandler.class);
    private final Authentication authService;
    private Settings settings;
    private boolean authorized;

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
        String username = userFromRequest(request);
        InetAddress address = addressFromRequestOrChannel(request, ctx.channel());
        ConnectionProperties connectionProperties =
            new ConnectionProperties(address, Protocol.HTTP, getSession(ctx.channel()));
        AuthenticationMethod authMethod = authService.resolveAuthenticationType(username, connectionProperties);
        if (authMethod == null) {
            String errorMessage = String.format(
                Locale.ENGLISH,
                "No valid auth.host_based.config entry found for host \"%s\", user \"%s\", protocol \"%s\"",
                address.getHostAddress(), username, Protocol.HTTP.toString());
            sendUnauthorized(ctx.channel(), errorMessage);
        } else {
            try {
                User user = authMethod.authenticate(username, connectionProperties);
                if (user != null && LOGGER.isTraceEnabled()) {
                    LOGGER.trace("Authentication succeeded user \"{}\" and method \"{}\".", username, authMethod.name());
                }
                authorized = true;
                ctx.fireChannelRead(request);
            } catch (Exception e) {
                sendUnauthorized(ctx.channel(), e.getMessage());
            }
        }
    }

    private void handleHttpChunk(ChannelHandlerContext ctx, HttpContent msg) {
        if (authorized) {
            ctx.fireChannelRead(msg);
        } else {
            // We won't forward the message downstream, thus we have to release
            msg.release();
            sendUnauthorized(ctx.channel(), null);
        }
    }

    @VisibleForTesting
    static void sendUnauthorized(Channel channel, @Nullable String body) {
        LOGGER.warn(body == null ? "unauthorized http chunk" : body);
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
        channel.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
    }

    @VisibleForTesting
    boolean authorized() {
        return authorized;
    }

    private String userFromRequest(HttpRequest request) {
        if (request.headers().contains(AuthenticationProvider.HTTP_HEADER_USER)) {
            return request.headers().get(AuthenticationProvider.HTTP_HEADER_USER);
        } else {
            return AuthenticationProvider.AUTH_TRUST_HTTP_DEFAULT_HEADER.setting().get(settings);
        }
    }

    private InetAddress addressFromRequestOrChannel(HttpRequest request, Channel channel) {
        if (request.headers().contains(AuthenticationProvider.HTTP_HEADER_REAL_IP)) {
            return InetAddresses.forString(request.headers().get(AuthenticationProvider.HTTP_HEADER_REAL_IP));
        } else {
            return CrateNettyHttpServerTransport.getRemoteAddress(channel);
        }
    }
}

