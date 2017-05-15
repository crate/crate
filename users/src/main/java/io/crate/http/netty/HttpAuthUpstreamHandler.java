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

package io.crate.http.netty;

import com.google.common.annotations.VisibleForTesting;
import io.crate.operation.auth.*;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.http.*;
import org.jboss.netty.util.CharsetUtil;

import javax.annotation.Nullable;
import java.net.InetAddress;
import java.util.Locale;

import static org.jboss.netty.buffer.ChannelBuffers.*;

public class HttpAuthUpstreamHandler extends SimpleChannelUpstreamHandler {

    private static final Logger LOGGER = Loggers.getLogger(HttpAuthUpstreamHandler.class);
    private final Authentication authService;
    private Settings settings;
    private boolean authorized;

    public HttpAuthUpstreamHandler(Settings settings, Authentication authService) {
        super();
        this.settings = settings;
        this.authService = authService;
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        Object msg = e.getMessage();
        if (msg instanceof HttpRequest) {
            handleHttpRequest(ctx, e, (HttpRequest) msg);
        } else if (msg instanceof HttpChunk) {
            handleHttpChunk(ctx, e);
        } else {
            // neither http request nor http chunk - send upstream and see ...
            ctx.sendUpstream(e);
        }
    }

    private void handleHttpRequest(ChannelHandlerContext ctx, MessageEvent e, HttpRequest request) {
        String username = userFromRequest(request);
        InetAddress address = CrateNettyHttpServerTransport.getRemoteAddress(ctx.getChannel());
        AuthenticationMethod authMethod = authService.resolveAuthenticationType(username, address, HbaProtocol.HTTP);
        if (authMethod == null) {
            String errorMessage = String.format(
                Locale.ENGLISH,
                "No valid auth.host_based.config entry found for host \"%s\", user \"%s\", protocol \"%s\"",
                address.getHostAddress(), username, HbaProtocol.HTTP.toString());
            sendUnauthorized(ctx.getChannel(), errorMessage);
        } else {
            authMethod.httpAuthentication(username)
                .whenComplete((success, throwable) -> {
                    if (success) {
                        authorized = true;
                        if (LOGGER.isTraceEnabled()) {
                            LOGGER.trace("Authentication succeeded user \"{}\" and method \"{}\".",
                                username, authMethod.name());
                        }
                        ctx.sendUpstream(e);
                    } else {
                        String message = String.format(
                            Locale.ENGLISH,
                            "Authentication failed for user \"%s\" and method \"%s\".",
                            username, authMethod.name()
                        );
                        LOGGER.warn(message);
                        sendUnauthorized(ctx.getChannel(), message);
                    }
                });
        }
    }

    private void handleHttpChunk(ChannelHandlerContext ctx, MessageEvent e) {
        if (authorized) {
            ctx.sendUpstream(e);
        } else {
            sendUnauthorized(ctx.getChannel(), null);
        }
    }

    @VisibleForTesting
    static void sendUnauthorized(Channel channel, @Nullable String body) {
        DefaultHttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.UNAUTHORIZED);
        if (body != null) {
            if (!body.endsWith("\n")) {
                body += "\n";
            }
            HttpHeaders.setContentLength(response, body.length());
            response.setContent(copiedBuffer(body, CharsetUtil.UTF_8));
        }
        channel.write(response).addListener(ChannelFutureListener.CLOSE);
    }

    @VisibleForTesting
    boolean authorized() {
        return authorized;
    }

    private String userFromRequest(HttpRequest request) {
        if (request.headers().contains(AuthenticationProvider.HTTP_HEADER_USER)) {
            return request.headers().get(AuthenticationProvider.HTTP_HEADER_USER);
        } else {
            return AuthenticationProvider.AUTH_HOST_BASED_HTTP_DEFAULT_USER_SETTING.setting().get(settings);
        }
    }

}

