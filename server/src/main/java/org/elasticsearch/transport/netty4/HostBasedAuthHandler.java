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

package org.elasticsearch.transport.netty4;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.security.sasl.AuthenticationException;

import io.crate.auth.AuthenticationMethod;
import io.crate.auth.ClientCertAuth;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.ssl.SslHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.network.CloseableChannel;
import org.elasticsearch.http.netty4.Netty4HttpServerTransport;

import io.crate.auth.Authentication;
import io.crate.auth.Protocol;
import io.crate.protocols.SSL;
import io.crate.protocols.postgres.ConnectionProperties;
import io.crate.user.User;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;

import static org.elasticsearch.transport.netty4.ClientStartTLSHandler.STARTTLS_MSG_LENGTH;
import static org.elasticsearch.transport.netty4.Netty4Transport.SERVER_SSL_HANDLER_NAME;

public class HostBasedAuthHandler extends ChannelInboundHandlerAdapter {

    private static final Logger LOGGER = LogManager.getLogger(HostBasedAuthHandler.class);

    private final Authentication authentication;
    private Exception authError;

    public HostBasedAuthHandler(Authentication authentication) {
        this.authentication = authentication;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (authError != null) {
            closeAndThrowException(ctx, msg, authError);
        }

        Channel channel = ctx.channel();
        InetAddress remoteAddress = Netty4HttpServerTransport.getRemoteAddress(channel);
        ConnectionProperties connectionProperties = new ConnectionProperties(
            remoteAddress,
            Protocol.TRANSPORT,
            SSL.getSession(channel)
        );
        String userName = User.CRATE_USER.name();
        var authMethod = authentication.resolveAuthenticationType(userName, connectionProperties);
        if (authMethod == null) {
            closeAndThrowException(ctx, msg, new AuthenticationException("No valid auth.host_based entry found for: " + remoteAddress));
        }
        try {
            authMethod.authenticate(userName, null, connectionProperties);
            ctx.pipeline().remove(this);

            if (authMethod.name().equals(ClientCertAuth.NAME) && ((ClientCertAuth) authMethod).isSwitchToPlaintext()) {
                // Presence of this handler implies that transport.mode != OFF and != LEGACY (and thus equal to the only other valid mode: ON)
                // which in turn implies that SSlHandler was added to the pipeline.
                // This handler comes after SSLHandler, so handshake already happened.
                // This handler is configured with startTls true so first message (STARTTLS response) goes unecrypted and client can react accordingly
                // switch_to_plaintext flag in combination with method cert indicates that it's safe to downgrade to plaintext.
                SslHandler sslHandler = (SslHandler) ctx.pipeline().get(SERVER_SSL_HANDLER_NAME);
                if (sslHandler != null) {
                        LOGGER.info("SSL switch to plaintext enabled, node {} switching from SSL to plaintext",
                            ((InetSocketAddress) channel.localAddress()).getHostName()
                        );

                        ByteBuf buf = Unpooled.buffer(STARTTLS_MSG_LENGTH);
                        buf.writeCharSequence("NOSTART!", StandardCharsets.UTF_8);
                        ctx.writeAndFlush(buf);


                } else {
                    closeAndThrowException(ctx, msg, new IllegalStateException("Auth method cert and switch_to_plaintext set " +
                        "but SSL in not configured for transport protocol on node: " + ((InetSocketAddress) channel.localAddress()).getHostName()));
                }
            } else {
                ByteBuf buf = Unpooled.buffer(STARTTLS_MSG_LENGTH);
                buf.writeCharSequence("NOSTART!", StandardCharsets.UTF_8);
                ctx.writeAndFlush(buf);
            }
        } catch (Exception e) {
            closeAndThrowException(ctx, msg, e);
        } finally {
            ReferenceCountUtil.release(msg);
        }
    }

    private void closeAndThrowException(ChannelHandlerContext ctx, Object msg, Exception e) throws Exception {
        authError = e;
        Netty4TcpChannel tcpChannel = ctx.channel().attr(Netty4Transport.CHANNEL_KEY).get();
        CloseableChannel.closeChannel(tcpChannel, true);
        throw authError;
    }
}
