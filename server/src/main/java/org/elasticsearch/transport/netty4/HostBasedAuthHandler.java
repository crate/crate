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

import javax.security.sasl.AuthenticationException;

import org.elasticsearch.common.network.CloseableChannel;
import org.elasticsearch.http.netty4.Netty4HttpServerTransport;

import io.crate.auth.Authentication;
import io.crate.auth.Protocol;
import io.crate.protocols.SSL;
import io.crate.protocols.postgres.ConnectionProperties;
import io.crate.user.Role;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;

public class HostBasedAuthHandler extends ChannelInboundHandlerAdapter {

    private final Authentication authentication;
    private Exception authError;

    public HostBasedAuthHandler(Authentication authentication) {
        this.authentication = authentication;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (authError != null) {
            ReferenceCountUtil.release(msg);
            CloseableChannel tcpChannel = ctx.channel().attr(Netty4Transport.CHANNEL_KEY).get();
            CloseableChannel.closeChannel(tcpChannel, true);
            throw authError;
        }

        Channel channel = ctx.channel();
        InetAddress remoteAddress = Netty4HttpServerTransport.getRemoteAddress(channel);
        ConnectionProperties connectionProperties = new ConnectionProperties(
            remoteAddress,
            Protocol.TRANSPORT,
            SSL.getSession(channel)
        );
        String userName = Role.CRATE_USER.name();
        var authMethod = authentication.resolveAuthenticationType(userName, connectionProperties);
        if (authMethod == null) {
            ReferenceCountUtil.release(msg);
            authError = new AuthenticationException("No valid auth.host_based entry found for: " + remoteAddress);
            CloseableChannel tcpChannel = ctx.channel().attr(Netty4Transport.CHANNEL_KEY).get();
            CloseableChannel.closeChannel(tcpChannel, true);
            throw authError;
        }
        try {
            authMethod.authenticate(userName, null, connectionProperties);
            ctx.pipeline().remove(this);
            super.channelRead(ctx, msg);
        } catch (Exception e) {
            ReferenceCountUtil.release(msg);
            authError = e;
            CloseableChannel tcpChannel = ctx.channel().attr(Netty4Transport.CHANNEL_KEY).get();
            CloseableChannel.closeChannel(tcpChannel, true);
            throw e;
        }
    }
}
