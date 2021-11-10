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


package io.crate.protocols.postgres;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.transport.RemoteConnectionParser;
import org.elasticsearch.transport.Transport.Connection;
import org.elasticsearch.transport.TransportSettings;

import io.crate.common.collections.BorrowedItem;
import io.crate.netty.EventLoopGroups;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

public class PgClient implements Closeable {

    private final Settings settings;
    private final EventLoopGroups eventLoopGroups;
    private final InetSocketAddress remoteAddress;

    private BorrowedItem<EventLoopGroup> eventLoopGroup;
    private Bootstrap bootstrap;
    private Channel channel;


    public PgClient(Settings nodeSettings,
                    EventLoopGroups eventLoopGroups,
                    String host) {
        this.settings = nodeSettings;
        this.eventLoopGroups = eventLoopGroups;
        this.remoteAddress = RemoteConnectionParser.parseConfiguredAddress(host);
    }

    public CompletableFuture<Connection> connect() {
        bootstrap = new Bootstrap();
        eventLoopGroup = eventLoopGroups.getEventLoopGroup(settings);
        bootstrap.group(eventLoopGroup.item());
        bootstrap.channel(eventLoopGroups.clientChannel());
        bootstrap.option(ChannelOption.TCP_NODELAY, TransportSettings.TCP_NO_DELAY.get(settings));
        bootstrap.option(ChannelOption.SO_KEEPALIVE, TransportSettings.TCP_KEEP_ALIVE.get(settings));

        CompletableFuture<Connection> result = new CompletableFuture<>();
        bootstrap.handler(new ClientChannelInitializer(result));
        bootstrap.remoteAddress(remoteAddress);
        ChannelFuture connect = bootstrap.connect();
        channel = connect.channel();

        ByteBuf buffer = channel.alloc().buffer();
        /// TODO: user must come from connectionInfo
        ClientMessages.writeStartupMessage(buffer, "doc", Map.of("user", "crate"));
        channel.writeAndFlush(buffer);
        return result;
    }

    @Override
    public void close() throws IOException {
        if (eventLoopGroup != null) {
            eventLoopGroup.close();
            eventLoopGroup = null;
        }
        if (bootstrap != null) {
            bootstrap = null;
        }
        if (channel != null) {
            channel.close().awaitUninterruptibly();
            channel = null;
        }
    }

    static class ClientChannelInitializer extends ChannelInitializer<Channel> {

        private final CompletableFuture<Connection> result;

        public ClientChannelInitializer(CompletableFuture<Connection> result) {
            this.result = result;
        }

        @Override
        protected void initChannel(Channel ch) throws Exception {
            ch.pipeline().addLast("decoder", new Decoder());
            ch.pipeline().addLast("dispatcher", new Handler(result));
        }
    }

    static class Handler extends SimpleChannelInboundHandler<ByteBuf> {

        private final CompletableFuture<Connection> result;

        public Handler(CompletableFuture<Connection> result) {
            this.result = result;
        }

        @Override
        public boolean acceptInboundMessage(Object msg) throws Exception {
            return true;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
            byte msgType = msg.readByte();
            int msgLength = msg.readInt() - 4; // exclude self

            switch (msgType) {
                // Authentication request
                case 'R' -> handleAuth(msg);
                default -> throw new IllegalStateException("Unexpected message type: " + msgType);
            }
        }

        private void handleAuth(ByteBuf msg) {
            AuthType authType = AuthType.of(msg.readInt());
            System.out.println(authType);
            switch (authType) {
                case Ok:
                    break;
                case CleartextPassword:
                    break;
                case KerberosV5:
                    break;
                case MD5Password:
                    break;
                default:
                    break;
            }
        }
    }

    enum AuthType {
        Ok,
        KerberosV5,
        CleartextPassword,
        MD5Password;

        public static AuthType of(int type) {
            return switch (type) {
                case 0 -> Ok;
                case 2 -> KerberosV5;
                case 3 -> CleartextPassword;
                case 5 -> MD5Password;
                default -> throw new IllegalArgumentException("Unknown auth type: " + type);
            };
        }
    }


    static class Decoder extends LengthFieldBasedFrameDecoder {

        // PostgreSQL wire protocol message format:
        // | Message Type (Byte1) | Length including self (Int32) | Body (depending on type) |
        private static final int LENGTH_FIELD_OFFSET = 1;
        private static final int LENGTH_FIELD_LENGTH = 4;
        private static final int LENGTH_ADJUSTMENT = -4;

        // keep the header
        private static final int INITIAL_BYTES_TO_STRIP = 0;

        public Decoder() {
            super(
                Integer.MAX_VALUE,
                LENGTH_FIELD_OFFSET,
                LENGTH_FIELD_LENGTH,
                LENGTH_ADJUSTMENT,
                INITIAL_BYTES_TO_STRIP
            );
        }
    }
}
