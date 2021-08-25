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


package io.crate.test.utils;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.ReferenceCountUtil;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import java.net.InetSocketAddress;
import java.util.List;


public final class ConnectionTest {

    public enum ProbeResult {
        SSL_AVAILABLE,
        SSL_MISSING,
    }

    static class SslProbeHandler extends SslHandler  {

        public SslProbeHandler(SSLEngine engine) {
            super(engine);
        }

        private static final int SSL_RECORD_HEADER_LENGTH = 5;
        private ProbeResult probeAfterHandshake = null;

        @Override
        protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws SSLException {
            if (in.readableBytes() < SSL_RECORD_HEADER_LENGTH) {
                return;
            }

            if (SslHandler.isEncrypted(in)) {
                if (super.handshakeFuture().isSuccess()) {
                    probeAfterHandshake = ProbeResult.SSL_AVAILABLE;
                }
                super.decode(ctx, in, out);

            } else {
                if (super.handshakeFuture().isSuccess()) {
                    probeAfterHandshake = ProbeResult.SSL_MISSING;
                }
                in.clear();
                ctx.close();
            }
        }

        public ProbeResult getProbeAfterHandshake() {
            return probeAfterHandshake;
        }
    }

    /**
     * Checks whether connection was downgraded to plaintext after successfully establishing SSL connection.
     * Sends a valid ping message so that server responds back with the same ping.
     *
     * @return SSL_AVAILABLE if response was encrypted (server didn't downgrade to plaintext)
     *         SSL_MISSING if response was unencrypted (server did downgrade to plaintext).
     */
    public static ProbeResult probeAfterHandshake(SSLContext sslContext, InetSocketAddress address) throws InterruptedException {

        EventLoopGroup group = new NioEventLoopGroup();
        try {
            Bootstrap clientBootstrap = new Bootstrap();
            clientBootstrap.group(group);
            clientBootstrap.channel(NioSocketChannel.class);
            clientBootstrap.remoteAddress(new InetSocketAddress(address.getHostName(), address.getPort()));

            SslProbeHandler sslProbeHandler = new SslProbeHandler(sslContext.createSSLEngine());
            sslProbeHandler.engine().setUseClientMode(true);

            clientBootstrap.handler(new ChannelInitializer<>() {
                protected void initChannel(Channel channel) throws Exception {
                    channel.config().setOption(ChannelOption.CONNECT_TIMEOUT_MILLIS, 5000);
                    channel.pipeline().addLast(sslProbeHandler);
                    channel.pipeline().addLast(createPingHandler());
                }
            });

            ChannelFuture connectFuture = clientBootstrap.connect().sync();
            connectFuture.channel().closeFuture().sync();
            return sslProbeHandler.getProbeAfterHandshake();
        } catch(Exception ignored) {
            return ProbeResult.SSL_MISSING;
        }
        finally {
            group.shutdownGracefully().sync();
        }
    }

    public static ProbeResult probeSSL(SSLContext sslContext, InetSocketAddress address) {
        var socketFactory = sslContext.getSocketFactory();
        try (var socket = socketFactory.createSocket(address.getHostName(), address.getPort())) {
            // need to write something to trigger SSL handshake
            var out = socket.getOutputStream(); // Closed by outer try-with-resources.
            out.write(1);

            return ProbeResult.SSL_AVAILABLE;
        } catch (Exception ignored) {
            return ProbeResult.SSL_MISSING;
        }
    }

    private static ChannelInboundHandlerAdapter createPingHandler() {
        return new ChannelInboundHandlerAdapter() {

            @Override
            public void channelActive(ChannelHandlerContext ctx) {
                ByteBuf buf = Unpooled.buffer(6); // TcpHeader.MARKER_BYTES_SIZE + TcpHeader.MESSAGE_LENGTH_SIZE;
                buf.writeByte('E');
                buf.writeByte('S');
                buf.writeInt(-1);
                ctx.writeAndFlush(buf); // Write ES ping message as minimal imitation of normal client-server flow and get a response (ping) from the server.
            }

            @Override
            public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
                // This code triggers when SSL is not downgraded
                // and SslProbeHandler passes message to that handler by super.decode(ctx, in, out);
                // Closing channel so that closeFuture().sync() finishes
                ReferenceCountUtil.release(msg);
                ctx.channel().close();
            }
        };
    }
}
