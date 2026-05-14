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

package io.crate.integrationtests;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jspecify.annotations.Nullable;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http3.DefaultHttp3DataFrame;
import io.netty.handler.codec.http3.DefaultHttp3Headers;
import io.netty.handler.codec.http3.DefaultHttp3HeadersFrame;
import io.netty.handler.codec.http3.Http3;
import io.netty.handler.codec.http3.Http3ClientConnectionHandler;
import io.netty.handler.codec.http3.Http3DataFrame;
import io.netty.handler.codec.http3.Http3HeadersFrame;
import io.netty.handler.codec.http3.Http3RequestStreamInboundHandler;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicSslContext;
import io.netty.handler.codec.quic.QuicSslContextBuilder;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;

/**
 * Minimal HTTP/3 test client built on the Netty QUIC codec for integration tests.
 */
public final class Http3TestClient implements AutoCloseable {

    private static final Logger LOGGER = LogManager.getLogger(Http3TestClient.class);
    private static final String TEST_AUTH = "Basic "
        + Base64.getEncoder().encodeToString("crate:passwd".getBytes(StandardCharsets.UTF_8));

    private final InetSocketAddress serverAddress;
    private final EventLoopGroup group;
    private final Channel udpChannel;
    private final QuicChannel quicChannel;

    public record Http3Response(int status, String body) {}

    public Http3TestClient(InetSocketAddress serverAddress) throws Exception {
        this.serverAddress = serverAddress;

        QuicSslContext clientSsl = QuicSslContextBuilder.forClient()
            .trustManager(InsecureTrustManagerFactory.INSTANCE)
            .applicationProtocols(Http3.supportedApplicationProtocols())
            .build();

        EventLoopGroup eventLoopGroup = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        Channel boundUdp = null;
        QuicChannel connectedQuic = null;
        try {
            boundUdp = new Bootstrap()
                .group(eventLoopGroup)
                .channel(NioDatagramChannel.class)
                .handler(Http3.newQuicClientCodecBuilder()
                    .sslContext(clientSsl)
                    .maxIdleTimeout(5_000, TimeUnit.MILLISECONDS)
                    .initialMaxData(10_000_000)
                    .initialMaxStreamDataBidirectionalLocal(1_000_000)
                    .build())
                .bind(0)
                .sync()
                .channel();

            connectedQuic = QuicChannel.newBootstrap(boundUdp)
                .handler(new ChannelInitializer<QuicChannel>() {
                    @Override
                    protected void initChannel(QuicChannel ch) {
                        ch.pipeline().addLast(new Http3ClientConnectionHandler());
                    }
                })
                .remoteAddress(serverAddress)
                .connect()
                .get(5, TimeUnit.SECONDS);

            this.group = eventLoopGroup;
            this.udpChannel = boundUdp;
            this.quicChannel = connectedQuic;
        } catch (Exception e) {
            release(connectedQuic, boundUdp, eventLoopGroup);
            throw e;
        }
    }

    public String post(String path, String body) throws Exception {
        return request("POST", path, body.getBytes(StandardCharsets.UTF_8), HttpHeaderValues.APPLICATION_JSON, false)
            .body();
    }

    public Http3Response put(String path, byte[] body) throws Exception {
        return request("PUT", path, body, HttpHeaderValues.APPLICATION_OCTET_STREAM, true);
    }

    public Http3Response get(String path) throws Exception {
        return request("GET", path, null, null, true);
    }

    private Http3Response request(String method,
                                  String path,
                                  @Nullable byte[] body,
                                  @Nullable CharSequence contentType,
                                  boolean withAuth) throws Exception {
        CompletableFuture<Http3Response> responseFuture = new CompletableFuture<>();
        AtomicInteger statusCode = new AtomicInteger();

        QuicStreamChannel stream = Http3.newRequestStream(quicChannel, new Http3RequestStreamInboundHandler() {

            final StringBuilder sb = new StringBuilder();

            @Override
            protected void channelRead(ChannelHandlerContext ctx, Http3HeadersFrame frame) {
                QuicChannel qc = (QuicChannel) ctx.channel().parent();
                String protocol = qc.sslEngine().getApplicationProtocol();
                CharSequence status = frame.headers().status();
                if (status != null) {
                    statusCode.set(Integer.parseInt(status.toString()));
                }
                LOGGER.info("Response received — protocol={}, :status={}", protocol, status);
            }

            @Override
            protected void channelRead(ChannelHandlerContext ctx, Http3DataFrame frame) {
                sb.append(frame.content().toString(StandardCharsets.UTF_8));
                frame.content().release();
            }

            @Override
            protected void channelInputClosed(ChannelHandlerContext ctx) {
                responseFuture.complete(new Http3Response(statusCode.get(), sb.toString()));
            }
        }).sync().getNow();

        var headers = new DefaultHttp3Headers()
            .method(method)
            .path(path)
            .scheme("https")
            .authority(serverAddress.getHostString() + ":" + serverAddress.getPort());
        if (withAuth) {
            headers.set(HttpHeaderNames.AUTHORIZATION, TEST_AUTH);
        }
        if (contentType != null) {
            headers.set(HttpHeaderNames.CONTENT_TYPE, contentType);
        }
        if (body != null) {
            headers.set(HttpHeaderNames.CONTENT_LENGTH, String.valueOf(body.length));
        }

        stream.write(new DefaultHttp3HeadersFrame(headers));
        if (body != null) {
            stream.writeAndFlush(new DefaultHttp3DataFrame(Unpooled.wrappedBuffer(body)));
        } else {
            stream.flush();
        }
        stream.shutdownOutput().sync();

        return responseFuture.get(10, TimeUnit.SECONDS);
    }

    @Override
    public void close() throws Exception {
        release(quicChannel, udpChannel, group);
    }

    private static void release(@Nullable QuicChannel quic,
                                @Nullable Channel udp,
                                @Nullable EventLoopGroup eventLoopGroup) {
        if (quic != null) {
            quic.close().syncUninterruptibly();
        }
        if (udp != null) {
            udp.close().syncUninterruptibly();
        }
        if (eventLoopGroup != null) {
            eventLoopGroup.shutdownGracefully(0, 1, TimeUnit.SECONDS).syncUninterruptibly();
        }
    }
}
