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

package io.crate.http;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.HttpContentCompressor;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.util.ReferenceCountUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class HttpTestServer {

    private final int port;
    private final boolean fail;
    private final static JsonFactory jsonFactory;

    private Channel channel;
    private NioEventLoopGroup group;

    public List<String> responses = new ArrayList<>();

    static {
        jsonFactory = new JsonFactory();
        jsonFactory.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
        jsonFactory.configure(JsonGenerator.Feature.QUOTE_FIELD_NAMES, true);
        jsonFactory.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
    }



    /**
     * @param port the port to listen on
     * @param fail of set to true, the server will emit error responses
     */
    public HttpTestServer(int port, boolean fail) {
        this.port = port;
        this.fail = fail;
    }

    public void run() throws InterruptedException {
        // Configure the server.
        ServerBootstrap bootstrap = new ServerBootstrap();
        group = new NioEventLoopGroup();
        bootstrap.group(group);
        bootstrap.channel(NioServerSocketChannel.class);
        bootstrap.childHandler(new ChannelInitializer<Channel>() {

            @Override
            protected void initChannel(Channel ch) {
                ChannelPipeline pipeline = ch.pipeline();
                pipeline.addLast("decoder", new HttpRequestDecoder());
                pipeline.addLast("encoder", new HttpResponseEncoder());
                pipeline.addLast("deflater", new HttpContentCompressor());
                pipeline.addLast("handler", new HttpTestServerHandler());
            }
        });

        // Bind and start to accept incoming connections.
        channel = bootstrap.bind(new InetSocketAddress(port)).sync().channel();
    }

    public void shutDown() {
        channel.close().awaitUninterruptibly();
        if (group != null) {
            group.shutdownGracefully().awaitUninterruptibly();
            group.terminationFuture().awaitUninterruptibly();
            group = null;
        }
    }

    @ChannelHandler.Sharable
    public class HttpTestServerHandler extends SimpleChannelInboundHandler<Object> {

        private final Logger logger = LogManager.getLogger(HttpTestServerHandler.class.getName());

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
            if (!(msg instanceof HttpRequest)) {
                ctx.fireChannelRead(msg);
                return;
            }
            try {
                handleHttpRequest(ctx, (HttpRequest) msg);
            } finally {
                ReferenceCountUtil.release(msg);
            }
        }

        private void handleHttpRequest(ChannelHandlerContext ctx, HttpRequest msg) throws UnsupportedEncodingException {
            String uri = msg.uri();
            QueryStringDecoder decoder = new QueryStringDecoder(uri);
            logger.debug("Got Request for " + uri);
            HttpResponseStatus status = fail ? HttpResponseStatus.BAD_REQUEST : HttpResponseStatus.OK;

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            try {
                JsonGenerator generator = jsonFactory.createGenerator(out, JsonEncoding.UTF8);
                generator.writeStartObject();
                for (Map.Entry<String, List<String>> entry : decoder.parameters().entrySet()) {
                    if (entry.getValue().size() == 1) {
                        generator.writeStringField(entry.getKey(), URLDecoder.decode(entry.getValue().get(0), "UTF-8"));
                    } else {
                        generator.writeArrayFieldStart(entry.getKey());
                        for (String value : entry.getValue()) {
                            generator.writeString(URLDecoder.decode(value, "UTF-8"));
                        }
                        generator.writeEndArray();
                    }
                }
                generator.writeEndObject();
                generator.close();

            } catch (Exception ex) {
                status = HttpResponseStatus.INTERNAL_SERVER_ERROR;
            }
            ByteBuf byteBuf = Unpooled.wrappedBuffer(out.toByteArray());
            responses.add(out.toString(StandardCharsets.UTF_8.name()));

            DefaultFullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status, byteBuf);
            ChannelFuture future = ctx.channel().writeAndFlush(response);
            future.addListener(ChannelFutureListener.CLOSE);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            logger.warn("Unexpected exception from downstream.", cause);
            ctx.close();
        }
    }
}
