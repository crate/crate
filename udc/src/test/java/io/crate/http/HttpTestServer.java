/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.*;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.logging.Loggers;

import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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
            protected void initChannel(Channel ch) throws Exception {
                ChannelPipeline pipeline = ch.pipeline();
                // Uncomment the following line if you want HTTPS
                //SSLEngine engine = SecureChatSslContextFactory.getServerContext().createSSLEngine();
                //engine.setUseClientMode(false);
                //pipeline.addLast("ssl", new SslHandler(engine));

                pipeline.addLast("decoder", new HttpRequestDecoder());
                // Uncomment the following line if you don't want to handle HttpChunks.
                //pipeline.addLast("aggregator", new HttpChunkAggregator(1048576));
                pipeline.addLast("encoder", new HttpResponseEncoder());
                // Remove the following line if you don't want automatic content compression.
                pipeline.addLast("deflater", new HttpContentCompressor());
                pipeline.addLast("handler", new HttpTestServerHandler());
            }
        });

        // Bind and start to accept incoming connections.
        channel = bootstrap.bind(new InetSocketAddress(port)).sync().channel();
    }

    public void shutDown() throws InterruptedException, ExecutionException, TimeoutException {
        channel.close();
        if (group != null) {
            group.shutdownGracefully().get(10, TimeUnit.SECONDS);
        }
    }

    public static void main(String[] args) throws Exception {
        int port;
        if (args.length > 0) {
            port = Integer.parseInt(args[0]);
        } else {
            port = 8080;
        }
        new HttpTestServer(port, false).run();
    }

    @ChannelHandler.Sharable
    public class HttpTestServerHandler extends SimpleChannelInboundHandler<Object> {

        private final Logger logger = Loggers.getLogger(HttpTestServerHandler.class.getName());

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
            if (msg instanceof HttpRequest) {
                HttpRequest request = (HttpRequest) msg;
                String uri = request.uri();
                QueryStringDecoder decoder = new QueryStringDecoder(uri);
                logger.debug("Got Request for " + uri);
                HttpResponse response;

                if (fail) {
                    response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.BAD_REQUEST);
                } else {
                    response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
                }
                BytesStreamOutput out = new BytesStreamOutput();
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
                    response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.INTERNAL_SERVER_ERROR);
                }
                responses.add(out.bytes().utf8ToString());
                if (logger.isDebugEnabled()) {
                    logger.debug("Sending response: " + out.bytes().utf8ToString());
                }

                ChannelFuture future = ctx.channel().writeAndFlush(response);
                future.addListener(ChannelFutureListener.CLOSE);
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            logger.warn("Unexpected exception from downstream.", cause);
            ctx.channel().close();
        }
    }
}
