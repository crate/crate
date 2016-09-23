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
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.http.*;
import org.jboss.netty.util.CharsetUtil;

import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class HttpTestServer {

    private final int port;
    private final boolean fail;
    private final static JsonFactory jsonFactory;
    private Channel channel;
    public List<String> responses = new ArrayList<>();

    private final ChannelFactory channelFactory;

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
        this.channelFactory = new NioServerSocketChannelFactory();
    }

    public void run() {
        // Configure the server.
        ServerBootstrap bootstrap = new ServerBootstrap(
            this.channelFactory);

        // Set up the pipeline factory.
        bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
            public ChannelPipeline getPipeline() throws Exception {
                // Create a default pipeline implementation.
                ChannelPipeline pipeline = Channels.pipeline();

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
                return pipeline;
            }
        });

        // Bind and start to accept incoming connections.
        channel = bootstrap.bind(new InetSocketAddress(port));

    }

    public void shutDown() {
        try {
            channel.close().await();
            channelFactory.shutdown();
            channelFactory.releaseExternalResources();
        } catch (InterruptedException e) {
            e.printStackTrace();
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

    public class HttpTestServerHandler extends SimpleChannelUpstreamHandler {

        private final ESLogger logger = Loggers.getLogger(
            HttpTestServerHandler.class.getName());

        @Override
        public void messageReceived(
            ChannelHandlerContext ctx, MessageEvent e) {
            Object msg = e.getMessage();

            if (msg instanceof HttpRequest) {
                HttpRequest request = (HttpRequest) msg;
                String uri = request.getUri();
                QueryStringDecoder decoder = new QueryStringDecoder(uri);
                logger.debug("Got Request for " + uri);
                HttpResponse response;

                BytesStreamOutput out = new BytesStreamOutput();
                if (fail) {
                    response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.BAD_REQUEST);
                } else {
                    response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
                }
                try {
                    JsonGenerator generator = jsonFactory.createGenerator(out, JsonEncoding.UTF8);
                    generator.writeStartObject();
                    for (Map.Entry<String, List<String>> entry : decoder.getParameters().entrySet()) {
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
                response.setContent(ChannelBuffers.copiedBuffer(out.bytes().toUtf8(), CharsetUtil.UTF_8));

                responses.add(out.bytes().toUtf8());
                if (logger.isDebugEnabled()) {
                    logger.debug("Sending response: " + out.bytes().toUtf8());
                }

                ChannelFuture future = e.getChannel().write(response);
                future.addListener(ChannelFutureListener.CLOSE);
            }

        }

        @Override
        public void exceptionCaught(
            ChannelHandlerContext ctx, ExceptionEvent e) {
            // Close the connection when an exception is raised.
            logger.warn(
                "Unexpected exception from downstream.",
                e.getCause());
            e.getChannel().close();
        }
    }
}
