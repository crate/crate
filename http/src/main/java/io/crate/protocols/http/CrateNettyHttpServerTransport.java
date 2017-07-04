/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.protocols.http;

import io.crate.plugin.PipelineRegistry;
import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.http.HttpServerAdapter;
import org.elasticsearch.http.netty4.Netty4HttpServerTransport;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Path;


public class CrateNettyHttpServerTransport extends Netty4HttpServerTransport {

    private final PipelineRegistry pipelineRegistry;
    private final Path siteDirectory;

    public CrateNettyHttpServerTransport(Settings settings,
                                         NetworkService networkService,
                                         BigArrays bigArrays,
                                         ThreadPool threadPool,
                                         NamedXContentRegistry namedXContentRegistry,
                                         PipelineRegistry pipelineRegistry) {
        super(settings, networkService, bigArrays, threadPool, namedXContentRegistry);
        this.siteDirectory = new Environment(settings).libFile().resolve("site");
        this.pipelineRegistry = pipelineRegistry;
    }

    @Override
    public void httpServerAdapter(HttpServerAdapter httpServerAdapter) {
        super.httpServerAdapter((request, channel, context) -> {
            if (request.rawPath().startsWith("/static")) {
                try {
                    StaticSite.serveSite(siteDirectory, request, channel);
                } catch (IOException e) {
                    logger.error("Couldn't serve static file", e);
                }
            } else {
                httpServerAdapter.dispatchRequest(request, channel, context);
            }
        });
    }

    @Override
    public HttpChannelHandler configureServerChannelHandler() {
        return new CrateHttpChannelHandler(this, detailedErrorsEnabled, threadPool);
    }

    protected class CrateHttpChannelHandler extends HttpChannelHandler {

        CrateHttpChannelHandler(CrateNettyHttpServerTransport transport,
                                boolean detailedErrorsEnabled,
                                ThreadPool threadPool) {
            super(transport, detailedErrorsEnabled, threadPool.getThreadContext());
        }

        @Override
        protected void initChannel(Channel ch) throws Exception {
            super.initChannel(ch);
            ChannelPipeline pipeline = ch.pipeline();
            pipelineRegistry.registerItems(pipeline);
        }
    }

    public static InetAddress getRemoteAddress(Channel channel) {
        if (channel.remoteAddress() instanceof InetSocketAddress) {
            return ((InetSocketAddress) channel.remoteAddress()).getAddress();
        }
        // In certain cases the channel is an EmbeddedChannel (e.g. in tests)
        // and this type of channel has an EmbeddedSocketAddress instance as remoteAddress
        // which does not have an address.
        // An embedded socket address is handled like a local connection via loopback.
        return InetAddresses.forString("127.0.0.1");
    }
}
