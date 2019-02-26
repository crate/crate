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
import org.apache.logging.log4j.Logger;
import io.crate.es.common.io.PathUtils;
import org.apache.logging.log4j.LogManager;
import io.crate.es.common.network.InetAddresses;
import io.crate.es.common.network.NetworkService;
import io.crate.es.common.settings.Settings;
import io.crate.es.common.util.BigArrays;
import io.crate.es.common.util.concurrent.ThreadContext;
import io.crate.es.common.xcontent.NamedXContentRegistry;
import io.crate.es.http.HttpServerTransport;
import io.crate.es.http.netty4.Netty4HttpServerTransport;
import io.crate.es.http.netty4.cors.Netty4CorsHandler;
import io.crate.es.rest.RestChannel;
import io.crate.es.rest.RestRequest;
import io.crate.es.threadpool.ThreadPool;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Path;

import static io.crate.es.env.Environment.PATH_HOME_SETTING;
import static io.crate.es.http.HttpTransportSettings.SETTING_CORS_ENABLED;


public class CrateNettyHttpServerTransport extends Netty4HttpServerTransport {

    private final PipelineRegistry pipelineRegistry;

    public CrateNettyHttpServerTransport(Settings settings,
                                         NetworkService networkService,
                                         BigArrays bigArrays,
                                         ThreadPool threadPool,
                                         NamedXContentRegistry namedXContentRegistry,
                                         Dispatcher dispatcher,
                                         PipelineRegistry pipelineRegistry) {
        super(settings, networkService, bigArrays, threadPool, namedXContentRegistry,
            new CrateDispatcher(settings, dispatcher));
        this.pipelineRegistry = pipelineRegistry;
    }

    @Override
    public HttpChannelHandler configureServerChannelHandler() {
        return new CrateHttpChannelHandler(this, detailedErrorsEnabled, threadPool);
    }

    protected class CrateHttpChannelHandler extends HttpChannelHandler {

        private final CrateNettyHttpServerTransport transport;

        CrateHttpChannelHandler(CrateNettyHttpServerTransport transport,
                                boolean detailedErrorsEnabled,
                                ThreadPool threadPool) {
            super(transport, detailedErrorsEnabled, threadPool.getThreadContext());
            this.transport = transport;
        }

        @Override
        protected void initChannel(Channel ch) throws Exception {
            super.initChannel(ch);
            ChannelPipeline pipeline = ch.pipeline();
            pipelineRegistry.registerItems(pipeline, transport.getCorsConfig());
            // re-arrange cors so that it is utilized before the auth handler.
            // (Options pre-flight requests shouldn't require auth)
            if (SETTING_CORS_ENABLED.get(transport.settings())) {
                pipeline.remove("cors");
                pipeline.addAfter("encoder", "cors", new Netty4CorsHandler(transport.getCorsConfig()));
            }
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

    private static class CrateDispatcher implements HttpServerTransport.Dispatcher {

        private static final Logger LOG = LogManager.getLogger(CrateDispatcher.class);

        private final Path sitePath;
        private final Dispatcher fallbackDispatcher;

        CrateDispatcher(Settings settings, Dispatcher fallbackDispatcher) {
            this.sitePath = PathUtils.get(PATH_HOME_SETTING.get(settings)).normalize().resolve("lib").resolve("site");
            this.fallbackDispatcher = fallbackDispatcher;
        }

        @Override
        public void dispatchRequest(RestRequest request, RestChannel channel, ThreadContext threadContext) {
            if (request.rawPath().startsWith("/static")) {
                try {
                    StaticSite.serveSite(sitePath, request, channel);
                } catch (IOException e) {
                    LOG.error("Couldn't serve static file", e);
                    fallbackDispatcher.dispatchBadRequest(request, channel, threadContext, e);
                }
            } else {
                fallbackDispatcher.dispatchRequest(request, channel, threadContext);
            }
        }

        @Override
        public void dispatchBadRequest(RestRequest request, RestChannel channel, ThreadContext threadContext, Throwable cause) {
            fallbackDispatcher.dispatchBadRequest(request, channel, threadContext, cause);
        }
    }
}
