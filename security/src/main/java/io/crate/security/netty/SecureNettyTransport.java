/*
Copyright 2015 Hendrik Saly

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package io.crate.security.netty;

import org.elasticsearch.Version;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.netty.NettyTransport;
import org.jboss.netty.channel.ChannelPipeline;

public abstract class SecureNettyTransport extends NettyTransport {

    protected SecureNettyTransport(final Settings settings, final ThreadPool threadPool, final NetworkService networkService,
            final BigArrays bigArrays, final Version version) {
        super(settings, threadPool, networkService, bigArrays, version);
    }

    protected static class SecureServerChannelPipelineFactory extends ServerChannelPipelineFactory {

        protected final NettyTransport nettyTransport;
        protected static final ESLogger log = Loggers.getLogger(SecureServerChannelPipelineFactory.class);

        public SecureServerChannelPipelineFactory(final NettyTransport nettyTransport, final String name, final Settings settings) {
            super(nettyTransport, name, settings);
            this.nettyTransport = nettyTransport;
        }

        @Override
        public ChannelPipeline getPipeline() throws Exception {
            final ChannelPipeline pipeline = super.getPipeline();
            pipeline.replace("dispatcher", "dispatcher", new SecureMessageChannelHandler(nettyTransport, log));
            return pipeline;
        }
    }

    protected static class SecureClientChannelPipelineFactory extends ClientChannelPipelineFactory {

        protected final NettyTransport nettyTransport;
        protected static final ESLogger log = Loggers.getLogger(SecureClientChannelPipelineFactory.class);

        public SecureClientChannelPipelineFactory(final NettyTransport nettyTransport) {
            super(nettyTransport);
            this.nettyTransport = nettyTransport;
        }

        @Override
        public ChannelPipeline getPipeline() throws Exception {
            final ChannelPipeline pipeline = super.getPipeline();
            pipeline.replace("dispatcher", "dispatcher", new SecureMessageChannelHandler(nettyTransport, log));
            return pipeline;
        }

    }
}
