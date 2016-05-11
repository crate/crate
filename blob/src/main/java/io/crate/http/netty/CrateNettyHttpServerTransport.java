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

package io.crate.http.netty;

import com.google.common.collect.ImmutableMap;
import io.crate.blob.BlobService;
import io.crate.blob.v2.BlobIndices;
import org.elasticsearch.cluster.node.DiscoveryNodeService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.http.netty.NettyHttpServerTransport;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;

import java.util.Map;

public class CrateNettyHttpServerTransport extends NettyHttpServerTransport {

    private final BlobService blobService;
    private final BlobIndices blobIndices;
    private final DiscoveryNodeService discoveryNodeService;

    @Inject
    public CrateNettyHttpServerTransport(Settings settings,
                                         NetworkService networkService,
                                         BigArrays bigArrays,
                                         BlobService blobService,
                                         BlobIndices blobIndices,
                                         DiscoveryNodeService discoveryNodeService) {
        super(settings, networkService, bigArrays);
        this.blobService = blobService;
        this.blobIndices = blobIndices;
        this.discoveryNodeService = discoveryNodeService;
    }

    @Override
    protected void doStart() {
        super.doStart();

        final String httpAddress = "http://" + boundAddress.publishAddress().getHost() + ":" + boundAddress.publishAddress().getPort();
        discoveryNodeService.addCustomAttributeProvider(new DiscoveryNodeService.CustomAttributesProvider() {
            @Override
            public Map<String, String> buildAttributes() {
                return ImmutableMap.<String, String>builder().put("http_address", httpAddress).build();
            }
        });
    }

    @Override
    public ChannelPipelineFactory configureServerChannelPipelineFactory() {
        return new CrateHttpChannelPipelineFactory(this, detailedErrorsEnabled);
    }

    private static class CrateHttpChannelPipelineFactory extends HttpChannelPipelineFactory {

        private final CrateNettyHttpServerTransport transport;

        public CrateHttpChannelPipelineFactory(CrateNettyHttpServerTransport transport, boolean detailedErrorsEnabled) {
            super(transport, detailedErrorsEnabled);
            this.transport = transport;
        }

        @Override
        public ChannelPipeline getPipeline() throws Exception {
            ChannelPipeline pipeline = super.getPipeline();

            HttpBlobHandler blobHandler = new HttpBlobHandler(transport.blobService, transport.blobIndices);
            pipeline.addBefore("aggregator", "blob_handler", blobHandler);
            return pipeline;
        }
    }
}
