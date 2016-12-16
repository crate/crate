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

package io.crate.testing;

import io.crate.blob.BlobService;
import io.crate.blob.exceptions.RedirectService;
import io.crate.blob.v2.BlobIndicesService;
import io.crate.http.netty.CrateNettyHttpServerTransport;
import io.crate.plugin.BlobPlugin;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.http.HttpServerModule;
import org.jboss.netty.channel.ChannelPipelineFactory;

public class SslDummyPlugin extends BlobPlugin {

    public SslDummyPlugin(Settings settings) {
        super(settings);
    }

    @Override
    public String name() {
        return "ssl-dummy";
    }

    @Override
    public String description() {
        return "ssl-dummy plugin";
    }

    public void onModule(HttpServerModule module) {
        module.setHttpServerTransport(SslHttpServerTransport.class, "crate ssl");
    }

    public static class SslHttpServerTransport extends CrateNettyHttpServerTransport {

        @Inject
        public SslHttpServerTransport(Settings settings,
                                      NetworkService networkService,
                                      BigArrays bigArrays,
                                      BlobService blobService,
                                      RedirectService redirectService,
                                      BlobIndicesService blobIndicesService) {
            super(settings, networkService, bigArrays, blobService, redirectService, blobIndicesService);
        }

        @Override
        public ChannelPipelineFactory configureServerChannelPipelineFactory() {
            return new SslChannelPipelineFactory(this, redirectService, true, detailedErrorsEnabled);
        }

        public static class SslChannelPipelineFactory extends CrateNettyHttpServerTransport.CrateHttpChannelPipelineFactory {

            public SslChannelPipelineFactory(CrateNettyHttpServerTransport transport,
                                             RedirectService redirectService,
                                             boolean sslEnabled,
                                             boolean detailedErrorsEnabled) {
                super(transport, redirectService, sslEnabled, detailedErrorsEnabled);
            }
        }
    }
}
