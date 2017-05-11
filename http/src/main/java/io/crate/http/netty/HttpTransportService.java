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

import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.http.HttpServer;

import static org.elasticsearch.common.network.NetworkModule.HTTP_ENABLED;

public class HttpTransportService extends AbstractLifecycleComponent {

    private final Injector injector;

    @Inject
    public HttpTransportService(Settings settings, Injector injector) {
        super(settings);
        this.injector = injector;
    }

    @Override
    protected void doStart() {
        logger.info("Starting HTTP transport service ...");
        // by default the http server is started after the discovery service.
        // For the HttpTransportService this is too late.
        // The HttpServer has to be started before so that the boundAddress
        // can be added to DiscoveryNodes - this is required for the redirect logic.
        if (HTTP_ENABLED.get(settings)) {
            injector.getInstance(HttpServer.class).start();
        } else {
            logger.warn("HTTP must be enabled for the CrateDB /_sql endpoint and BLOB support to work.");
        }
    }

    @Override
    protected void doStop() {
        if (HTTP_ENABLED.get(settings)) {
            injector.getInstance(HttpServer.class).stop();
        }
    }

    @Override
    protected void doClose() {
        if (HTTP_ENABLED.get(settings)) {
            injector.getInstance(HttpServer.class).close();
        }
    }

}
