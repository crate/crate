/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.transport;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.http.netty4.Netty4HttpServerTransport;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.plugins.NetworkPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.netty4.Netty4Transport;
import org.elasticsearch.transport.netty4.Netty4Utils;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class Netty4Plugin extends Plugin implements NetworkPlugin {

    static {
        Netty4Utils.setup();
    }

    public static final String NETTY_TRANSPORT_NAME = "netty4";
    public static final String NETTY_HTTP_TRANSPORT_NAME = "netty4";

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(
            Netty4HttpServerTransport.SETTING_HTTP_NETTY_MAX_COMPOSITE_BUFFER_COMPONENTS,
            Netty4HttpServerTransport.SETTING_HTTP_WORKER_COUNT,
            Netty4HttpServerTransport.SETTING_HTTP_NETTY_RECEIVE_PREDICTOR_SIZE,
            Netty4HttpServerTransport.SETTING_HTTP_NETTY_RECEIVE_PREDICTOR_MIN,
            Netty4HttpServerTransport.SETTING_HTTP_NETTY_RECEIVE_PREDICTOR_MAX,
            Netty4Transport.WORKER_COUNT,
            Netty4Transport.NETTY_RECEIVE_PREDICTOR_SIZE,
            Netty4Transport.NETTY_RECEIVE_PREDICTOR_MIN,
            Netty4Transport.NETTY_RECEIVE_PREDICTOR_MAX,
            Netty4Transport.NETTY_BOSS_COUNT
        );
    }

    @Override
    public Settings additionalSettings() {
        return Settings.builder()
                // here we set the netty4 transport and http transport as the default. This is a set once setting
                // ie. if another plugin does that as well the server will fail - only one default network can exist!
                .put(NetworkModule.HTTP_DEFAULT_TYPE_SETTING.getKey(), NETTY_HTTP_TRANSPORT_NAME)
                .put(NetworkModule.TRANSPORT_DEFAULT_TYPE_SETTING.getKey(), NETTY_TRANSPORT_NAME)
                .build();
    }

    @Override
    public Map<String, Supplier<Transport>> getTransports(Settings settings, ThreadPool threadPool, BigArrays bigArrays,
                                                          PageCacheRecycler pageCacheRecycler,
                                                          CircuitBreakerService circuitBreakerService,
                                                          NamedWriteableRegistry namedWriteableRegistry,
                                                          NetworkService networkService) {
        return Collections.singletonMap(NETTY_TRANSPORT_NAME, () -> new Netty4Transport(settings, threadPool, networkService, bigArrays,
            namedWriteableRegistry, circuitBreakerService));
    }

    @Override
    public Map<String, Supplier<HttpServerTransport>> getHttpTransports(Settings settings, ThreadPool threadPool, BigArrays bigArrays,
                                                                        CircuitBreakerService circuitBreakerService,
                                                                        NamedWriteableRegistry namedWriteableRegistry,
                                                                        NamedXContentRegistry xContentRegistry,
                                                                        NetworkService networkService,
                                                                        HttpServerTransport.Dispatcher dispatcher) {
        return Collections.singletonMap(NETTY_HTTP_TRANSPORT_NAME,
            () -> new Netty4HttpServerTransport(settings, networkService, bigArrays, threadPool, xContentRegistry, dispatcher));
    }
}
