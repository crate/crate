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
package io.crate.es.transport;

import io.crate.es.common.io.stream.NamedWriteableRegistry;
import io.crate.es.common.network.NetworkService;
import io.crate.es.common.settings.Settings;
import io.crate.es.common.util.BigArrays;
import io.crate.es.common.util.PageCacheRecycler;
import io.crate.es.indices.breaker.CircuitBreakerService;
import io.crate.es.plugins.NetworkPlugin;
import io.crate.es.plugins.Plugin;
import io.crate.es.threadpool.ThreadPool;

import java.util.Collections;
import java.util.Map;
import java.util.function.Supplier;

public class MockTcpTransportPlugin extends Plugin implements NetworkPlugin {

    public static final String MOCK_TCP_TRANSPORT_NAME = "mock-socket-network";

    @Override
    public Map<String, Supplier<Transport>> getTransports(Settings settings, ThreadPool threadPool, BigArrays bigArrays,
                                                          PageCacheRecycler pageCacheRecycler,
                                                          CircuitBreakerService circuitBreakerService,
                                                          NamedWriteableRegistry namedWriteableRegistry,
                                                          NetworkService networkService) {
        return Collections.singletonMap(MOCK_TCP_TRANSPORT_NAME,
            () -> new MockTcpTransport(settings, threadPool, bigArrays, circuitBreakerService, namedWriteableRegistry, networkService));
    }
}
