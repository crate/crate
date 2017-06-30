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

package io.crate.plugin;

import io.crate.protocols.http.CrateNettyHttpServerTransport;
import io.crate.rest.CrateRestFilter;
import io.crate.rest.CrateRestMainAction;
import io.crate.rest.CrateRestModule;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.NetworkPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchRequestParsers;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.elasticsearch.common.network.NetworkModule.HTTP_TYPE_KEY;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_COMPRESSION;


public class HttpTransportPlugin extends Plugin implements NetworkPlugin, ActionPlugin {

    private static final String CRATE_HTTP_TRANSPORT_NAME = "crate";

    private final PipelineRegistry pipelineRegistry;
    private final Settings settings;

    public HttpTransportPlugin(Settings settings) {
        this.pipelineRegistry = new PipelineRegistry(settings);
        this.settings = settings;
    }

    public String name() {
        return "http";
    }

    public String description() {
        return "Plugin for extending HTTP transport";
    }

    @Override
    public Collection<Object> createComponents(Client client,
                                               ClusterService clusterService,
                                               ThreadPool threadPool,
                                               ResourceWatcherService resourceWatcherService,
                                               ScriptService scriptService,
                                               SearchRequestParsers searchRequestParsers) {
        // pipelineRegistry is returned here so that it's bound in guice and can be injected in other places
        return Collections.singletonList(pipelineRegistry);
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Collections.singletonList(CrateRestFilter.ES_API_ENABLED_SETTING);
    }

    @Override
    public Collection<Module> createGuiceModules() {
        return Collections.singletonList(new CrateRestModule(settings));
    }

    @Override
    public Settings additionalSettings() {
        return Settings.builder()
            .put(HTTP_TYPE_KEY, CRATE_HTTP_TRANSPORT_NAME)
            .put(SETTING_HTTP_COMPRESSION.getKey(), false)
            .build();
    }

    @Override
    public Map<String, Supplier<HttpServerTransport>> getHttpTransports(Settings settings,
                                                                        ThreadPool threadPool,
                                                                        BigArrays bigArrays,
                                                                        CircuitBreakerService circuitBreakerService,
                                                                        NamedWriteableRegistry namedWriteableRegistry,
                                                                        NetworkService networkService) {
        return Collections.singletonMap(
            CRATE_HTTP_TRANSPORT_NAME,
            () -> new CrateNettyHttpServerTransport(settings, networkService, bigArrays, threadPool, pipelineRegistry));
    }

    @Override
    public List<Class<? extends RestHandler>> getRestHandlers() {
        return Collections.singletonList(CrateRestMainAction.class);
    }
}
