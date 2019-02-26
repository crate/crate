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
import io.crate.rest.CrateRestMainAction;
import io.crate.es.client.Client;
import io.crate.es.cluster.metadata.IndexNameExpressionResolver;
import io.crate.es.cluster.node.DiscoveryNodes;
import io.crate.es.cluster.service.ClusterService;
import io.crate.es.common.io.PathUtils;
import io.crate.es.common.io.stream.NamedWriteableRegistry;
import io.crate.es.common.network.NetworkService;
import io.crate.es.common.settings.ClusterSettings;
import io.crate.es.common.settings.IndexScopedSettings;
import io.crate.es.common.settings.Setting;
import io.crate.es.common.settings.Settings;
import io.crate.es.common.settings.SettingsFilter;
import io.crate.es.common.util.BigArrays;
import io.crate.es.common.util.concurrent.ThreadContext;
import io.crate.es.common.xcontent.NamedXContentRegistry;
import io.crate.es.env.Environment;
import io.crate.es.env.NodeEnvironment;
import io.crate.es.http.HttpServerTransport;
import io.crate.es.indices.breaker.CircuitBreakerService;
import io.crate.es.plugins.ActionPlugin;
import io.crate.es.plugins.NetworkPlugin;
import io.crate.es.plugins.Plugin;
import io.crate.es.rest.RestController;
import io.crate.es.rest.RestHandler;
import io.crate.es.threadpool.ThreadPool;

import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

import static io.crate.es.common.network.NetworkModule.HTTP_TYPE_KEY;
import static io.crate.es.env.Environment.PATH_HOME_SETTING;
import static io.crate.es.http.HttpTransportSettings.SETTING_HTTP_COMPRESSION;


public class HttpTransportPlugin extends Plugin implements NetworkPlugin, ActionPlugin {

    private static final String CRATE_HTTP_TRANSPORT_NAME = "crate";

    private final PipelineRegistry pipelineRegistry;
    private final Settings settings;

    public HttpTransportPlugin(Settings settings) {
        this.pipelineRegistry = new PipelineRegistry();
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
                                               NamedXContentRegistry xContentRegistry,
                                               Environment environment,
                                               NodeEnvironment nodeEnvironment,
                                               NamedWriteableRegistry namedWriteableRegistry) {
        // pipelineRegistry is returned here so that it's bound in guice and can be injected in other places
        return Collections.singletonList(pipelineRegistry);
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Collections.emptyList();
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
                                                                        NamedXContentRegistry xContentRegistry,
                                                                        NetworkService networkService,
                                                                        HttpServerTransport.Dispatcher dispatcher) {
        return Collections.singletonMap(
            CRATE_HTTP_TRANSPORT_NAME,
            () -> new CrateNettyHttpServerTransport(
                settings,
                networkService,
                bigArrays,
                threadPool,
                xContentRegistry,
                dispatcher,
                pipelineRegistry));
    }

    @Override
    public UnaryOperator<RestHandler> getRestHandlerWrapper(ThreadContext threadContext) {
        return restHandler -> new CrateRestMainAction.RestFilter(settings, restHandler);
    }

    @Override
    public List<RestHandler> getRestHandlers(Settings settings,
                                             RestController restController,
                                             ClusterSettings clusterSettings,
                                             IndexScopedSettings indexScopedSettings,
                                             SettingsFilter settingsFilter,
                                             IndexNameExpressionResolver indexNameExpressionResolver,
                                             Supplier<DiscoveryNodes> nodesInCluster) {
        Path siteDir = PathUtils.get(PATH_HOME_SETTING.get(settings)).normalize().resolve("lib").resolve("site");
        return Collections.singletonList(new CrateRestMainAction(settings, siteDir, restController));
    }
}
