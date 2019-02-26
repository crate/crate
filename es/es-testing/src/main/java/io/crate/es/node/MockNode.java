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

package io.crate.es.node;

import io.crate.es.client.node.NodeClient;
import io.crate.es.cluster.ClusterInfo;
import io.crate.es.cluster.ClusterInfoService;
import io.crate.es.cluster.MockInternalClusterInfoService;
import io.crate.es.cluster.node.DiscoveryNode;
import io.crate.es.cluster.service.ClusterService;
import io.crate.es.common.settings.ClusterSettings;
import io.crate.es.common.settings.Settings;
import io.crate.es.common.transport.BoundTransportAddress;
import io.crate.es.common.util.BigArrays;
import io.crate.es.common.util.MockBigArrays;
import io.crate.es.common.util.MockPageCacheRecycler;
import io.crate.es.common.util.PageCacheRecycler;
import io.crate.es.env.Environment;
import io.crate.es.indices.breaker.CircuitBreakerService;
import io.crate.es.indices.recovery.RecoverySettings;
import io.crate.es.plugins.Plugin;
import io.crate.es.test.transport.MockTransportService;
import io.crate.es.threadpool.ThreadPool;
import io.crate.es.transport.Transport;
import io.crate.es.transport.TransportInterceptor;
import io.crate.es.transport.TransportService;

import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * A node for testing which allows:
 * <ul>
 *   <li>Overriding Version.CURRENT</li>
 *   <li>Adding test plugins that exist on the classpath</li>
 * </ul>
 */
public class MockNode extends Node {

    private final Collection<Class<? extends Plugin>> classpathPlugins;

    public MockNode(final Settings settings, final Collection<Class<? extends Plugin>> classpathPlugins) {
        this(settings, classpathPlugins, true);
    }

    public MockNode(
            final Settings settings,
            final Collection<Class<? extends Plugin>> classpathPlugins,
            final boolean forbidPrivateIndexSettings) {
        this(settings, classpathPlugins, null, forbidPrivateIndexSettings);
    }

    public MockNode(
            final Settings settings,
            final Collection<Class<? extends Plugin>> classpathPlugins,
            final Path configPath) {
        this(settings, classpathPlugins, configPath, true);
    }

    public MockNode(
            final Settings settings,
            final Collection<Class<? extends Plugin>> classpathPlugins,
            final Path configPath,
            final boolean forbidPrivateIndexSettings) {
        this(
                InternalSettingsPreparer.prepareEnvironment(settings, null, Collections.emptyMap(), configPath),
                classpathPlugins,
                forbidPrivateIndexSettings);
    }

    public MockNode(final Environment environment, final Collection<Class<? extends Plugin>> classpathPlugins) {
        this(environment, classpathPlugins, true);
    }

    private MockNode(
            final Environment environment,
            final Collection<Class<? extends Plugin>> classpathPlugins,
            final boolean forbidPrivateIndexSettings) {
        super(environment, classpathPlugins, forbidPrivateIndexSettings);
        this.classpathPlugins = classpathPlugins;
    }

    /**
     * The classpath plugins this node was constructed with.
     */
    public Collection<Class<? extends Plugin>> getClasspathPlugins() {
        return classpathPlugins;
    }

    @Override
    protected BigArrays createBigArrays(PageCacheRecycler pageCacheRecycler, CircuitBreakerService circuitBreakerService) {
        if (getPluginsService().filterPlugins(NodeMocksPlugin.class).isEmpty()) {
            return super.createBigArrays(pageCacheRecycler, circuitBreakerService);
        }
        return new MockBigArrays(pageCacheRecycler, circuitBreakerService);
    }

    @Override
    PageCacheRecycler createPageCacheRecycler(Settings settings) {
        if (getPluginsService().filterPlugins(NodeMocksPlugin.class).isEmpty()) {
            return super.createPageCacheRecycler(settings);
        }
        return new MockPageCacheRecycler(settings);
    }

    @Override
    protected TransportService newTransportService(Settings settings, Transport transport, ThreadPool threadPool,
                                                   TransportInterceptor interceptor,
                                                   Function<BoundTransportAddress, DiscoveryNode> localNodeFactory,
                                                   ClusterSettings clusterSettings, Set<String> taskHeaders) {
        // we use the MockTransportService.TestPlugin class as a marker to create a network
        // module with this MockNetworkService. NetworkService is such an integral part of the systme
        // we don't allow to plug it in from plugins or anything. this is a test-only override and
        // can't be done in a production env.
        if (getPluginsService().filterPlugins(MockTransportService.TestPlugin.class).isEmpty()) {
            return super.newTransportService(settings, transport, threadPool, interceptor, localNodeFactory, clusterSettings, taskHeaders);
        } else {
            return new MockTransportService(settings, transport, threadPool, interceptor, localNodeFactory, clusterSettings, taskHeaders);
        }
    }

    @Override
    protected void processRecoverySettings(ClusterSettings clusterSettings, RecoverySettings recoverySettings) {
        if (false == getPluginsService().filterPlugins(RecoverySettingsChunkSizePlugin.class).isEmpty()) {
            clusterSettings.addSettingsUpdateConsumer(RecoverySettingsChunkSizePlugin.CHUNK_SIZE_SETTING, recoverySettings::setChunkSize);
        }
    }

    @Override
    protected ClusterInfoService newClusterInfoService(Settings settings, ClusterService clusterService,
                                                       ThreadPool threadPool, NodeClient client, Consumer<ClusterInfo> listener) {
        if (getPluginsService().filterPlugins(MockInternalClusterInfoService.TestPlugin.class).isEmpty()) {
            return super.newClusterInfoService(settings, clusterService, threadPool, client, listener);
        } else {
            return new MockInternalClusterInfoService(settings, clusterService, threadPool, client, listener);
        }
    }

    @Override
    protected void registerDerivedNodeNameWithLogger(String nodeName) {
        // Nothing to do because test uses the thread name
    }
}
