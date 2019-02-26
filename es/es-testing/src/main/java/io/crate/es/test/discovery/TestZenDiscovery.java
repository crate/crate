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

package io.crate.es.test.discovery;

import io.crate.es.cluster.routing.allocation.AllocationService;
import io.crate.es.cluster.service.ClusterApplier;
import io.crate.es.cluster.service.MasterService;
import io.crate.es.common.io.stream.NamedWriteableRegistry;
import io.crate.es.common.settings.ClusterSettings;
import io.crate.es.common.settings.Setting;
import io.crate.es.common.settings.Settings;
import io.crate.es.discovery.Discovery;
import io.crate.es.discovery.DiscoveryModule;
import io.crate.es.discovery.zen.UnicastHostsProvider;
import io.crate.es.discovery.zen.ZenDiscovery;
import io.crate.es.discovery.zen.ZenPing;
import io.crate.es.plugins.DiscoveryPlugin;
import io.crate.es.plugins.Plugin;
import io.crate.es.threadpool.ThreadPool;
import io.crate.es.transport.TransportService;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static io.crate.es.discovery.zen.SettingsBasedHostsProvider.DISCOVERY_ZEN_PING_UNICAST_HOSTS_SETTING;

/**
 * A alternative zen discovery which allows using mocks for things like pings, as well as
 * giving access to internals.
 */
public class TestZenDiscovery extends ZenDiscovery {

    public static final Setting<Boolean> USE_MOCK_PINGS =
        Setting.boolSetting("discovery.zen.use_mock_pings", true, Setting.Property.NodeScope);

    /** A plugin which installs mock discovery and configures it to be used. */
    public static class TestPlugin extends Plugin implements DiscoveryPlugin {
        protected final Settings settings;
        public TestPlugin(Settings settings) {
            this.settings = settings;
        }

        @Override
        public Map<String, Supplier<Discovery>> getDiscoveryTypes(ThreadPool threadPool, TransportService transportService,
                                                                  NamedWriteableRegistry namedWriteableRegistry,
                                                                  MasterService masterService, ClusterApplier clusterApplier,
                                                                  ClusterSettings clusterSettings, UnicastHostsProvider hostsProvider,
                                                                  AllocationService allocationService) {
            return Collections.singletonMap("test-zen",
                () -> new TestZenDiscovery(
                    // we don't get the latest setting which were updated by the extra settings for the plugin. TODO: fix.
                    Settings.builder().put(settings).putList(DISCOVERY_ZEN_PING_UNICAST_HOSTS_SETTING.getKey()).build(),
                    threadPool, transportService, namedWriteableRegistry, masterService,
                    clusterApplier, clusterSettings, hostsProvider, allocationService));
        }

        @Override
        public List<Setting<?>> getSettings() {
            return Collections.singletonList(USE_MOCK_PINGS);
        }

        @Override
        public Settings additionalSettings() {
            return Settings.builder()
                .put(DiscoveryModule.DISCOVERY_TYPE_SETTING.getKey(), "test-zen")
                .putList(DISCOVERY_ZEN_PING_UNICAST_HOSTS_SETTING.getKey())
                .build();
        }
    }

    private TestZenDiscovery(Settings settings, ThreadPool threadPool, TransportService transportService,
                             NamedWriteableRegistry namedWriteableRegistry, MasterService masterService,
                             ClusterApplier clusterApplier, ClusterSettings clusterSettings, UnicastHostsProvider hostsProvider,
                             AllocationService allocationService) {
        super(settings, threadPool, transportService, namedWriteableRegistry, masterService, clusterApplier, clusterSettings,
            hostsProvider, allocationService, Collections.emptyList());
    }

    @Override
    protected ZenPing newZenPing(Settings settings, ThreadPool threadPool, TransportService transportService,
                                 UnicastHostsProvider hostsProvider) {
        if (USE_MOCK_PINGS.get(settings)) {
            return new MockZenPing(settings, this);
        } else {
            return super.newZenPing(settings, threadPool, transportService, hostsProvider);
        }
    }

    public ZenPing getZenPing() {
        return zenPing;
    }
}
