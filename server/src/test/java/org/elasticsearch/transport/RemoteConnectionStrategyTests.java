/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.elasticsearch.transport;

import io.crate.common.unit.TimeValue;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import static org.mockito.Mockito.mock;

public class RemoteConnectionStrategyTests extends ESTestCase {

    public void testStrategyChangeMeansThatStrategyMustBeRebuilt() {
        ClusterConnectionManager connectionManager = new ClusterConnectionManager(Settings.EMPTY, mock(Transport.class));
        RemoteConnectionManager remoteConnectionManager = new RemoteConnectionManager("cluster-alias", connectionManager);
        FakeConnectionStrategy first = new FakeConnectionStrategy("cluster-alias", mock(TransportService.class), remoteConnectionManager,
                                                                  RemoteConnectionStrategy.ConnectionStrategy.PROXY);
        Settings newSettings = Settings.builder()
            .put(RemoteConnectionStrategy.REMOTE_CONNECTION_MODE.getKey(), "sniff")
            .put(SniffConnectionStrategy.REMOTE_CLUSTER_SEEDS.getKey(), "127.0.0.1:9300")
            .build();
        assertTrue(first.shouldRebuildConnection(newSettings));
    }

    public void testSameStrategyChangeMeansThatStrategyDoesNotNeedToBeRebuilt() {
        ClusterConnectionManager connectionManager = new ClusterConnectionManager(Settings.EMPTY, mock(Transport.class));
        RemoteConnectionManager remoteConnectionManager = new RemoteConnectionManager("cluster-alias", connectionManager);
        FakeConnectionStrategy first = new FakeConnectionStrategy("cluster-alias", mock(TransportService.class), remoteConnectionManager,
                                                                  RemoteConnectionStrategy.ConnectionStrategy.PROXY);
        Settings newSettings = Settings.builder()
            .put(RemoteConnectionStrategy.REMOTE_CONNECTION_MODE.getKey(), "proxy")
            .put(ProxyConnectionStrategy.PROXY_ADDRESS.getKey(), "127.0.0.1:9300")
            .build();
        assertFalse(first.shouldRebuildConnection(newSettings));
    }

    public void testChangeInConnectionProfileMeansTheStrategyMustBeRebuilt() {
        ClusterConnectionManager connectionManager = new ClusterConnectionManager(TestProfiles.LIGHT_PROFILE, mock(Transport.class));
        assertEquals(TimeValue.MINUS_ONE, connectionManager.getConnectionProfile().getPingInterval());
        assertEquals(false, connectionManager.getConnectionProfile().getCompressionEnabled());
        RemoteConnectionManager remoteConnectionManager = new RemoteConnectionManager("cluster-alias", connectionManager);
        FakeConnectionStrategy first = new FakeConnectionStrategy("cluster-alias", mock(TransportService.class), remoteConnectionManager,
                                                                  RemoteConnectionStrategy.ConnectionStrategy.PROXY);

        Settings.Builder newBuilder = Settings.builder();
        newBuilder.put(RemoteConnectionStrategy.REMOTE_CONNECTION_MODE.getKey(), "proxy");
        newBuilder.put(ProxyConnectionStrategy.PROXY_ADDRESS.getKey(), "127.0.0.1:9300");
        if (randomBoolean()) {
            newBuilder.put(RemoteConnectionStrategy.REMOTE_CONNECTION_PING_SCHEDULE.getKey(),
                           TimeValue.timeValueSeconds(5));
        } else {
            newBuilder.put(RemoteConnectionStrategy.REMOTE_CONNECTION_COMPRESS.getKey(), true);
        }
        assertTrue(first.shouldRebuildConnection(newBuilder.build()));
    }

    public void testCorrectChannelNumber() {
        for (RemoteConnectionStrategy.ConnectionStrategy strategy : RemoteConnectionStrategy.ConnectionStrategy.values()) {
            String settingKey = RemoteConnectionStrategy.REMOTE_CONNECTION_MODE.getKey();
            Settings proxySettings = Settings.builder().put(settingKey, strategy.name()).build();
            ConnectionProfile proxyProfile = RemoteConnectionStrategy.buildConnectionProfile(Settings.EMPTY, proxySettings);
            assertEquals("Incorrect number of channels for " + strategy.name(),
                         strategy.getNumberOfChannels(), proxyProfile.getNumConnections());
        }
    }

    private static class FakeConnectionStrategy extends RemoteConnectionStrategy {

        private final ConnectionStrategy strategy;

        FakeConnectionStrategy(String clusterAlias, TransportService transportService, RemoteConnectionManager connectionManager,
                               RemoteConnectionStrategy.ConnectionStrategy strategy) {
            super(clusterAlias, transportService, connectionManager, Settings.EMPTY);
            this.strategy = strategy;
        }

        @Override
        protected boolean strategyMustBeRebuilt(Settings newSettings) {
            return false;
        }

        @Override
        protected ConnectionStrategy strategyType() {
            return this.strategy;
        }

        @Override
        protected boolean shouldOpenMoreConnections() {
            return false;
        }

        @Override
        protected void connectImpl(ActionListener<Void> listener) {

        }
    }
}
