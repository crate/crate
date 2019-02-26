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
package io.crate.es.transport;

import io.crate.es.Version;
import io.crate.es.cluster.node.DiscoveryNode;
import io.crate.es.common.io.stream.NamedWriteableRegistry;
import io.crate.es.common.network.NetworkService;
import io.crate.es.common.settings.ClusterSettings;
import io.crate.es.common.settings.Settings;
import io.crate.es.common.unit.TimeValue;
import io.crate.es.common.util.BigArrays;
import io.crate.es.indices.breaker.NoneCircuitBreakerService;
import io.crate.es.test.transport.MockTransportService;

import java.io.IOException;
import java.util.Collections;

public class MockTcpTransportTests extends AbstractSimpleTransportTestCase {

    @Override
    protected MockTransportService build(Settings settings, Version version, ClusterSettings clusterSettings, boolean doHandshake) {
        NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(Collections.emptyList());
        Transport transport = new MockTcpTransport(settings, threadPool, BigArrays.NON_RECYCLING_INSTANCE,
            new NoneCircuitBreakerService(), namedWriteableRegistry, new NetworkService(Collections.emptyList()), version) {
            @Override
            public Version executeHandshake(DiscoveryNode node, TcpChannel mockChannel, TimeValue timeout) throws IOException,
                InterruptedException {
                if (doHandshake) {
                    return super.executeHandshake(node, mockChannel, timeout);
                } else {
                    return version.minimumCompatibilityVersion();
                }
            }
        };
        MockTransportService mockTransportService =
            MockTransportService.createNewService(settings, transport, version, threadPool, clusterSettings, Collections.emptySet());
        mockTransportService.start();
        return mockTransportService;
    }

    @Override
    public int channelsPerNodeConnection() {
        return 1;
    }
}
