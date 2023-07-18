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

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.util.concurrent.TestFutureUtils;
import org.elasticsearch.test.ESTestCase;

public abstract class AbstractSimpleTransportTestCase extends ESTestCase {

    /**
     * Connect to the specified node with the default connection profile
     *
     * @param service service to connect from
     * @param node the node to connect to
     */
    public static void connectToNode(TransportService service, DiscoveryNode node) throws ConnectTransportException {
        TestFutureUtils.get(fut -> service.connectToNode(node, fut.map(x -> null)));
    }

    /**
     * Establishes and returns a new connection to the given node from the given {@link TransportService}.
     *
     * @param service service to connect from
     * @param node the node to connect to
     * @param connectionProfile the connection profile to use
     */
    public static Transport.Connection openConnection(TransportService service, DiscoveryNode node, ConnectionProfile connectionProfile) {
        return TestFutureUtils.get(fut -> service.openConnection(node, connectionProfile, fut));
    }
}
