/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.testing;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

public final class DiscoveryNodes {

    private DiscoveryNodes() {}

    // Use high initial value to avoid conflict with ESTestCase#portGenerator
    private static final AtomicInteger PORT_GENERATOR = new AtomicInteger(100);

    public static DiscoveryNode newNode(String nodeId) {
        return new DiscoveryNode(
            nodeId,
            newFakeAddress(),
            Version.CURRENT);
    }

    /**
     * Re-implementation of {@link ESTestCase#buildNewFakeTransportAddress()}
     * that does not cause RandomizedTest to be loaded so it's usable in benchmarks
     */
    static TransportAddress newFakeAddress() {
        return new TransportAddress(TransportAddress.META_ADDRESS, PORT_GENERATOR.incrementAndGet());
    }

    public static DiscoveryNode newNode(String name, String id) {
        return new DiscoveryNode(
            name,
            id,
            newFakeAddress(),
            Collections.emptyMap(),
            Collections.emptySet(),
            Version.CURRENT);
    }
}
