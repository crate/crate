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

package io.crate.client.transport;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

/**
 * Test to make sure TransportClient works correctly with InOut plugin
 */
public class TransportClientTest {


    /**
     * Instantiate a TransportClient to make sure dependency injection works correctly
     */
    @Test
    public void testTransportClient() {

        /**
         * InOut plugin modules must not be loaded for TransportClient instances
         */
        TransportClient client = new TransportClient();
        assertNotNull(client);

        /**
         * Internally, this get determined by the settings flag node.client which is set to true in case of
         * a TransportClient object. Thought the setting was given to the TransportClient with node.client = false
         * the constructor of TransportClient overwrites it to node.client = true
         */
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("node.client", false)
                .build();

        client = null;
        client = new TransportClient(settings);
        assertNotNull(client);

    }
}
