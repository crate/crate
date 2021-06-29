/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.integrationtests;

import static org.hamcrest.CoreMatchers.is;

import java.nio.file.Path;
import java.nio.file.Paths;

import javax.net.ssl.SSLContext;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.Transport;
import org.junit.BeforeClass;
import org.junit.Test;

import io.crate.protocols.ssl.ConnectionTest;
import io.crate.protocols.ssl.ConnectionTest.ProbeResult;
import io.crate.protocols.ssl.SslContextProvider;
import io.crate.protocols.ssl.SslSettings;

@ESIntegTestCase.ClusterScope(numDataNodes = 3, supportsDedicatedMasters = false, numClientNodes = 0)
public class SSLDualModeTransportITest extends SQLIntegrationTestCase {

    private static Path keyStoreFile;
    private Settings sslSettings = Settings.builder()
        .put(SslSettings.SSL_TRANSPORT_MODE.getKey(), "dual")
        .put(SslSettings.SSL_KEYSTORE_FILEPATH.getKey(), keyStoreFile.toAbsolutePath().toString())
        .put(SslSettings.SSL_KEYSTORE_PASSWORD.getKey(), "keystorePassword")
        .put(SslSettings.SSL_KEYSTORE_KEY_PASSWORD.getKey(), "keystorePassword")
        .build();

    @BeforeClass
    public static void beforeIntegrationTest() throws Exception {
        keyStoreFile = Paths.get(SSLDualModeTransportITest.class.getClassLoader().getResource("keystore.pcks12").toURI());
    }

    @Override
    protected boolean addMockTransportService() {
        return false;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Builder builder = Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(sslSettings);

        if (nodeOrdinal == 1) {
            builder.put(SslSettings.SSL_TRANSPORT_MODE.getKey(), "off");
        }
        if (nodeOrdinal == 2) {
            builder.put(SslSettings.SSL_TRANSPORT_MODE.getKey(), "off");
        }
        return builder.build();
    }

    @Test
    public void test_dual_mode_on_first_node_allows_cluster_to_form_if_other_nodes_have_no_ssl() throws Exception {
        // This test covers all kinds of client-server pairs where SSL must not be used.
        // 1 dual node, 2 ssl-off node setup covers dual->off, off->dual, off->off connections.
        // Pairs which must be connected by SSL are tested in SSLTransportITest
        execute("select count(*) from sys.nodes");
        assertThat(response.rows()[0][0], is(3L));

        SslContextProvider sslContextProvider = new SslContextProvider(sslSettings);
        SSLContext sslContext = sslContextProvider.jdkSSLContext();

        for (var transport : internalCluster().getInstances(Transport.class)) {
            var publishAddress = transport.boundAddress().publishAddress();
            var address = publishAddress.address();
            ProbeResult probeResult = ConnectionTest.probeSSL(sslContext, address);
            assertThat(probeResult, is(ProbeResult.SSL_MISSING));
        }
    }
}
