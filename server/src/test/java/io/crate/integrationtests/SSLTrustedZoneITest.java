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
import static org.mockito.ArgumentMatchers.isNull;

import java.nio.file.Path;
import java.nio.file.Paths;

import javax.net.ssl.SSLContext;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.Transport;
import org.junit.BeforeClass;
import org.junit.Test;

import io.crate.test.utils.ConnectionTest;
import io.crate.test.utils.ConnectionTest.ProbeResult;
import io.crate.protocols.ssl.SslContextProvider;
import io.crate.protocols.ssl.SslSettings;

@ESIntegTestCase.ClusterScope(numDataNodes = 2, supportsDedicatedMasters = false, numClientNodes = 0)
public class SSLTrustedZoneITest extends SQLIntegrationTestCase {

    private static Path keyStoreFile;
    private Settings sslSettings = Settings.builder()
        .put(SslSettings.SSL_TRANSPORT_MODE.getKey(), "on")
        .put(SslSettings.SSL_KEYSTORE_FILEPATH.getKey(), keyStoreFile.toAbsolutePath().toString())
        .put(SslSettings.SSL_KEYSTORE_PASSWORD.getKey(), "keystorePassword")
        .put(SslSettings.SSL_KEYSTORE_KEY_PASSWORD.getKey(), "keystorePassword")
        .build();

    @BeforeClass
    public static void beforeIntegrationTest() throws Exception {
        keyStoreFile = Paths.get(SSLTransportITest.class.getClassLoader().getResource("keystore.pcks12").toURI());
    }

    @Override
    protected boolean addMockTransportService() {
        return false;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        var commonSettings =  Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put("auth.host_based.enabled", true)
            .put("auth.host_based.config.a.method", "cert")
            .put("auth.host_based.config.a.protocol", "transport")

            .put("auth.host_based.config.c.method", "trust")
            .put("auth.host_based.config.c.protocol", "http")
            .put("auth.host_based.config.d.method", "trust")
            .put("auth.host_based.config.d.protocol", "pg")
            .put(sslSettings);

        switch (nodeOrdinal) {
            case 0:
                // This node can switch to plaintext
                commonSettings.put("auth.host_based.config.a.switch_to_plaintext", "true");
                break;

            // Node 1 has default value "false" for switch_to_plaintext
        }
        return commonSettings.build();
    }



    @Test
    public void test_dual_mode_on_first_node_allows_cluster_to_form_if_other_nodes_have_no_ssl() throws Exception {
        execute("select count(*) from sys.nodes");
        assertThat(response.rows()[0][0], is(2L));

        SslContextProvider sslContextProvider = new SslContextProvider(sslSettings);
        SSLContext sslContext = sslContextProvider.jdkSSLContext();



        String[] nodeNames = internalCluster().getNodeNames();
        String nodeName1 = nodeNames[0];
        {
            var transport = internalCluster().getInstance(Transport.class, nodeName1);
            var publishAddress = transport.boundAddress().publishAddress();
            var address = publishAddress.address();
            ProbeResult probeResult = ConnectionTest.probeAfterHandshake(sslContext, address);
            assertThat(probeResult, is(ProbeResult.SSL_MISSING));
        }

        String nodeName2 = nodeNames[1];
        {
            var transport = internalCluster().getInstance(Transport.class, nodeName2);
            var publishAddress = transport.boundAddress().publishAddress();
            var address = publishAddress.address();
            ProbeResult probeResult = ConnectionTest.probeAfterHandshake(sslContext, address);
            assertThat(probeResult, is(ProbeResult.SSL_AVAILABLE));
        }
    }
}
