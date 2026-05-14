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

package io.crate.integrationtests;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.test.IntegTestCase;
import org.junit.Before;
import org.junit.Test;

import io.netty.handler.codec.quic.Quic;

@IntegTestCase.ClusterScope(numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class Http3BlobIntegrationTest extends Http3IntegrationTest {

    private static final String BLOB_BODY = "abcdefghijklmnopqrstuvwxyz";
    private static final String BLOB_DIGEST = "32d10c7b8cf96570ca04ce37f2a19d84240d3a89";
    private static final String BLOB_PATH = "/_blobs/test/" + BLOB_DIGEST;

    @Override
    protected boolean http3Enabled() {
        return true;
    }

    @Before
    public void createBlobTable() throws Exception {
        execute("create blob table test clustered into 1 shards with (number_of_replicas = 0)");
        ensureGreen();
    }

    @Test
    public void test_put_and_get_blob_via_http3() throws Exception {
        assumeThat(Quic.isAvailable())
            .as("native QUIC must be available on this platform")
            .isTrue();

        InetSocketAddress serverAddress = cluster().getInstance(HttpServerTransport.class)
            .boundAddress().publishAddress().address();
        try (Http3TestClient client = new Http3TestClient(serverAddress)) {
            var putResponse = client.put(BLOB_PATH, BLOB_BODY.getBytes(StandardCharsets.UTF_8));
            assertThat(putResponse.status()).isEqualTo(201);

            var getResponse = client.get(BLOB_PATH);
            assertThat(getResponse.status()).isEqualTo(200);
            assertThat(getResponse.body()).isEqualTo(BLOB_BODY);
        }
    }
}
