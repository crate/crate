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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_QUIC_ENABLED;

import java.net.InetSocketAddress;
import java.net.http.HttpResponse;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.http.HttpServerTransport;
import org.junit.BeforeClass;

import io.crate.protocols.ssl.SslSettings;

public abstract class Http3IntegrationTest extends SQLHttpIntegrationTest {

    private static final String SELECT_1 = "{\"stmt\":\"SELECT 1\"}";

    private static String keystorePath;

    protected Http3IntegrationTest() {
        super(true);
    }

    @BeforeClass
    public static void resolveKeystore() throws Exception {
        keystorePath = Paths.get(
            Http3IntegrationTest.class.getClassLoader().getResource("keystore.pcks12").toURI()
        ).toAbsolutePath().toString();
    }

    protected abstract boolean http3Enabled();

    @Override
    protected final Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(SETTING_HTTP_QUIC_ENABLED.getKey(), http3Enabled())
            .put(SslSettings.SSL_HTTP_ENABLED.getKey(), true)
            .put(SslSettings.SSL_KEYSTORE_FILEPATH.getKey(), keystorePath)
            .put(SslSettings.SSL_KEYSTORE_PASSWORD.getKey(), "keystorePassword")
            .put(SslSettings.SSL_KEYSTORE_KEY_PASSWORD.getKey(), "keystorePassword")
            .build();
    }

    protected final HttpResponse<String> postHttpSelect() throws Exception {
        return post(SELECT_1);
    }

    protected final void assertSelectSucceeded(HttpResponse<String> response) {
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body()).contains("\"rowcount\":1");
    }

    protected final Optional<String> altSvcFromHeader(HttpResponse<String> response) {
        return response.headers().firstValue("alt-svc");
    }

    protected final String expectedAltSvcValue() {
        InetSocketAddress publishAddress = cluster().getInstance(HttpServerTransport.class)
            .boundAddress().publishAddress().address();
        return "h3=\":" + publishAddress.getPort() + "\"; ma=86400";
    }

    protected final String postHttp3Select() throws Exception {
        InetSocketAddress serverAddress = cluster().getInstance(HttpServerTransport.class)
            .boundAddress().publishAddress().address();
        try (Http3TestClient client = new Http3TestClient(serverAddress)) {
            return client.post("/_sql", SELECT_1);
        }
    }

    protected final void assertHttp3ClientCannotConnect() {
        InetSocketAddress serverAddress = cluster().getInstance(HttpServerTransport.class)
            .boundAddress().publishAddress().address();
        assertThatThrownBy(() -> {
            try (Http3TestClient client = new Http3TestClient(serverAddress)) {
                client.post("/_sql", SELECT_1);
            }
        }).satisfiesAnyOf(
            e -> assertThat(e).isInstanceOf(TimeoutException.class),
            e -> assertThat(e).isInstanceOf(ExecutionException.class)
                .cause().isInstanceOf(io.netty.handler.codec.quic.QuicClosedChannelException.class)
        );
    }
}
