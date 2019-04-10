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


import io.crate.common.Hex;
import io.crate.test.utils.Blobs;
import org.apache.http.Header;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.http.HttpServerTransport;
import org.junit.Before;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Locale;

import static org.hamcrest.Matchers.is;

public abstract class SQLHttpIntegrationTest extends SQLTransportIntegrationTest {

    private HttpPost httpPost;
    private InetSocketAddress address;

    protected final CloseableHttpClient httpClient;
    private final boolean usesSSL;

    public SQLHttpIntegrationTest() {
        this(false);
    }

    public SQLHttpIntegrationTest(boolean useSSL) {
        this.httpClient = HttpClients.custom()
            .setSSLHostnameVerifier(new NoopHostnameVerifier()).build();
        this.usesSSL = useSSL;
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put("http.host", "127.0.0.1")
            .build();
    }

    @Before
    public void setup() {
        HttpServerTransport httpServerTransport = internalCluster().getInstance(HttpServerTransport.class);
        address = httpServerTransport.boundAddress().publishAddress().address();
        httpPost = new HttpPost(String.format(Locale.ENGLISH,
            "%s://%s:%s/_sql?error_trace",
            usesSSL ? "https" : "http", address.getHostName(), address.getPort()));
    }


    protected CloseableHttpResponse post(String body, @Nullable Header[] headers) throws IOException {
        if (body != null) {
            StringEntity bodyEntity = new StringEntity(body, ContentType.APPLICATION_JSON);
            httpPost.setEntity(bodyEntity);
        }
        httpPost.setHeaders(headers);
        return httpClient.execute(httpPost);
    }

    protected CloseableHttpResponse post(String body) throws IOException {
        return post(body, null);
    }

    protected String upload(String table, String content) throws IOException {
        String digest = blobDigest(content);
        String url = Blobs.url(usesSSL, address, table, digest);
        HttpPut httpPut = new HttpPut(url);
        httpPut.setEntity(new StringEntity(content));

        CloseableHttpResponse response = httpClient.execute(httpPut);
        assertThat(response.getStatusLine().getStatusCode(), is(201));
        response.close();

        return url;
    }

    protected String blobDigest(String content) {
        return Hex.encodeHexString(Blobs.digest(content));
    }
}
