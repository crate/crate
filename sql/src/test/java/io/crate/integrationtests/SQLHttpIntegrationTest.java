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


import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.http.HttpServerTransport;
import org.junit.Before;

import java.io.IOException;
import java.net.InetSocketAddress;

public abstract class SQLHttpIntegrationTest extends SQLTransportIntegrationTest {

    private HttpPost httpPost;

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return ImmutableSettings.settingsBuilder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("http.host", "127.0.0.1")
                .put("http.enabled", true)
                .build();
    }

    @Before
    public void setup() {
        HttpServerTransport httpServerTransport = internalCluster().getInstance(HttpServerTransport.class);
        InetSocketAddress address = ((InetSocketTransportAddress) httpServerTransport.boundAddress().publishAddress())
                .address();
        httpPost = new HttpPost(String.format("http://%s:%s/_sql?error_trace", address.getHostName(), address.getPort()));
    }

    protected CloseableHttpClient httpClient = HttpClients.createDefault();

    protected CloseableHttpResponse post(String body) throws IOException {

        if(body != null){
            StringEntity bodyEntity = new StringEntity(body);
            httpPost.setEntity(bodyEntity);
        }
        return httpClient.execute(httpPost);
    }

    protected CloseableHttpResponse post() throws IOException {
        return post(null);
    }

}
