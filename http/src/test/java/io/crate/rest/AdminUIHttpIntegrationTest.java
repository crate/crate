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

package io.crate.rest;

import io.crate.plugin.HttpTransportPlugin;
import org.apache.http.Header;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.Netty4Plugin;
import org.junit.Before;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.common.network.NetworkModule.HTTP_DEFAULT_TYPE_SETTING;
import static org.elasticsearch.common.network.NetworkModule.HTTP_ENABLED;
import static org.hamcrest.core.Is.is;

public abstract class AdminUIHttpIntegrationTest extends ESIntegTestCase {

    protected InetSocketAddress address;
    protected CloseableHttpClient httpClient = HttpClients.createDefault();

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(HTTP_ENABLED.getKey(), true)
            .put(HTTP_DEFAULT_TYPE_SETTING.getKey(), "netty4")
            .build();
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(HttpTransportPlugin.class, Netty4Plugin.class);
    }

    @Before
    public void setup() throws IOException {
        Iterable<HttpServerTransport> transports = internalCluster().getInstances(HttpServerTransport.class);
        Iterator<HttpServerTransport> httpTransports = transports.iterator();
        address = httpTransports.next().boundAddress().publishAddress().address();
        // place index file
        final Path indexDirectory = internalCluster().getInstance(Environment.class).libFile().resolve("site");
        Files.createDirectories(indexDirectory);
        final Path indexFile = indexDirectory.resolve("index.html");
        Files.write(indexFile, Collections.singletonList("<h1>Crate Admin</h1>"), Charset.forName("UTF-8"));
    }

    private CloseableHttpResponse executeAndDefaultAssertions(HttpUriRequest request) throws IOException {
        CloseableHttpResponse resp = httpClient.execute(request);
        assertThat(resp.containsHeader("Connection"), is(false));
        return resp;
    }

    CloseableHttpResponse get(String uri) throws IOException {
        return get(uri, null);
    }

    CloseableHttpResponse get(String uri, Header[] headers) throws IOException {
        HttpGet httpGet = new HttpGet(String.format(Locale.ENGLISH, "http://%s:%s/%s", address.getHostName(), address.getPort(), uri));
        if (headers != null) {
            httpGet.setHeaders(headers);
        }
        return executeAndDefaultAssertions(httpGet);
    }

    CloseableHttpResponse browserGet(String uri) throws IOException {
        Header[] headers = {
            browserHeader()
        };
        return get(uri, headers);
    }

    CloseableHttpResponse post(String uri) throws IOException {
        HttpPost httpPost = new HttpPost(String.format(Locale.ENGLISH, "http://%s:%s/%s", address.getHostName(), address.getPort(), uri));
        return executeAndDefaultAssertions(httpPost);
    }

    List<URI> getAllRedirectLocations(String uri, Header[] headers) throws IOException {
        CloseableHttpResponse response = null;
        try {
            HttpClientContext context = HttpClientContext.create();
            HttpGet httpGet = new HttpGet(String.format(Locale.ENGLISH, "http://%s:%s/%s", address.getHostName(), address.getPort(), uri));
            if (headers != null) {
                httpGet.setHeaders(headers);
            }
            response = httpClient.execute(httpGet, context);

            // get all redirection locations
            return context.getRedirectLocations();
        } finally {
            if(response != null) {
                response.close();
            }
        }
    }

    static BasicHeader browserHeader() {
        return new BasicHeader("User-Agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.106 Safari/537.36");
    }
}
