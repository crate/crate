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

package io.crate.rest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.elasticsearch.common.network.NetworkModule.HTTP_DEFAULT_TYPE_SETTING;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpClient.Redirect;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpRequest.Builder;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.IntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Netty4Plugin;
import org.junit.After;
import org.junit.Before;

public abstract class AdminUIHttpIntegrationTest extends IntegTestCase {

    protected InetSocketAddress address;
    protected HttpClient httpClient;

    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(HTTP_DEFAULT_TYPE_SETTING.getKey(), "netty4")
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(Netty4Plugin.class);
    }

    @Before
    public void setup() throws Exception {
        httpClient = HttpClient.newBuilder()
            .followRedirects(Redirect.NORMAL)
            .executor(cluster().getInstance(ThreadPool.class).generic())
            .build();
        Iterable<HttpServerTransport> transports = cluster().getInstances(HttpServerTransport.class);
        Iterator<HttpServerTransport> httpTransports = transports.iterator();
        address = httpTransports.next().boundAddress().publishAddress().address();
        // place index file
        final Path indexDirectory = cluster().getInstance(Environment.class).libFile().resolve("site");
        Files.createDirectories(indexDirectory);
        final Path indexFile = indexDirectory.resolve("index.html");
        Files.write(indexFile, Collections.singletonList("<h1>Crate Admin</h1>"), Charset.forName("UTF-8"));
    }

    @After
    public void closeClient() {
        httpClient.close();
    }

    private HttpResponse<String> executeAndDefaultAssertions(HttpRequest request) throws Exception {
        var resp = httpClient.send(request, BodyHandlers.ofString());
        assertThat(resp.headers().firstValue("Connection")).isNotPresent();
        return resp;
    }

    HttpResponse<String> get(String path, String[] ... headers) throws Exception {
        URI uri = URI.create(String.format(Locale.ENGLISH, "http://%s:%s%s", address.getHostName(), address.getPort(), path));
        Builder builder = HttpRequest.newBuilder(uri);
        for (String[] header : headers) {
            builder.header(header[0], header[1]);
        }
        return executeAndDefaultAssertions(builder.build());
    }

    HttpResponse<String> browserGet(String uri) throws Exception {
        return get(uri, browserHeader());
    }

    HttpResponse<String> post(String path) throws Exception {
        URI uri = URI.create(String.format(Locale.ENGLISH, "http://%s:%s%s", address.getHostName(), address.getPort(), path));
        HttpRequest request = HttpRequest.newBuilder(uri)
            .POST(BodyPublishers.noBody())
            .build();
        return executeAndDefaultAssertions(request);
    }

    List<String> getAllRedirectLocations(String path, String[] ... headers) throws Exception {
        URI uri = URI.create(String.format(Locale.ENGLISH, "http://%s:%s%s", address.getHostName(), address.getPort(), path));
        Builder builder = HttpRequest.newBuilder(uri);
        for (String[] header : headers) {
            builder.header(header[0], header[1]);
        }
        var response = httpClient.send(builder.build(), BodyHandlers.discarding());
        response = response.previousResponse().orElse(null);
        List<String> redirects = new ArrayList<>();
        while (response != null) {
            redirects.addAll(response.headers().allValues("location"));
            response = response.previousResponse().orElse(null);
        }
        return redirects;
    }

    static String[] browserHeader() {
        return new String[] {
            "User-Agent",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.106 Safari/537.36"
        };
    }
}
