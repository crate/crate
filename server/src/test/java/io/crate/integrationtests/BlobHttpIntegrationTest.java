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

import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpClient.Redirect;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import io.crate.blob.BlobTransferStatus;
import io.crate.blob.BlobTransferTarget;
import io.crate.common.concurrent.CompletableFutures;

public abstract class BlobHttpIntegrationTest extends BlobIntegrationTestBase {

    protected InetSocketAddress dataNode1;
    protected InetSocketAddress dataNode2;
    protected InetSocketAddress randomNode;

    protected HttpClient httpClient;

    static {
        System.setProperty("tests.short_timeouts", "true");
    }


    @Before
    public void setup() throws ExecutionException, InterruptedException {
        httpClient = HttpClient.newBuilder()
            .followRedirects(Redirect.NORMAL)
            .executor(cluster().getInstance(ThreadPool.class).generic())
            .build();
        randomNode = cluster().getInstances(HttpServerTransport.class)
            .iterator().next().boundAddress().publishAddress().address();
        Iterable<HttpServerTransport> transports = cluster().getDataNodeInstances(HttpServerTransport.class);
        Iterator<HttpServerTransport> httpTransports = transports.iterator();
        dataNode1 = httpTransports.next().boundAddress().publishAddress().address();
        dataNode2 = httpTransports.next().boundAddress().publishAddress().address();
        execute("create blob table test clustered into 2 shards with (number_of_replicas = 0)");
        execute("create blob table test_blobs2 clustered into 2 shards with (number_of_replicas = 0)");
        execute("create table test_no_blobs (x int) clustered into 2 shards with (number_of_replicas = 0)");
        ensureGreen();
    }

    @After
    public void closeClient() throws Exception {
        httpClient.close();
    }

    @SuppressWarnings("unchecked")
    @After
    public void assertNoActiveTransfersRemaining() throws Exception {
        Iterable<BlobTransferTarget> transferTargets = cluster().getInstances(BlobTransferTarget.class);
        final Field activeTransfersField = BlobTransferTarget.class.getDeclaredField("activeTransfers");
        activeTransfersField.setAccessible(true);
        assertBusy(() -> {
            for (BlobTransferTarget transferTarget : transferTargets) {
                try {
                    var activeTransfers = (Map<UUID, BlobTransferStatus>) activeTransfersField.get(transferTarget);
                    assertThat(activeTransfers.keySet()).isEmpty();
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    protected URI blobUri(String digest) {
        return blobUri("test", digest);
    }

    protected URI blobUri(String index, String digest) {
        String url = String.format(Locale.ENGLISH, "http://%s:%s/_blobs/%s/%s", dataNode1.getHostName(), dataNode1.getPort(), index, digest);
        return URI.create(url);
    }

    protected HttpResponse<String> put(String index, String digest, String body) throws Exception {
        return put(blobUri(index, digest), body);
    }

    protected HttpResponse<String> put(URI uri, String body) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(uri)
            .PUT(BodyPublishers.ofString(body))
            .build();
        HttpResponse<String> response = httpClient.send(request, BodyHandlers.ofString());
        assertThat(response.headers().firstValue("Connection")).isNotPresent();
        return response;
    }

    protected HttpResponse<String> get(String index, String digest) throws Exception {
        return get(blobUri(index, digest));
    }

    protected HttpResponse<String> get(URI uri) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(uri)
            .build();
        return httpClient.send(request, BodyHandlers.ofString());
    }

    protected void mget(URI[] uris, String[][] headers, final String[] expectedContent) throws Throwable {
        final List<CompletableFuture<HttpResponse<String>>> results = new ArrayList<>(uris.length);
        for (int i = 0; i < uris.length; i++) {
            var requestBuilder = HttpRequest.newBuilder(uris[i]);
            String[] header = headers[i];
            if (header != null) {
                requestBuilder.header(header[0], header[1]);
            }
            var responseFuture = httpClient.sendAsync(requestBuilder.build(), BodyHandlers.ofString());
            results.add(responseFuture);
        }
        List<HttpResponse<String>> responses = CompletableFutures.allAsList(results).get(60, TimeUnit.SECONDS);
        for (int i = 0; i < uris.length; i++) {
            HttpResponse<String> httpResponse = responses.get(i);
            String expected = expectedContent[i];
            assertThat(httpResponse.body()).isEqualTo(expected);
        }
    }


    protected HttpResponse<Void> head(URI uri) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(uri).HEAD().build();
        return httpClient.send(request, BodyHandlers.discarding());
    }

    protected HttpResponse<Void> delete(URI uri) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(uri).DELETE().build();
        return httpClient.send(request, BodyHandlers.discarding());
    }

    public static List<String> getRedirectLocations(HttpClient client, URI uri) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(uri)
            .build();
        var response = client.send(request, BodyHandlers.discarding());
        response = response.previousResponse().orElse(null);
        List<String> redirects = new ArrayList<>();
        while (response != null) {
            redirects.addAll(response.headers().allValues("location"));
            response = response.previousResponse().orElse(null);
        }
        return redirects;
    }

    public int getNumberOfRedirects(String digest, InetSocketAddress address) throws Exception {
        String url = String.format(Locale.ENGLISH, "http://%s:%s/_blobs/test/%s", address.getHostName(), address.getPort(), digest);
        return getRedirectLocations(httpClient, URI.create(url)).size();
    }

}
