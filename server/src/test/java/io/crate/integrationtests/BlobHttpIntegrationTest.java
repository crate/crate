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

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.http.Header;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.cluster.metadata.AutoExpandReplicas;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.http.HttpServerTransport;
import org.junit.After;
import org.junit.Before;

import io.crate.blob.BlobTransferStatus;
import io.crate.blob.BlobTransferTarget;
import io.crate.blob.v2.BlobAdminClient;
import io.crate.test.utils.Blobs;

public abstract class BlobHttpIntegrationTest extends BlobIntegrationTestBase {

    protected InetSocketAddress dataNode1;
    protected InetSocketAddress dataNode2;
    protected InetSocketAddress randomNode;

    protected CloseableHttpClient httpClient = HttpClients.createDefault();

    static {
        System.setProperty("tests.short_timeouts", "true");
    }


    @Before
    public void setup() throws ExecutionException, InterruptedException {
        randomNode = cluster().getInstances(HttpServerTransport.class)
            .iterator().next().boundAddress().publishAddress().address();
        Iterable<HttpServerTransport> transports = cluster().getDataNodeInstances(HttpServerTransport.class);
        Iterator<HttpServerTransport> httpTransports = transports.iterator();
        dataNode1 = httpTransports.next().boundAddress().publishAddress().address();
        dataNode2 = httpTransports.next().boundAddress().publishAddress().address();
        BlobAdminClient blobAdminClient = cluster().getInstance(BlobAdminClient.class);

        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2)
            // SETTING_AUTO_EXPAND_REPLICAS is enabled by default
            // but for this test it needs to be disabled so we can have 0 replicas
            .put(AutoExpandReplicas.SETTING_KEY, "false")
            .build();
        blobAdminClient.createBlobTable("test", indexSettings).get();
        blobAdminClient.createBlobTable("test_blobs2", indexSettings).get();

        client().admin().indices().create(new CreateIndexRequest("test_no_blobs")
            .settings(
                Settings.builder()
                    .put("number_of_shards", 2)
                    .put("number_of_replicas", 0).build())).get();
        ensureGreen();
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

    protected String blobUri(String digest) {
        return blobUri("test", digest);
    }

    protected String blobUri(String index, String digest) {
        return String.format(Locale.ENGLISH, "%s/%s", index, digest);
    }

    protected CloseableHttpResponse put(String uri, String body) throws IOException {
        HttpPut httpPut = new HttpPut(Blobs.url(false, randomNode, uri));
        if (body != null) {
            StringEntity bodyEntity = new StringEntity(body);
            httpPut.setEntity(bodyEntity);
        }
        return executeAndDefaultAssertions(httpPut);
    }

    protected CloseableHttpResponse executeAndDefaultAssertions(HttpUriRequest request) throws IOException {
        CloseableHttpResponse resp = httpClient.execute(request);
        assertThat(resp.containsHeader("Connection")).isFalse();
        return resp;
    }

    protected CloseableHttpResponse get(String uri) throws IOException {
        return get(uri, new Header[] { new BasicHeader("Origin", "http://example.com") });
    }

    protected boolean mget(String[] uris, Header[][] headers, final String[] expectedContent) throws Throwable {
        final CountDownLatch latch = new CountDownLatch(uris.length);
        final ConcurrentHashMap<Integer, Boolean> results = new ConcurrentHashMap<>(uris.length);
        for (int i = 0; i < uris.length; i++) {
            final int indexerId = i;
            final String uri = uris[indexerId];
            final Header[] header = headers[indexerId];
            final String expected = expectedContent[indexerId];
            Thread thread = new Thread(() -> {
                try {
                    CloseableHttpResponse res = get(uri, header);
                    int statusCode = res.getStatusLine().getStatusCode();
                    String resultContent = EntityUtils.toString(res.getEntity());
                    if (!resultContent.equals(expected)) {
                        logger.warn(String.format(Locale.ENGLISH, "incorrect response %d -- length: %d expected: %d%n",
                            indexerId, resultContent.length(), expected.length()));
                    }
                    results.put(indexerId, (statusCode >= 200 && statusCode < 300 &&
                                            expected.equals(resultContent)));
                } catch (Exception e) {
                    logger.warn("**** failed indexing thread: " + indexerId, e);
                } finally {
                    latch.countDown();
                }
            });
            thread.start();
        }
        assertThat(latch.await(30L, TimeUnit.SECONDS)).isTrue();
        return results.values().stream().allMatch(input -> input);
    }

    protected CloseableHttpResponse get(String uri, Header[] headers) throws IOException {
        HttpGet httpGet = new HttpGet(String.format(Locale.ENGLISH, "http://%s:%s/_blobs/%s", dataNode1.getHostName(), dataNode1.getPort(), uri));
        if (headers != null) {
            httpGet.setHeaders(headers);
        }
        return executeAndDefaultAssertions(httpGet);
    }

    protected CloseableHttpResponse head(String uri) throws IOException {
        HttpHead httpHead = new HttpHead(String.format(Locale.ENGLISH, "http://%s:%s/_blobs/%s", dataNode1.getHostName(), dataNode1.getPort(), uri));
        return executeAndDefaultAssertions(httpHead);
    }

    protected CloseableHttpResponse delete(String uri) throws IOException {
        HttpDelete httpDelete = new HttpDelete(String.format(Locale.ENGLISH, "http://%s:%s/_blobs/%s", dataNode1.getHostName(), dataNode1.getPort(), uri));
        return executeAndDefaultAssertions(httpDelete);
    }

    public static List<String> getRedirectLocations(CloseableHttpClient client, String uri, InetSocketAddress address) throws IOException {
        CloseableHttpResponse response = null;
        try {
            HttpClientContext context = HttpClientContext.create();
            HttpHead httpHead = new HttpHead(String.format(Locale.ENGLISH, "http://%s:%s/_blobs/%s", address.getHostName(), address.getPort(), uri));
            response = client.execute(httpHead, context);

            List<URI> redirectLocations = context.getRedirectLocations();
            if (redirectLocations == null) {
                // client might not follow redirects automatically
                if (response.containsHeader("location")) {
                    List<String> redirects = new ArrayList<>(1);
                    for (Header location : response.getHeaders("location")) {
                        redirects.add(location.getValue());
                    }
                    return redirects;
                }
                return Collections.emptyList();
            }

            List<String> redirects = new ArrayList<>(1);
            for (URI redirectLocation : redirectLocations) {
                redirects.add(redirectLocation.toString());
            }
            return redirects;
        } finally {
            if (response != null) {
                IOUtils.closeWhileHandlingException(response);
            }
        }
    }

    public int getNumberOfRedirects(String uri, InetSocketAddress address) throws IOException {
        return getRedirectLocations(httpClient, uri, address).size();
    }

}
