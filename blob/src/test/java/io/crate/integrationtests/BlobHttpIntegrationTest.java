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


import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import io.crate.blob.v2.BlobIndices;
import io.crate.test.integration.CrateIntegrationTest;
import org.apache.http.Header;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.*;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.http.HttpServerTransport;
import org.junit.Before;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.Locale;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class BlobHttpIntegrationTest extends CrateIntegrationTest {

    protected InetSocketAddress address;
    protected InetSocketAddress address2;

    protected CloseableHttpClient httpClient = HttpClients.createDefault();

    @Before
    public void setup() throws ExecutionException, InterruptedException {
        Iterable<HttpServerTransport> transports = cluster().getInstances(HttpServerTransport.class);
        Iterator<HttpServerTransport> httpTransports = transports.iterator();
        address = ((InetSocketTransportAddress) httpTransports.next().boundAddress().publishAddress()).address();
        address2 = ((InetSocketTransportAddress) httpTransports.next().boundAddress().publishAddress()).address();
        BlobIndices blobIndices = cluster().getInstance(BlobIndices.class);

        Settings indexSettings = ImmutableSettings.builder()
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 2)
                .build();
        blobIndices.createBlobTable("test", indexSettings).get();
        blobIndices.createBlobTable("test_blobs2", indexSettings).get();

        client().admin().indices().prepareCreate("test_no_blobs")
                .setSettings(
                        ImmutableSettings.builder()
                                .put("number_of_shards", 2)
                                .put("number_of_replicas", 0).build()).execute().actionGet();
        ensureGreen();
    }

    protected String blobUri(String digest){
        return blobUri("test", digest);
    }

    protected String blobUri(String index, String digest){
        return String.format("%s/%s", index, digest);
    }

    protected CloseableHttpResponse put(String uri, String body) throws IOException {

        HttpPut httpPut = new HttpPut(String.format(Locale.ENGLISH, "http://%s:%s/_blobs/%s", address.getHostName(), address.getPort(), uri));
        if(body != null){
            StringEntity bodyEntity = new StringEntity(body);
            httpPut.setEntity(bodyEntity);
        }
        return httpClient.execute(httpPut);
    }

    protected CloseableHttpResponse get(String uri) throws IOException {
        return get(uri, null);
    }

    protected boolean mget(String[] uris, Header[][] headers, final String[] expectedContent) throws Throwable {
        final CountDownLatch latch = new CountDownLatch(uris.length);
        final ConcurrentHashMap<Integer, Boolean> results = new ConcurrentHashMap<>(uris.length);
        for (int i = 0; i < uris.length; i++) {
            final int indexerId = i;
            final String uri = uris[indexerId];
            final Header[] header = headers[indexerId];
            final String expected = expectedContent[indexerId];
            Thread thread = new Thread() {
                @Override
                public void run() {
                    try {
                        CloseableHttpResponse res = get(uri, header);
                        Integer statusCode = res.getStatusLine().getStatusCode();
                        String resultContent = EntityUtils.toString(res.getEntity());
                        if (!resultContent.equals(expected)) {
                            logger.warn(String.format(Locale.ENGLISH, "incorrect response %d -- length: %d expected: %d\n",
                                    indexerId, resultContent.length(), expected.length()));
                        }
                        results.put(indexerId, (statusCode >= 200 && statusCode < 300 && expected.equals(resultContent)));
                    } catch (Exception e) {
                        logger.warn("**** failed indexing thread {}", e, indexerId);
                    } finally {
                        latch.countDown();
                    }
                }
            };
            thread.start();
        }
        latch.await(30L, TimeUnit.SECONDS);
        return Iterables.all(results.values(), new Predicate<Boolean>() {
            @Override
            public boolean apply(Boolean input) {
                return input;
            }
        });
    }

    protected CloseableHttpResponse get(String uri, Header[] headers) throws IOException {
        HttpGet httpGet = new HttpGet(String.format(Locale.ENGLISH, "http://%s:%s/_blobs/%s", address.getHostName(), address.getPort(), uri));
        if(headers != null){
           httpGet.setHeaders(headers);
        }
        return httpClient.execute(httpGet);
    }

    protected CloseableHttpResponse head(String uri) throws IOException {
        HttpHead httpHead = new HttpHead(String.format(Locale.ENGLISH, "http://%s:%s/_blobs/%s", address.getHostName(), address.getPort(), uri));
        return httpClient.execute(httpHead);
    }

    protected CloseableHttpResponse delete(String uri) throws IOException {
        HttpDelete httpDelete = new HttpDelete(String.format(Locale.ENGLISH, "http://%s:%s/_blobs/%s", address.getHostName(), address.getPort(), uri));
        return httpClient.execute(httpDelete);
    }

    public int getNumberOfRedirects(String uri, InetSocketAddress address) throws ClientProtocolException, IOException {
        CloseableHttpResponse response = null;
        int redirects = 0;

        try {
            HttpClientContext context = HttpClientContext.create();
            HttpHead httpHead = new HttpHead(String.format(Locale.ENGLISH, "http://%s:%s/_blobs/%s", address.getHostName(), address.getPort(), uri));
            response = httpClient.execute(httpHead, context);
            // get all redirection locations
            if(context.getRedirectLocations() != null){
                redirects = context.getRedirectLocations().size();
            }
        } finally {
            if(response != null) {
                response.close();
            }
        }
        return redirects;
    }

}
