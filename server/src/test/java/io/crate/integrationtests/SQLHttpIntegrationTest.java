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

import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpClient.Redirect;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpRequest.Builder;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Locale;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509ExtendedTrustManager;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.test.IntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.jetbrains.annotations.Nullable;
import org.junit.After;
import org.junit.Before;

import io.crate.common.Hex;
import io.crate.test.utils.Blobs;

public abstract class SQLHttpIntegrationTest extends IntegTestCase {

    private final boolean usesSSL;
    private final SSLContext sslContext;

    private InetSocketAddress address;
    protected URI uri;
    protected HttpClient httpClient;

    private static TrustManager[] trustAll = new TrustManager[]{
        // Workaround to disable hostname verification (+ all other other certificate validaiton)
        new X509ExtendedTrustManager() {

            public X509Certificate[] getAcceptedIssuers() {
                return null;
            }

            public void checkClientTrusted(X509Certificate[] certs, String authType) {
            }

            public void checkServerTrusted(java.security.cert.X509Certificate[] certs, String authType) {
            }

            @Override
            public void checkClientTrusted(X509Certificate[] chain, String authType, Socket socket) throws CertificateException {
            }

            @Override
            public void checkServerTrusted(X509Certificate[] chain, String authType, Socket socket) throws CertificateException {
            }

            @Override
            public void checkClientTrusted(X509Certificate[] chain, String authType, SSLEngine engine) throws CertificateException {
            }

            @Override
            public void checkServerTrusted(X509Certificate[] chain, String authType, SSLEngine engine) throws CertificateException {
            }
        }
    };

    public SQLHttpIntegrationTest() {
        this(false);
    }

    public SQLHttpIntegrationTest(boolean useSSL) {
        try {
            sslContext = SSLContext.getInstance("TLSv1.2");
            sslContext.init(null, trustAll, new SecureRandom());
        } catch (KeyManagementException | NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        this.usesSSL = useSSL;
    }

    @After
    public void closeClient() throws Exception {
        this.httpClient.close();
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
        this.httpClient = HttpClient.newBuilder()
            .followRedirects(Redirect.NORMAL)
            .executor(cluster().getInstance(ThreadPool.class).generic())
            .sslContext(sslContext)
            .build();
        HttpServerTransport httpServerTransport = cluster().getInstance(HttpServerTransport.class);
        address = httpServerTransport.boundAddress().publishAddress().address();
        uri = URI.create(String.format(Locale.ENGLISH,
            "%s://%s:%s/_sql?error_trace",
            usesSSL ? "https" : "http", address.getHostName(), address.getPort()));
    }

    protected HttpResponse<String> get(String urlPath) throws Exception {
        assert urlPath != null : "url cannot be null";
        URI uri = URI.create(String.format(Locale.ENGLISH,
            "%s://%s:%s/%s", usesSSL ? "https" : "http", address.getHostName(), address.getPort(), urlPath));
        HttpRequest request = HttpRequest.newBuilder(uri)
            .build();
        return httpClient.send(request, BodyHandlers.ofString());
    }

    protected HttpResponse<String> post(String urlPath,
                                        @Nullable String body, String[] ... headers) throws Exception {
        assert urlPath != null : "url cannot be null";
        URI uri = URI.create(String.format(Locale.ENGLISH,
            "%s://%s:%s/%s", usesSSL ? "https" : "http", address.getHostName(), address.getPort(), urlPath));
        HttpRequest request = HttpRequest.newBuilder(uri)
            .POST(body == null ? BodyPublishers.noBody() : BodyPublishers.ofString(body))
            .build();
        return httpClient.send(request, BodyHandlers.ofString());
    }

    protected HttpResponse<String> post(String body, String[] ... headers) throws Exception {
        Builder builder = HttpRequest.newBuilder(uri)
            .header("Content-Type", "application/json");
        if (body != null) {
            builder.POST(BodyPublishers.ofString(body));
        }
        for (String[] header : headers) {
            builder.headers(header[0], header[1]);
        }
        return httpClient.send(builder.build(), BodyHandlers.ofString());
    }

    protected URI upload(String table, String content) throws Exception {
        String digest = blobDigest(content);
        URI uri = Blobs.url(usesSSL, address, table, digest);
        HttpRequest request = HttpRequest.newBuilder(uri)
            .PUT(BodyPublishers.ofString(content))
            .build();
        HttpResponse<Void> response = httpClient.send(request, BodyHandlers.discarding());

        assertThat(response.statusCode()).isEqualTo(201);
        return uri;
    }

    protected String blobDigest(String content) {
        return Hex.encodeHexString(Blobs.digest(content));
    }
}
