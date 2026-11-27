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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.GZIPOutputStream;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.test.IntegTestCase;
import org.junit.Test;

@IntegTestCase.ClusterScope(numDataNodes = 1, supportsDedicatedMasters = false, numClientNodes = 0)
public class HttpInFlightRequestsBreakerIntegrationTest extends SQLHttpIntegrationTest {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(HierarchyCircuitBreakerService.IN_FLIGHT_REQUESTS_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "2mb")
            .build();
    }

    private static byte[] gzip(byte[] data) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (GZIPOutputStream gzip = new GZIPOutputStream(out)) {
            gzip.write(data);
        }
        return out.toByteArray();
    }

    private static byte[] deflate(byte[] data) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (DeflaterOutputStream deflate = new DeflaterOutputStream(out)) {
            deflate.write(data);
        }
        return out.toByteArray();
    }

    private HttpResponse<String> postGzip(byte[] body) throws Exception {
        return post("gzip", gzip(body));
    }

    private HttpResponse<String> post(String contentEncoding, byte[] encodedBody) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(uri)
            .header("Content-Type", "application/json")
            .header("Content-Encoding", contentEncoding)
            .POST(BodyPublishers.ofByteArray(encodedBody))
            .build();
        return httpClient.send(request, BodyHandlers.ofString());
    }

    @Test
    public void test_gzip_request_expanding_beyond_the_in_flight_requests_breaker_is_rejected() throws Exception {
        CircuitBreaker breaker = cluster().getInstance(CircuitBreakerService.class)
            .getBreaker(CircuitBreaker.IN_FLIGHT_REQUESTS);
        long usedBefore = breaker.getUsed();

        // Below http.max_content_length, but expands beyond the breaker's limit
        byte[] spaces = new byte[4 * 1024 * 1024];
        Arrays.fill(spaces, (byte) ' ');
        HttpResponse<String> response = postGzip(spaces);

        assertThat(response.statusCode()).isEqualTo(429);
        assertThat(response.body()).contains("http_request");
        assertBusy(() -> assertThat(breaker.getUsed()).isEqualTo(usedBefore));

        // The node is still serving requests
        response = postGzip("{\"stmt\": \"select 1\"}".getBytes(StandardCharsets.UTF_8));
        assertThat(response.statusCode()).isEqualTo(200);
        assertBusy(() -> assertThat(breaker.getUsed()).isEqualTo(usedBefore));
    }

    @Test
    public void test_deflate_request_expanding_beyond_the_in_flight_requests_breaker_is_rejected() throws Exception {
        CircuitBreaker breaker = cluster().getInstance(CircuitBreakerService.class)
            .getBreaker(CircuitBreaker.IN_FLIGHT_REQUESTS);
        long usedBefore = breaker.getUsed();

        byte[] spaces = new byte[4 * 1024 * 1024];
        Arrays.fill(spaces, (byte) ' ');
        HttpResponse<String> response = post("deflate", deflate(spaces));

        assertThat(response.statusCode()).isEqualTo(429);
        assertThat(response.body()).contains("http_request");
        assertBusy(() -> assertThat(breaker.getUsed()).isEqualTo(usedBefore));

        response = post("deflate", deflate("{\"stmt\": \"select 1\"}".getBytes(StandardCharsets.UTF_8)));
        assertThat(response.statusCode()).isEqualTo(200);
        assertBusy(() -> assertThat(breaker.getUsed()).isEqualTo(usedBefore));
    }

    @Test
    public void test_concurrent_gzip_requests_trip_on_their_combined_usage_and_nothing_stays_accounted()
            throws Exception {
        CircuitBreaker breaker = cluster().getInstance(CircuitBreakerService.class)
            .getBreaker(CircuitBreaker.IN_FLIGHT_REQUESTS);
        long usedBefore = breaker.getUsed();

        // Leading whitespace is valid JSON. Random whitespace compresses poorly enough for
        // the body to be received in many reads, so that concurrent requests interleave.
        byte[] stmt = "{\"stmt\": \"select 1\"}".getBytes(StandardCharsets.UTF_8);
        byte[] whitespace = {' ', '\t', '\n', '\r'};
        byte[] body = new byte[768 * 1024 + stmt.length];
        for (int i = 0; i < body.length - stmt.length; i++) {
            body[i] = whitespace[random().nextInt(whitespace.length)];
        }
        System.arraycopy(stmt, 0, body, body.length - stmt.length, stmt.length);
        byte[] compressed = gzip(body);

        // A single request fits within the limit
        assertThat(post("gzip", compressed).statusCode()).isEqualTo(200);
        assertBusy(() -> assertThat(breaker.getUsed()).isEqualTo(usedBefore));

        long trippedBefore = breaker.getTrippedCount();
        List<CompletableFuture<HttpResponse<String>>> responses = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            HttpRequest request = HttpRequest.newBuilder(uri)
                .header("Content-Type", "application/json")
                .header("Content-Encoding", "gzip")
                .POST(BodyPublishers.ofByteArray(compressed))
                .build();
            responses.add(httpClient.sendAsync(request, BodyHandlers.ofString()));
        }
        int rejected = 0;
        for (CompletableFuture<HttpResponse<String>> future : responses) {
            HttpResponse<String> response = future.get(30, TimeUnit.SECONDS);
            assertThat(response.statusCode()).isIn(200, 429);
            if (response.statusCode() == 429) {
                assertThat(response.body()).contains("http_request");
                rejected++;
            }
        }
        assertThat(breaker.getTrippedCount() - trippedBefore).isEqualTo(rejected);
        assertBusy(() -> assertThat(breaker.getUsed()).isEqualTo(usedBefore));

        assertThat(postGzip(stmt).statusCode()).isEqualTo(200);
    }
}
