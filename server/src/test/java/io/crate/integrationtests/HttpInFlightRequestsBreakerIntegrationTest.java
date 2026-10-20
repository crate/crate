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
import java.util.Arrays;
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
            .put(HierarchyCircuitBreakerService.IN_FLIGHT_REQUESTS_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "10mb")
            .build();
    }

    private static byte[] gzip(byte[] data) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (GZIPOutputStream gzip = new GZIPOutputStream(out)) {
            gzip.write(data);
        }
        return out.toByteArray();
    }

    private HttpResponse<String> postGzip(byte[] body) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(uri)
            .header("Content-Type", "application/json")
            .header("Content-Encoding", "gzip")
            .POST(BodyPublishers.ofByteArray(gzip(body)))
            .build();
        return httpClient.send(request, BodyHandlers.ofString());
    }

    @Test
    public void test_gzip_request_expanding_beyond_the_in_flight_requests_breaker_is_rejected() throws Exception {
        CircuitBreaker breaker = cluster().getInstance(CircuitBreakerService.class)
            .getBreaker(CircuitBreaker.IN_FLIGHT_REQUESTS);
        long usedBefore = breaker.getUsed();

        // Below http.max_content_length, but expands beyond the breaker's limit
        byte[] spaces = new byte[20 * 1024 * 1024];
        Arrays.fill(spaces, (byte) ' ');
        HttpResponse<String> response = postGzip(spaces);

        assertThat(response.statusCode()).isEqualTo(429);
        assertThat(response.body()).contains("http_request");
        assertBusy(() -> assertThat(breaker.getUsed()).isEqualTo(usedBefore));

        // The node is still serving requests, gzip encoded ones included
        response = postGzip("{\"stmt\": \"select 1\"}".getBytes(StandardCharsets.UTF_8));
        assertThat(response.statusCode()).isEqualTo(200);
        assertBusy(() -> assertThat(breaker.getUsed()).isEqualTo(usedBefore));
    }
}
