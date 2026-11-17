/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.auth;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.SocketTimeoutException;
import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.auth0.jwk.JwkException;

public class CachingJwkProviderTest {

    @Test
    public void test_get_cache_control_max_age_from_request() throws Exception {
        var duration = CachingJwkProvider.parseCacheControl("max-age=1000");
        assertThat(duration).isEqualTo(Duration.ofSeconds(1000));

        duration = CachingJwkProvider.parseCacheControl("wrong");
        assertThat(duration).isEqualTo(Duration.ZERO);

        duration = CachingJwkProvider.parseCacheControl("max-age=-1");
        assertThat(duration).isEqualTo(Duration.ZERO);

        duration = CachingJwkProvider.parseCacheControl("max-age=1.1");
        assertThat(duration).isEqualTo(Duration.ZERO);

        duration = CachingJwkProvider.parseCacheControl("max-age=0");
        assertThat(duration).isEqualTo(Duration.ZERO);
    }

    @Test
    public void test_resolving_the_jwk_url_times_out_if_the_issuer_does_not_respond() throws Exception {
        try (var server = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            String issuer = "http://" + server.getInetAddress().getHostAddress() + ":" + server.getLocalPort();

            // Run in separate thread, so the test fails instead of hanging if there is no timeout
            Duration timeout = Duration.ofMillis(10);
            var provider = CompletableFuture.supplyAsync(() -> new CachingJwkProvider(issuer, Clock.systemUTC(), timeout));

            // Resolving the jwks url falls back to the issuer URL
            assertThat(provider).succeedsWithin(Duration.ofSeconds(1));
        }
    }

    @Test
    public void test_fetching_the_keys_times_out_if_the_issuer_does_not_respond() throws Exception {
        try (var server = new ServerSocket(0, 0, InetAddress.getLoopbackAddress())) {
            String issuer = "http://" + server.getInetAddress().getHostAddress() + ":" + server.getLocalPort();

            // Run in separate thread, so the test fails instead of hanging if there is no timeout
            Duration timeout = Duration.ofMillis(10);
            var keys = CompletableFuture.supplyAsync(() -> {
                var provider = new CachingJwkProvider(issuer, Clock.systemUTC(), timeout);
                try {
                    return provider.get("kid");
                } catch (JwkException e) {
                    throw new CompletionException(e);
                }
            });

            Throwable error = keys.handle((_, e) -> e).get(1, TimeUnit.SECONDS);
            // The keys are fetched from the issuer URL, as resolving the jwks url fell back to it
            assertThat(error)
                .hasMessageContaining("Cannot obtain jwks from url " + issuer)
                .hasRootCauseInstanceOf(SocketTimeoutException.class);
        }
    }
}
