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

package io.crate.protocols.http;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;

import org.elasticsearch.common.breaker.ChildMemoryCircuitBreaker;
import org.elasticsearch.indices.breaker.BreakerSettings;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;

public class InFlightRequestsBreakerHandlerTest {

    private static final long LIMIT = 100;

    private ChildMemoryCircuitBreaker breaker;
    private EmbeddedChannel channel;

    @Before
    public void setUpChannel() {
        breaker = new ChildMemoryCircuitBreaker(
            new BreakerSettings("in_flight_requests", LIMIT),
            new NoneCircuitBreakerService()
        );
        channel = new EmbeddedChannel(new InFlightRequestsBreakerHandler(() -> breaker));
    }

    @After
    public void releaseChannel() {
        channel.finishAndReleaseAll();
    }

    private static HttpRequest request(String uri) {
        return new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, uri);
    }

    private static HttpContent content(int bytes) {
        return new DefaultHttpContent(buffer(bytes));
    }

    private static LastHttpContent lastContent(int bytes) {
        return new DefaultLastHttpContent(buffer(bytes));
    }

    private static ByteBuf buffer(int bytes) {
        return Unpooled.copiedBuffer(" ".repeat(bytes), StandardCharsets.UTF_8);
    }

    private static FullHttpResponse response(HttpResponseStatus status) {
        return new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status);
    }

    @Test
    public void test_content_is_accounted_until_the_response_is_written() {
        channel.writeInbound(request("/_sql"), content(10), lastContent(20));
        assertThat(breaker.getUsed()).isEqualTo(30L);

        channel.writeOutbound(response(HttpResponseStatus.OK));
        assertThat(breaker.getUsed()).isEqualTo(0L);
    }

    @Test
    public void test_chunked_response_releases_the_reservation_on_its_last_content() {
        channel.writeInbound(request("/_sql"), lastContent(30));

        channel.writeOutbound(new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK), content(5));
        assertThat(breaker.getUsed()).isEqualTo(30L);

        channel.writeOutbound(lastContent(5));
        assertThat(breaker.getUsed()).isEqualTo(0L);
    }

    @Test
    public void test_continue_response_does_not_release_the_reservation() {
        channel.writeInbound(request("/_sql"), content(30));

        channel.writeOutbound(response(HttpResponseStatus.CONTINUE));
        assertThat(breaker.getUsed()).isEqualTo(30L);

        channel.writeInbound(lastContent(10));
        assertThat(breaker.getUsed()).isEqualTo(40L);

        channel.writeOutbound(response(HttpResponseStatus.OK));
        assertThat(breaker.getUsed()).isEqualTo(0L);
    }

    @Test
    public void test_request_is_rejected_with_429_and_the_connection_is_closed_if_the_breaker_trips() {
        HttpContent first = content(60);
        HttpContent tripping = content(60);
        LastHttpContent last = lastContent(10);
        channel.writeInbound(request("/_sql"), first, tripping, last);

        assertThat((Object) channel.readInbound()).isInstanceOf(HttpRequest.class);
        assertThat((Object) channel.readInbound()).isSameAs(first);
        assertThat((Object) channel.readInbound()).isNull();
        assertThat(tripping.refCnt()).isEqualTo(0);
        assertThat(last.refCnt()).isEqualTo(0);
        first.release();

        FullHttpResponse response = channel.readOutbound();
        try {
            assertThat(response.status()).isEqualTo(HttpResponseStatus.TOO_MANY_REQUESTS);
            assertThat(response.content().toString(StandardCharsets.UTF_8)).contains(InFlightRequestsBreakerHandler.LABEL);
        } finally {
            response.release();
        }
        assertThat(channel.isOpen()).isFalse();
        assertThat(breaker.getUsed()).isEqualTo(0L);
        assertThat(breaker.getTrippedCount()).isEqualTo(1L);
    }

    @Test
    public void test_reservation_is_released_if_the_connection_is_closed_before_the_response() {
        channel.writeInbound(request("/_sql"), content(30));
        assertThat(breaker.getUsed()).isEqualTo(30L);

        channel.close();
        assertThat(breaker.getUsed()).isEqualTo(0L);
    }

    @Test
    public void test_content_received_after_an_early_response_is_not_accounted() {
        // e.g. the aggregator responds with 413 before the request is complete
        channel.writeInbound(request("/_sql"), content(30));
        channel.writeOutbound(response(HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE));
        assertThat(breaker.getUsed()).isEqualTo(0L);

        channel.writeInbound(content(30), lastContent(30));
        assertThat(breaker.getUsed()).isEqualTo(0L);
    }

    @Test
    public void test_blob_requests_are_not_accounted() {
        // Blob content is streamed to disk, it can exceed the breaker's limit
        channel.writeInbound(request("/_blobs/b1/" + "a".repeat(40)), content(80), lastContent(80));
        assertThat(breaker.getUsed()).isEqualTo(0L);
        assertThat(channel.isOpen()).isTrue();
    }

    @Test
    public void test_reservations_of_pipelined_requests_are_released_in_order() {
        channel.writeInbound(request("/_sql"), lastContent(30));
        channel.writeInbound(request("/_sql"), lastContent(20));
        assertThat(breaker.getUsed()).isEqualTo(50L);

        channel.writeOutbound(response(HttpResponseStatus.OK));
        assertThat(breaker.getUsed()).isEqualTo(20L);

        channel.writeOutbound(response(HttpResponseStatus.OK));
        assertThat(breaker.getUsed()).isEqualTo(0L);
    }
}
