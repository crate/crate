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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.GZIPOutputStream;

import org.elasticsearch.common.breaker.ChildMemoryCircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.indices.breaker.BreakerSettings;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.junit.After;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.PrematureChannelClosureException;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;

public class AccountingHttpContentDecompressorTest {

    private static final int DECOMPRESSED_SIZE = 256 * 1024;

    private ChildMemoryCircuitBreaker breaker;
    private EmbeddedChannel channel;

    private void setUp() {
        setUp(-1);
    }

    private void setUp(long limit) {
        breaker = new ChildMemoryCircuitBreaker(
            new BreakerSettings("in_flight_requests", limit),
            new NoneCircuitBreakerService()
        );
        channel = newChannel();
    }

    private EmbeddedChannel newChannel() {
        return new EmbeddedChannel(
            new AccountingHttpContentDecompressor(() -> breaker),
            new CircuitBreakingRejectionHandler(),
            new HttpObjectAggregator(Integer.MAX_VALUE)
        );
    }

    @After
    public void releaseChannel() {
        if (channel != null) {
            channel.finishAndReleaseAll();
        }
    }

    private static byte[] spaces(int size) {
        byte[] data = new byte[size];
        Arrays.fill(data, (byte) ' ');
        return data;
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

    private static HttpRequest request(CharSequence contentEncoding) {
        HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/_sql");
        if (contentEncoding != null) {
            request.headers().set(HttpHeaderNames.CONTENT_ENCODING, contentEncoding);
        }
        return request;
    }

    /// Splits the content into chunks, like the HttpRequestDecoder does.
    private static HttpContent[] chunks(byte[] content, int numChunks) {
        HttpContent[] chunks = new HttpContent[numChunks];
        int chunkSize = (content.length + numChunks - 1) / numChunks;
        for (int i = 0; i < numChunks; i++) {
            int from = Math.min(i * chunkSize, content.length);
            int to = Math.min(from + chunkSize, content.length);
            ByteBuf buf = Unpooled.copiedBuffer(content, from, to - from);
            chunks[i] = i == numChunks - 1 ? new DefaultLastHttpContent(buf) : new DefaultHttpContent(buf);
        }
        return chunks;
    }

    private void assertAccountedUntilReleased(CharSequence contentEncoding, byte[] content) {
        channel.writeInbound(request(contentEncoding));
        channel.writeInbound((Object[]) chunks(content, 4));

        FullHttpRequest aggregated = channel.readInbound();
        assertThat(aggregated.content().readableBytes()).isEqualTo(DECOMPRESSED_SIZE);
        assertThat(breaker.getUsed()).isGreaterThanOrEqualTo(DECOMPRESSED_SIZE);

        aggregated.release();
        assertThat(breaker.getUsed()).isEqualTo(0L);
    }

    @Test
    public void test_gzip_content_is_accounted_until_the_aggregated_request_is_released() throws Exception {
        setUp();
        assertAccountedUntilReleased(HttpHeaderValues.GZIP, gzip(spaces(DECOMPRESSED_SIZE)));
    }

    @Test
    public void test_x_gzip_content_is_accounted_until_the_aggregated_request_is_released() throws Exception {
        setUp();
        assertAccountedUntilReleased(HttpHeaderValues.X_GZIP, gzip(spaces(DECOMPRESSED_SIZE)));
    }

    @Test
    public void test_deflate_content_is_accounted_until_the_aggregated_request_is_released() throws Exception {
        setUp();
        assertAccountedUntilReleased(HttpHeaderValues.DEFLATE, deflate(spaces(DECOMPRESSED_SIZE)));
    }

    @Test
    public void test_x_deflate_content_is_accounted_until_the_aggregated_request_is_released() throws Exception {
        setUp();
        assertAccountedUntilReleased(HttpHeaderValues.X_DEFLATE, deflate(spaces(DECOMPRESSED_SIZE)));
    }

    @Test
    public void test_concurrent_requests_trip_on_their_combined_usage_and_only_the_tripping_one_is_rejected()
            throws Exception {
        // Each request fits on its own, two in flight at the same time don't
        setUp(DECOMPRESSED_SIZE * 3 / 2);
        HttpContent[] first = chunks(gzip(spaces(DECOMPRESSED_SIZE)), 4);
        HttpContent[] second = chunks(gzip(spaces(DECOMPRESSED_SIZE)), 4);
        EmbeddedChannel otherChannel = newChannel();
        try {
            // The aggregator holds the decompressed content of the first three chunks
            channel.writeInbound(request(HttpHeaderValues.GZIP));
            channel.writeInbound(first[0], first[1], first[2]);
            long usedByFirst = breaker.getUsed();
            assertThat(usedByFirst).isGreaterThanOrEqualTo(DECOMPRESSED_SIZE / 2);

            otherChannel.writeInbound(request(HttpHeaderValues.GZIP));
            otherChannel.writeInbound((Object[]) second);
            assertThatThrownBy(otherChannel::checkException)
                .isInstanceOf(PrematureChannelClosureException.class);
            FullHttpResponse response = otherChannel.readOutbound();
            assertThat(response.status()).isEqualTo(HttpResponseStatus.TOO_MANY_REQUESTS);
            response.release();
            assertThat(otherChannel.isOpen()).isFalse();
            for (HttpContent chunk : second) {
                assertThat(chunk.refCnt()).isEqualTo(0);
            }
            // Only the bytes of the rejected request are returned
            assertThat(breaker.getUsed()).isEqualTo(usedByFirst);

            channel.writeInbound(first[3]);
            FullHttpRequest aggregated = channel.readInbound();
            assertThat(aggregated.content().readableBytes()).isEqualTo(DECOMPRESSED_SIZE);
            assertThat(channel.isOpen()).isTrue();
            aggregated.release();
            assertThat(breaker.getUsed()).isEqualTo(0L);
        } finally {
            otherChannel.finishAndReleaseAll();
        }
    }

    @Test
    public void test_identity_content_is_not_accounted() {
        setUp();
        channel.writeInbound(request(null));
        channel.writeInbound((Object[]) chunks("{\"stmt\": \"select 1\"}".getBytes(StandardCharsets.UTF_8), 2));

        FullHttpRequest aggregated = channel.readInbound();
        assertThat(aggregated.content().toString(StandardCharsets.UTF_8)).isEqualTo("{\"stmt\": \"select 1\"}");
        assertThat(breaker.getUsed()).isEqualTo(0L);
        aggregated.release();
    }

    @Test
    public void test_request_is_rejected_and_the_channel_closed_if_the_breaker_trips() throws Exception {
        setUp(DECOMPRESSED_SIZE / 4);
        channel.writeInbound(request(HttpHeaderValues.GZIP));
        // Chunks after the trip are discarded. Closing the channel makes the aggregator
        // release the partial content and raise an exception, which EmbeddedChannel records.
        HttpContent[] chunks = chunks(gzip(spaces(DECOMPRESSED_SIZE)), 8);
        channel.writeInbound((Object[]) chunks);
        assertThatThrownBy(channel::checkException)
            .isInstanceOf(PrematureChannelClosureException.class);
        for (HttpContent chunk : chunks) {
            assertThat(chunk.refCnt()).isEqualTo(0);
        }

        FullHttpResponse response = channel.readOutbound();
        assertThat(response.status()).isEqualTo(HttpResponseStatus.TOO_MANY_REQUESTS);
        assertThat(response.headers().get(HttpHeaderNames.CONNECTION)).isEqualTo(HttpHeaderValues.CLOSE.toString());
        assertThat(response.content().toString(StandardCharsets.UTF_8)).contains(AccountingZlibDecoder.LABEL);
        response.release();
        assertThat((Object) channel.readOutbound()).isNull();

        assertThat(channel.isOpen()).isFalse();
        assertThat((Object) channel.readInbound()).isNull();
        assertThat(breaker.getUsed()).isEqualTo(0L);
    }

    @Test
    public void test_other_exceptions_are_propagated() {
        setUp();
        channel.pipeline().fireExceptionCaught(new DecoderException(new IllegalStateException("dummy")));

        assertThatThrownBy(channel::checkException)
            .isInstanceOf(DecoderException.class)
            .hasRootCauseInstanceOf(IllegalStateException.class);
        assertThat(channel.isOpen()).isTrue();
    }

    /// Holds back writes, so the channel isn't closed right after the rejection, like on a real socket.
    /// Run the returned writes to complete them.
    private List<Runnable> holdWrites() {
        List<Runnable> heldWrites = new ArrayList<>();
        channel.pipeline().addFirst(new ChannelOutboundHandlerAdapter() {
            @Override
            public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
                heldWrites.add(() -> ctx.writeAndFlush(msg, promise));
            }
        });
        return heldWrites;
    }

    @Test
    public void test_pipelined_request_after_a_rejected_one_is_discarded() throws Exception {
        setUp(DECOMPRESSED_SIZE / 4);
        List<Runnable> heldWrites = holdWrites();

        // A second request is already received, e.g. in the same read, when the first one trips the breaker
        HttpContent[] rejectedChunks = chunks(gzip(spaces(DECOMPRESSED_SIZE)), 4);
        HttpRequest pipelined = request(null);
        HttpContent pipelinedContent = new DefaultLastHttpContent(
            Unpooled.copiedBuffer("{\"stmt\": \"select 1\"}", StandardCharsets.UTF_8)
        );
        channel.writeInbound(request(HttpHeaderValues.GZIP));
        channel.writeInbound((Object[]) rejectedChunks);
        channel.writeInbound(pipelined, pipelinedContent);

        assertThat((Object) channel.readInbound()).isNull();
        assertThat(pipelinedContent.refCnt()).isEqualTo(0);
        assertThat(heldWrites).hasSize(1);

        heldWrites.forEach(Runnable::run);
        assertThatThrownBy(channel::checkException)
            .isInstanceOf(PrematureChannelClosureException.class);
        FullHttpResponse response = channel.readOutbound();
        assertThat(response.status()).isEqualTo(HttpResponseStatus.TOO_MANY_REQUESTS);
        response.release();
        // The pipelined request isn't answered, the client must retry it on a new connection
        assertThat((Object) channel.readOutbound()).isNull();
        assertThat(channel.isOpen()).isFalse();
        for (HttpContent chunk : rejectedChunks) {
            assertThat(chunk.refCnt()).isEqualTo(0);
        }
        assertThat(breaker.getUsed()).isEqualTo(0L);
        assertThat(breaker.getTrippedCount()).isEqualTo(1L);
    }

    @Test
    public void test_exceptions_after_a_rejection_are_not_propagated() {
        setUp();
        List<Runnable> heldWrites = holdWrites();
        channel.pipeline().fireExceptionCaught(new DecoderException(new CircuitBreakingException("dummy")));
        channel.pipeline().fireExceptionCaught(new DecoderException(new IllegalStateException("CRC mismatch")));
        channel.checkException();

        heldWrites.forEach(Runnable::run);
        assertThat(channel.isOpen()).isFalse();
        FullHttpResponse response = channel.readOutbound();
        assertThat(response.status()).isEqualTo(HttpResponseStatus.TOO_MANY_REQUESTS);
        response.release();
        assertThat((Object) channel.readOutbound()).isNull();
    }

    @Test
    public void test_circuit_breaking_exception_is_found_in_the_cause_chain() {
        setUp();
        channel.pipeline().fireExceptionCaught(
            new DecoderException(new DecoderException(new CircuitBreakingException("dummy")))
        );

        FullHttpResponse response = channel.readOutbound();
        assertThat(response.status()).isEqualTo(HttpResponseStatus.TOO_MANY_REQUESTS);
        response.release();
        assertThat(channel.isOpen()).isFalse();
    }
}
