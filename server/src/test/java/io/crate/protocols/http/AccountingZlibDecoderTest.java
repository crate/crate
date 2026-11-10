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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.GZIPOutputStream;

import org.elasticsearch.common.breaker.ChildMemoryCircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.indices.breaker.BreakerSettings;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.junit.After;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.compression.ZlibWrapper;

public class AccountingZlibDecoderTest {

    private static final int DECOMPRESSED_SIZE = 256 * 1024;

    private ChildMemoryCircuitBreaker breaker;
    private EmbeddedChannel channel;

    private void setUp(ZlibWrapper wrapper, long limit) {
        breaker = new ChildMemoryCircuitBreaker(
            new BreakerSettings(AccountingZlibDecoder.LABEL, limit),
            new NoneCircuitBreakerService()
        );
        channel = new EmbeddedChannel(new AccountingZlibDecoder(wrapper, () -> breaker));
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

    /// Collects all decompressed buffers into a composite, like {@code HttpObjectAggregator} does.
    private CompositeByteBuf aggregateInbound() {
        CompositeByteBuf composite = channel.alloc().compositeBuffer(Integer.MAX_VALUE);
        ByteBuf buf;
        while ((buf = channel.readInbound()) != null) {
            composite.addComponent(true, buf);
        }
        return composite;
    }

    @Test
    public void test_decompressed_content_is_accounted_until_released_through_a_composite() throws Exception {
        setUp(ZlibWrapper.GZIP, -1);
        channel.writeInbound(Unpooled.wrappedBuffer(gzip(spaces(DECOMPRESSED_SIZE))));

        CompositeByteBuf composite = aggregateInbound();
        assertThat(composite.readableBytes()).isEqualTo(DECOMPRESSED_SIZE);
        assertThat(breaker.getUsed()).isGreaterThanOrEqualTo(DECOMPRESSED_SIZE);

        composite.release();
        assertThat(breaker.getUsed()).isEqualTo(0L);
    }

    @Test
    public void test_deflate_content_is_accounted_until_released() throws Exception {
        setUp(ZlibWrapper.ZLIB_OR_NONE, -1);
        channel.writeInbound(Unpooled.wrappedBuffer(deflate(spaces(DECOMPRESSED_SIZE))));

        CompositeByteBuf composite = aggregateInbound();
        assertThat(composite.readableBytes()).isEqualTo(DECOMPRESSED_SIZE);
        assertThat(breaker.getUsed()).isGreaterThanOrEqualTo(DECOMPRESSED_SIZE);

        composite.release();
        assertThat(breaker.getUsed()).isEqualTo(0L);
    }

    @Test
    public void test_concatenated_gzip_members_are_fully_decompressed() throws Exception {
        setUp(ZlibWrapper.GZIP, -1);
        byte[] member = gzip(spaces(1024));
        byte[] twoMembers = new byte[member.length * 2];
        System.arraycopy(member, 0, twoMembers, 0, member.length);
        System.arraycopy(member, 0, twoMembers, member.length, member.length);
        channel.writeInbound(Unpooled.wrappedBuffer(twoMembers));

        CompositeByteBuf composite = aggregateInbound();
        assertThat(composite.readableBytes()).isEqualTo(2048);

        composite.release();
        assertThat(breaker.getUsed()).isEqualTo(0L);
    }

    @Test
    public void test_accounted_bytes_are_the_capacity_of_the_decompressed_buffers() throws Exception {
        setUp(ZlibWrapper.GZIP, -1);
        // Compresses about 4:1, so the decompressed buffers are forwarded partially filled
        byte[] whitespace = {' ', '\t', '\n', '\r'};
        byte[] data = new byte[DECOMPRESSED_SIZE];
        Random random = new Random(42);
        for (int i = 0; i < data.length; i++) {
            data[i] = whitespace[random.nextInt(whitespace.length)];
        }
        byte[] compressed = gzip(data);
        // Chunks of http.max_chunk_size, like the HttpRequestDecoder produces
        for (int offset = 0; offset < compressed.length; offset += 8192) {
            int length = Math.min(8192, compressed.length - offset);
            channel.writeInbound(Unpooled.copiedBuffer(compressed, offset, length));
        }

        List<ByteBuf> buffers = new ArrayList<>();
        ByteBuf buf;
        while ((buf = channel.readInbound()) != null) {
            buffers.add(buf);
        }
        long readable = buffers.stream().mapToLong(ByteBuf::readableBytes).sum();
        long capacity = buffers.stream().mapToLong(ByteBuf::capacity).sum();
        assertThat(readable).isEqualTo(DECOMPRESSED_SIZE);
        assertThat(breaker.getUsed()).isEqualTo(capacity);
        // The overhead of partially filled buffers is bounded
        assertThat(capacity).isLessThan(readable * 3 / 2);

        buffers.forEach(ByteBuf::release);
        assertThat(breaker.getUsed()).isEqualTo(0L);
    }

    @Test
    public void test_breaker_trip_is_propagated_and_nothing_stays_accounted() throws Exception {
        setUp(ZlibWrapper.GZIP, DECOMPRESSED_SIZE / 4);
        byte[] compressed = gzip(spaces(DECOMPRESSED_SIZE));
        int half = compressed.length / 2;
        ByteBuf firstHalf = Unpooled.copiedBuffer(compressed, 0, half);

        assertThatThrownBy(() -> channel.writeInbound(firstHalf))
            .rootCause()
            .isInstanceOf(CircuitBreakingException.class);
        assertThat(breaker.getTrippedCount()).isEqualTo(1L);
        releaseInbound();

        assertRemainingInputIsSkipped(Unpooled.copiedBuffer(compressed, half, compressed.length - half));
        assertThat(firstHalf.refCnt()).isEqualTo(0);
    }

    @Test
    public void test_breaker_trip_on_first_allocation_releases_the_buffer() throws Exception {
        // Limit below the first decompression buffer, so the very first allocation trips
        setUp(ZlibWrapper.GZIP, 1);
        byte[] compressed = gzip(spaces(1024));
        ByteBuf firstHalf = Unpooled.copiedBuffer(compressed, 0, compressed.length / 2);

        assertThatThrownBy(() -> channel.writeInbound(firstHalf))
            .rootCause()
            .isInstanceOf(CircuitBreakingException.class);
        assertThat(breaker.getTrippedCount()).isEqualTo(1L);
        assertThat((Object) channel.readInbound()).isNull();

        assertRemainingInputIsSkipped(
            Unpooled.copiedBuffer(compressed, compressed.length / 2, compressed.length - compressed.length / 2)
        );
        assertThat(firstHalf.refCnt()).isEqualTo(0);
    }

    private void releaseInbound() {
        ByteBuf buf;
        while ((buf = channel.readInbound()) != null) {
            buf.release();
        }
    }

    /// After a trip, neither the remaining input nor closing the channel decompresses anything or trips again.
    private void assertRemainingInputIsSkipped(ByteBuf remaining) {
        channel.writeInbound(remaining);
        assertThat(remaining.refCnt()).isEqualTo(0);
        assertThat((Object) channel.readInbound()).isNull();

        assertThat(channel.finishAndReleaseAll()).isFalse();
        channel = null;
        assertThat(breaker.getTrippedCount()).isEqualTo(1L);
        assertThat(breaker.getUsed()).isEqualTo(0L);
    }
}
