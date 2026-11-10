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

import java.nio.ByteOrder;

import org.elasticsearch.common.breaker.ChildMemoryCircuitBreaker;
import org.elasticsearch.indices.breaker.BreakerSettings;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.junit.Before;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;

public class AccountingWrappedByteBufTest {

    private static final PooledByteBufAllocator ALLOC = PooledByteBufAllocator.DEFAULT;

    private ChildMemoryCircuitBreaker breaker;

    @Before
    public void setUpBreaker() {
        breaker = new ChildMemoryCircuitBreaker(
            new BreakerSettings("in_flight_requests", -1),
            new NoneCircuitBreakerService()
        );
    }

    private ByteBuf accounted(int size) {
        ByteBuf buf = ALLOC.heapBuffer(size);
        buf.writeZero(size);
        breaker.addEstimateBytesAndMaybeBreak(buf.capacity(), AccountingZlibDecoder.LABEL);
        return new AccountingWrappedByteBuf(buf, () -> breaker, buf.capacity());
    }

    @Test
    public void test_bytes_are_returned_once_the_last_reference_is_released() {
        ByteBuf buf = accounted(100);
        buf.retain();

        assertThat(buf.release()).isFalse();
        assertThat(breaker.getUsed()).isEqualTo(100L);
        assertThat(buf.release()).isTrue();
        assertThat(breaker.getUsed()).isEqualTo(0L);
    }

    @Test
    public void test_additionally_accounted_bytes_are_returned() {
        ByteBuf buf = accounted(100);
        breaker.addEstimateBytesAndMaybeBreak(50, AccountingZlibDecoder.LABEL);
        ((AccountingWrappedByteBuf) buf).addAccounted(50);

        buf.release();
        assertThat(breaker.getUsed()).isEqualTo(0L);
    }

    @Test
    public void test_release_decrement() {
        ByteBuf buf = accounted(100);
        buf.retain(2);

        assertThat(buf.release(2)).isFalse();
        assertThat(breaker.getUsed()).isEqualTo(100L);
        assertThat(buf.release(1)).isTrue();
        assertThat(breaker.getUsed()).isEqualTo(0L);
    }

    @Test
    public void test_bytes_are_returned_when_released_through_a_composite() {
        CompositeByteBuf composite = ALLOC.compositeBuffer(Integer.MAX_VALUE);
        composite.addComponent(true, accounted(100));
        composite.addComponent(true, accounted(100));
        assertThat(breaker.getUsed()).isEqualTo(200L);

        composite.release();
        assertThat(breaker.getUsed()).isEqualTo(0L);
    }

    @Test
    public void test_composite_consolidation_returns_the_bytes_exactly_once() {
        CompositeByteBuf composite = ALLOC.compositeBuffer(2);
        composite.addComponent(true, accounted(100));
        composite.addComponent(true, accounted(100));
        composite.addComponent(true, accounted(100));
        // Exceeding the max number of components copies the components into a new buffer and releases them
        assertThat(breaker.getUsed()).isEqualTo(0L);

        composite.release();
        assertThat(breaker.getUsed()).isEqualTo(0L);
    }

    @Test
    public void test_bytes_are_returned_when_a_retained_slice_is_released_last() {
        ByteBuf buf = accounted(100);
        ByteBuf slice = buf.retainedSlice(0, 10);
        ByteBuf readSlice = buf.readRetainedSlice(10);

        buf.release();
        assertThat(breaker.getUsed()).isEqualTo(100L);
        slice.release();
        assertThat(breaker.getUsed()).isEqualTo(100L);
        readSlice.release();
        assertThat(breaker.getUsed()).isEqualTo(0L);
    }

    @Test
    public void test_bytes_are_returned_when_a_retained_duplicate_is_released_last() {
        ByteBuf buf = accounted(100);
        ByteBuf duplicate = buf.retainedDuplicate();

        buf.release();
        assertThat(breaker.getUsed()).isEqualTo(100L);
        duplicate.release();
        assertThat(breaker.getUsed()).isEqualTo(0L);
    }

    @Test
    public void test_bytes_are_returned_when_released_through_non_retained_views() {
        for (int i = 0; i < 5; i++) {
            ByteBuf buf = accounted(100);
            ByteBuf view = switch (i) {
                case 0 -> buf.slice();
                case 1 -> buf.slice(0, 10);
                case 2 -> buf.readSlice(10);
                case 3 -> buf.duplicate();
                default -> buf.asReadOnly();
            };
            view.release();
            assertThat(breaker.getUsed()).as("view %d", i).isEqualTo(0L);
        }
    }

    @Test
    public void test_same_order_returns_the_wrapper_and_swapped_order_stays_accounted() {
        ByteBuf buf = accounted(100);
        assertThat(buf.order(ByteOrder.BIG_ENDIAN)).isSameAs(buf);

        ByteBuf swapped = buf.order(ByteOrder.LITTLE_ENDIAN);
        assertThat(swapped).isInstanceOf(AccountingWrappedByteBuf.class);
        swapped.release();
        assertThat(breaker.getUsed()).isEqualTo(0L);
    }

    @Test
    public void test_derived_buffers_of_derived_buffers_share_the_accounting() {
        ByteBuf buf = accounted(100);
        ByteBuf slice = buf.retainedSlice();
        ByteBuf nested = slice.retainedDuplicate();

        buf.release();
        assertThat(breaker.getUsed()).isEqualTo(100L);
        nested.release();
        // The retained slice in between still holds a reference
        assertThat(breaker.getUsed()).isEqualTo(100L);
        slice.release();
        assertThat(breaker.getUsed()).isEqualTo(0L);
    }
}
