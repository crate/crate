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

package io.crate.netty;

import static io.crate.netty.AccountedByteBuf.SINGLE_DIRECT_BUF_LIMIT;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;

import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.junit.Test;

import io.crate.data.breaker.RamAccounting;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.netty.buffer.ByteBuf;

public class AccountedByteBufTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void test_delegate_direct_buffer_throws_cbe_on_OOM() {
        // It's hard and expensive to trigger real OOM.
        // When not specified in JVM args, direct memory is similar to XMX (equal to Runtime.getRuntime().maxMemory()).
        // We would need to write small buffer to bypass SINGLE_DIRECT_BUF_LIMIT check
        // and also allocate many large direct buffers (concurrently to write) to imitate direct buffer saturation.
        // It's not feasible and can be flaky, hence using mocks.
        ByteBuf delegate = mock(ByteBuf.class);
        when(delegate.isDirect()).thenReturn(true);
        byte[] data = "test".getBytes(StandardCharsets.UTF_8);
        when(delegate.writeBytes(eq(data), eq(0), eq(data.length))).thenThrow(new OutOfMemoryError("dummy"));

        ByteBuf buf = AccountedByteBuf.of(delegate, RamAccounting.NO_ACCOUNTING);

        assertThatThrownBy(() -> buf.writeBytes(data, 0, data.length))
            .isExactlyInstanceOf(CircuitBreakingException.class);
    }

    @Test
    public void test_single_buffer_cant_exceed_given_threshold() {
        // When not specified in JVM args, direct memory is similar to XMX (equal to Runtime.getRuntime().maxMemory()).
        // Using mock to avoid allocating 60% of Runtime.getRuntime().maxMemory().
        // Writing bytes is not important for this test, it only checks increment of usedBytes + eventual trigger of CBE.
        ByteBuf delegate = mock(ByteBuf.class);
        when(delegate.isDirect()).thenReturn(true);

        ByteBuf buf = AccountedByteBuf.of(delegate, RamAccounting.NO_ACCOUNTING);

        // If CI runs on a large heap, 60% of heap/direct memory can be > Integer.MAX_VALUE
        // length in writeBytes is int, so we increment usedBytes to the point when it's about to reach the limit.
        int iters = (int) (SINGLE_DIRECT_BUF_LIMIT / Integer.MAX_VALUE);
        for (int i = 0; i < iters; i++) {
            // Cheap, writeBytes is not doing anything, only incrementing "written" bytes.
            buf.writeBytes(new byte[] {}, 0, Integer.MAX_VALUE);
        }

        int minToThrow = (int) (SINGLE_DIRECT_BUF_LIMIT % Integer.MAX_VALUE) + 1;
        assertThatThrownBy(() -> buf.writeBytes(new byte[] {}, 0, minToThrow))
            .isExactlyInstanceOf(CircuitBreakingException.class);
    }

}
