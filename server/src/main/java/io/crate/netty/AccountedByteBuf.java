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

package io.crate.netty;

import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.monitor.jvm.JvmInfo;

import io.crate.data.breaker.RamAccounting;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.WrappedByteBuf;

public class AccountedByteBuf {

    public static final long SINGLE_DIRECT_BUF_LIMIT = (long) (JvmInfo.jvmInfo().getDirectMemorySize() * 0.60);

    private AccountedByteBuf() {}

    public static ByteBuf of(ByteBuf delegate, RamAccounting ramAccounting) {
        if (delegate.isDirect()) {
            return new AccountedDirectByteBuf(delegate);
        }
        return new AccountedHeapByteBuf(delegate, ramAccounting);
    }

    static class AccountedHeapByteBuf extends WrappedByteBuf {
        private final RamAccounting accounting;

        protected AccountedHeapByteBuf(ByteBuf buf, RamAccounting accounting) {
            super(buf);
            this.accounting = accounting;
        }

        @Override
        public ByteBuf writeBytes(byte[] src, int srcIndex, int length) {
            int oldCapacity = buf.capacity();
            ByteBuf result = buf.writeBytes(src, srcIndex, length);
            int newCapacity = buf.capacity();
            int delta = newCapacity - oldCapacity;
            accounting.addBytes(delta);
            return result;
        }
    }

    /**
     * Doesn't allow a single buffer to grow more than 60% of total direct memory.
     * We can't use ByteBufAllocatorMetric.usedDirectMemory() to check total direct memory usage
     * as it doesn't return precise results for big allocations (big result sets), see https://github.com/netty/netty/issues/16068.
     */
    static class AccountedDirectByteBuf extends WrappedByteBuf {
        private long usedBytes = 0;

        protected AccountedDirectByteBuf(ByteBuf buf) {
            super(buf);
        }

        @Override
        public ByteBuf writeBytes(byte[] src, int srcIndex, int length) {
            if (usedBytes + length > SINGLE_DIRECT_BUF_LIMIT) {
                throw new CircuitBreakingException(length, usedBytes + length, SINGLE_DIRECT_BUF_LIMIT, "result-buffer");
            }
            try {
                usedBytes += length;
                return buf.writeBytes(src, srcIndex, length);
            } catch (OutOfMemoryError error) {
                // writeBytes call will not leave a buffer in an inconsistent state (see issue linked above).
                // It's safe to catch OOM as a last resort and re-throw it as a CBE.
                throw new CircuitBreakingException(length, usedBytes + length, SINGLE_DIRECT_BUF_LIMIT, "result-buffer");
            }
        }
    }
}
