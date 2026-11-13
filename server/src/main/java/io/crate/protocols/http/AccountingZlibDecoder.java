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

package io.crate.protocols.http;

import java.util.List;
import java.util.function.Supplier;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.compression.JdkZlibDecoder;
import io.netty.handler.codec.compression.ZlibWrapper;

/// A {@link JdkZlibDecoder} accounting the capacity of its decompression buffers in a circuit breaker.
///
/// Relies on these internals of Netty's {@link JdkZlibDecoder} (verified with Netty 4.2.20):
/// - {@link #prepareDecompressBuffer} is the only place allocating and growing the decompression buffer.
/// - With {@code maxAllocation == 0} the buffer is forwarded once it holds 64 KB, which bounds
///   the bytes allocated before they are accounted.
public class AccountingZlibDecoder extends JdkZlibDecoder {

    static final String LABEL = "http_request";

    private final Supplier<CircuitBreaker> circuitBreaker;
    private boolean tripped = false;

    public AccountingZlibDecoder(ZlibWrapper wrapper, Supplier<CircuitBreaker> circuitBreaker) {
        super(wrapper, true, 0);
        this.circuitBreaker = circuitBreaker;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        if (tripped) {
            in.skipBytes(in.readableBytes());
            return;
        }
        super.decode(ctx, in, out);
    }

    /// Both the initial allocation and every growth of the decompression buffer go through here.
    /// The buffer is released by its consumer, which returns the accounted bytes (see {@link AccountingWrappedByteBuf}).
    @Override
    protected ByteBuf prepareDecompressBuffer(ChannelHandlerContext ctx, ByteBuf buffer, int preferredSize) {
        if (buffer == null) {
            ByteBuf allocated = super.prepareDecompressBuffer(ctx, null, preferredSize);
            int bytes = allocated.capacity();
            try {
                addEstimateBytesAndMaybeBreak(bytes);
            } catch (CircuitBreakingException e) {
                // Nobody else owns the buffer yet
                allocated.release();
                throw e;
            }
            return new AccountingWrappedByteBuf(allocated, circuitBreaker, bytes);
        }

        // On growth, `buffer` is owned by the caller which releases it on failure, so it must not be released here.
        int beforeBytes = buffer.capacity();
        ByteBuf result = super.prepareDecompressBuffer(ctx, buffer, preferredSize);
        int deltaBytes = result.capacity() - beforeBytes;
        if (deltaBytes > 0) {
            addEstimateBytesAndMaybeBreak(deltaBytes);
            ((AccountingWrappedByteBuf) result).addAccounted(deltaBytes);
        }
        return result;
    }

    private void addEstimateBytesAndMaybeBreak(int bytes) {
        try {
            circuitBreaker.get().addEstimateBytesAndMaybeBreak(bytes, LABEL);
        } catch (CircuitBreakingException e) {
            tripped = true;
            throw e;
        }
    }
}
