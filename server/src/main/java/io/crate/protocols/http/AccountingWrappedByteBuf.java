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

import java.nio.ByteOrder;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import org.elasticsearch.common.breaker.CircuitBreaker;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.WrappedByteBuf;

/// Returns the bytes accounted for a buffer to the circuit breaker once the buffer is deallocated.
///
/// - Derived buffers (slices, duplicates, swapped or read-only views) are wrapped as well and share
///   the accounting, so the bytes are returned no matter through which view the last reference is released.
/// - Retained derived buffers of pooled buffers have their own reference count. Releasing one to 0 drops
///   a single reference of the underlying buffer, so the bytes are returned only once the root buffer's
///   reference count reaches 0.
/// - {@link io.netty.buffer.CompositeByteBuf} consolidation copies the components into a new, unaccounted
///   buffer and releases the components, which returns their bytes. The copy is bounded by
///   {@code http.max_content_length} like any uncompressed request.
public class AccountingWrappedByteBuf extends WrappedByteBuf {

    private final Accounting accounting;

    private static final class Accounting {

        private final ByteBuf root;
        private final Supplier<CircuitBreaker> circuitBreaker;
        private final AtomicBoolean returned = new AtomicBoolean(false);

        /// Only increased on the event loop, but the buffer can be released on another thread.
        private volatile long bytes;

        private Accounting(ByteBuf root, Supplier<CircuitBreaker> circuitBreaker, long bytes) {
            this.root = root;
            this.circuitBreaker = circuitBreaker;
            this.bytes = bytes;
        }

        private void maybeReturn() {
            if (root.refCnt() == 0 && returned.compareAndSet(false, true)) {
                circuitBreaker.get().addWithoutBreaking(-bytes);
            }
        }
    }

    /// @param bytes already reserved on the circuit breaker for {@code buf}
    public AccountingWrappedByteBuf(ByteBuf buf, Supplier<CircuitBreaker> circuitBreaker, long bytes) {
        this(buf, new Accounting(buf, circuitBreaker, bytes));
    }

    private AccountingWrappedByteBuf(ByteBuf buf, Accounting accounting) {
        super(buf);
        this.accounting = accounting;
    }

    /// @param bytes additionally reserved on the circuit breaker, e.g. after the buffer grew
    void addAccounted(long bytes) {
        accounting.bytes += bytes;
    }

    private ByteBuf derived(ByteBuf derived) {
        return new AccountingWrappedByteBuf(derived, accounting);
    }

    @Override
    public boolean release() {
        boolean released = super.release();
        accounting.maybeReturn();
        return released;
    }

    @Override
    public boolean release(int decrement) {
        boolean released = super.release(decrement);
        accounting.maybeReturn();
        return released;
    }

    @Override
    public ByteBuf order(ByteOrder endianness) {
        // CompositeByteBuf keeps and releases `buf.order(BIG_ENDIAN)`, so this must not expose the wrapped buffer
        if (endianness == order()) {
            return this;
        }
        return derived(super.order(endianness));
    }

    @Override
    public ByteBuf slice() {
        return derived(super.slice());
    }

    @Override
    public ByteBuf slice(int index, int length) {
        return derived(super.slice(index, length));
    }

    @Override
    public ByteBuf readSlice(int length) {
        return derived(super.readSlice(length));
    }

    @Override
    public ByteBuf retainedSlice() {
        return derived(super.retainedSlice());
    }

    @Override
    public ByteBuf retainedSlice(int index, int length) {
        return derived(super.retainedSlice(index, length));
    }

    @Override
    public ByteBuf readRetainedSlice(int length) {
        return derived(super.readRetainedSlice(length));
    }

    @Override
    public ByteBuf duplicate() {
        return derived(super.duplicate());
    }

    @Override
    public ByteBuf retainedDuplicate() {
        return derived(super.retainedDuplicate());
    }

    @Override
    public ByteBuf asReadOnly() {
        return derived(super.asReadOnly());
    }
}
