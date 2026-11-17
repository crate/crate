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

import io.crate.data.breaker.RamAccounting;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.WrappedByteBuf;

/// Releases the {@link RamAccounting} of a buffer once the buffer is deallocated.
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

    /// Shared by the root buffer and all buffers derived from it.
    private record Accounting(ByteBuf root, RamAccounting ramAccounting) {

        private void maybeRelease() {
            if (root.refCnt() == 0) {
                // Safe to call multiple times, prevents from accounting bytes after release.
                ramAccounting.close();
            }
        }
    }

    /// @param ramAccounting already accounts the capacity of {@code buf}
    public AccountingWrappedByteBuf(ByteBuf buf, RamAccounting ramAccounting) {
        this(buf, new Accounting(buf, ramAccounting));
    }

    private AccountingWrappedByteBuf(ByteBuf buf, Accounting accounting) {
        super(buf);
        this.accounting = accounting;
    }

    /// Used to account additional bytes, e.g. after the buffer grew
    RamAccounting ramAccounting() {
        return accounting.ramAccounting();
    }

    private ByteBuf derived(ByteBuf derived) {
        return new AccountingWrappedByteBuf(derived, accounting);
    }

    @Override
    public boolean release() {
        boolean released = super.release();
        accounting.maybeRelease();
        return released;
    }

    @Override
    public boolean release(int decrement) {
        boolean released = super.release(decrement);
        accounting.maybeRelease();
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
