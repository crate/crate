/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.data;

import io.crate.exceptions.Exceptions;

import javax.annotation.Nonnull;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

import static java.util.concurrent.CompletableFuture.failedFuture;

public final class SupplierBatchIterator<T> implements BatchIterator<T> {

    private final Supplier<T> supplier;
    private final boolean involvesIO;
    private final Runnable resetSupplier;

    private boolean closed;
    private volatile Throwable killed;
    private T currentElement;

    public SupplierBatchIterator(Supplier<T> supplier, boolean involvesIO, Runnable resetSupplier) {
        this.supplier = supplier;
        this.involvesIO = involvesIO;
        this.resetSupplier = resetSupplier;
    }

    @Override
    public T currentElement() {
        return currentElement;
    }

    @Override
    public void moveToStart() {
        resetSupplier.run();
        currentElement = null;
    }

    @Override
    public boolean moveNext() {
        raiseIfKilledOrClosed();
        T t = supplier.get();
        if (t == null) {
            return false;
        }
        currentElement = t;
        return true;
    }

    private void raiseIfKilledOrClosed() {
        if (killed != null) {
            Exceptions.rethrowRuntimeException(killed);
        }
        if (closed) {
            throw new IllegalStateException("SupplierBatchIterator is closed");
        }
    }

    @Override
    public void close() {
        closed = true;
    }

    @Override
    public CompletionStage<?> loadNextBatch() {
        return failedFuture(new IllegalStateException("SupplierBatchIterator is already fully loaded"));
    }

    @Override
    public boolean allLoaded() {
        return true;
    }

    @Override
    public boolean involvesIO() {
        return involvesIO;
    }

    @Override
    public void kill(@Nonnull Throwable throwable) {
        killed = throwable;
    }
}
