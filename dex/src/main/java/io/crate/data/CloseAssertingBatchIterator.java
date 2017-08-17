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

import io.crate.concurrent.CompletableFutures;
import io.crate.exceptions.Exceptions;

import javax.annotation.Nonnull;
import java.util.concurrent.CompletionStage;

public class CloseAssertingBatchIterator<T> implements BatchIterator<T> {

    private final BatchIterator<T> delegate;
    private boolean closed = false;
    private volatile Throwable killed = null;

    public CloseAssertingBatchIterator(BatchIterator<T> delegate) {
        this.delegate = delegate;
    }

    @Override
    public T currentElement() {
        return delegate.currentElement();
    }

    @Override
    public void moveToStart() {
        raiseIfClosedOrKilled();
        delegate.moveToStart();
    }

    @Override
    public boolean moveNext() {
        raiseIfClosedOrKilled();
        return delegate.moveNext();
    }

    @Override
    public void close() {
        closed = true;
        delegate.close();
    }

    @Override
    public CompletionStage<?> loadNextBatch() {
        if (closed) {
            return CompletableFutures.failedFuture(new IllegalStateException("Iterator is closed"));
        }
        return delegate.loadNextBatch();
    }

    @Override
    public boolean allLoaded() {
        return delegate.allLoaded();
    }

    private void raiseIfClosedOrKilled() {
        if (killed != null) {
            Exceptions.rethrowUnchecked(killed);
        }
        if (closed) {
            throw new IllegalStateException("Iterator is closed");
        }
    }

    @Override
    public String toString() {
        return "Closing{" + delegate + '}';
    }

    @Override
    public void kill(@Nonnull Throwable throwable) {
        delegate.kill(throwable);
        killed = throwable;
    }
}
