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

package io.crate.jobs;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.concurrent.CompletionState;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

@ParametersAreNonnullByDefault
public class SubExecutionContextFuture implements ListenableFuture<CompletionState> {

    private final SettableFuture<CompletionState> internalFuture = SettableFuture.create();
    private final AtomicBoolean closeCalled = new AtomicBoolean(false);
    private final CompletionState state = new CompletionState();

    /**
     * @return true if this is the first call to this method
     */
    boolean firstClose() {
        return !closeCalled.getAndSet(true);
    }

    /**
     * @return true if in the process of closing or done
     */
    public synchronized boolean closed() {
        return closeCalled.get() || internalFuture.isDone();
    }

    public void bytesUsed(long bytes) {
        state.bytesUsed(bytes);
    }

    public boolean close(@Nullable Throwable t) {
        if (t == null) {
            return internalFuture.set(state);
        }  else {
            return internalFuture.setException(t);
        }
    }

    public void addCallback(FutureCallback<? super CompletionState> callback) {
        Futures.addCallback(internalFuture, callback);
    }

    @Override
    public CompletionState get() throws InterruptedException, ExecutionException {
        return internalFuture.get();
    }

    @Override
    public CompletionState get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return internalFuture.get(timeout, unit);
    }

    @Override
    public void addListener(Runnable listener, Executor executor) {
        internalFuture.addListener(listener, executor);
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return internalFuture.cancel(mayInterruptIfRunning);
    }

    @Override
    public boolean isCancelled() {
        return internalFuture.isCancelled();
    }

    @Override
    public boolean isDone() {
        return closed();
    }
}
