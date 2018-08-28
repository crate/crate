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

package io.crate.execution.jobs;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class AbstractTask implements Task {

    protected final int id;

    private final AtomicBoolean firstClose = new AtomicBoolean(false);
    private final CompletionState completionState = new CompletionState();
    private final CompletableFuture<CompletionState> future = new CompletableFuture<>();

    protected AbstractTask(int id) {
        this.id = id;
    }

    public int id() {
        return id;
    }

    protected void innerStart() {
    }

    protected void innerPrepare() throws Exception {
    }

    @Override
    public final void prepare() throws Exception {
        try {
            innerPrepare();
        } catch (Exception e) {
            close(e);
            throw e;
        }
    }

    @Override
    public final void start() {
        if (!firstClose.get()) {
            try {
                innerStart();
            } catch (Throwable t) {
                close(t);
            }
        }
    }

    protected void innerClose(@Nullable Throwable t) {
    }

    protected void close(@Nullable Throwable t) {
        if (firstClose.compareAndSet(false, true)) {
            try {
                innerClose(t);
            } catch (Throwable t2) {
                if (t == null) {
                    t = t2;
                }
            }
            completeFuture(t);
        }
    }

    private void completeFuture(@Nullable Throwable t) {
        if (t == null) {
            future.complete(completionState);
        } else {
            future.completeExceptionally(t);
        }
    }

    protected void innerKill(@Nonnull Throwable t) {
    }

    @Override
    public final void kill(@Nonnull Throwable t) {
        if (firstClose.compareAndSet(false, true)) {
            try {
                innerKill(t);
            } finally {
                future.completeExceptionally(t);
            }
        }
    }

    @Override
    public CompletableFuture<CompletionState> completionFuture() {
        return future;
    }

    protected void setBytesUsed(long bytesUsed) {
        completionState.bytesUsed(bytesUsed);
    }

    protected synchronized boolean isClosed() {
        return firstClose.get() || future.isDone();
    }
}
