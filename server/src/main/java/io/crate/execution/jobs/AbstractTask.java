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

package io.crate.execution.jobs;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

public abstract class AbstractTask implements Task {

    protected final int id;

    private final AtomicBoolean firstClose = new AtomicBoolean(false);
    private final CompletableFuture<Void> future = new CompletableFuture<>();

    protected AbstractTask(int id) {
        this.id = id;
    }

    public int id() {
        return id;
    }

    protected static BiConsumer<? super Object, Throwable> closeOrKill(AbstractTask t) {
        return (result, err) -> {
            if (err == null) {
                t.close();
            } else {
                t.kill(err);
            }
        };
    }

    protected CompletableFuture<Void> innerStart() {
        return null;
    }

    @Override
    public final CompletableFuture<Void> start() {
        if (!firstClose.get()) {
            try {
                return innerStart();
            } catch (Throwable t) {
                kill(t);
            }
        }
        return null;
    }

    protected void innerClose() {
    }

    protected void close() {
        if (firstClose.compareAndSet(false, true)) {
            try {
                innerClose();
                future.complete(null);
            } catch (Throwable t) {
                future.completeExceptionally(t);
            }
        }
    }

    protected void innerKill(Throwable t) {
    }

    @Override
    public final void kill(Throwable t) {
        if (firstClose.compareAndSet(false, true)) {
            try {
                innerKill(t);
            } finally {
                future.completeExceptionally(t);
            }
        }
    }

    @Override
    public CompletableFuture<Void> completionFuture() {
        return future;
    }

    protected synchronized boolean isClosed() {
        return firstClose.get() || future.isDone();
    }
}
