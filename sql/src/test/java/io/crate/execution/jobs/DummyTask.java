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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

public class DummyTask implements Task {

    private final int id;
    private final CompletableFuture<CompletionState> future = new CompletableFuture<>();

    final AtomicInteger numKill = new AtomicInteger(0);

    public DummyTask() {
        this(1);
    }

    public DummyTask(int id) {
        this.id = id;
    }

    @Override
    public void start() {
        future.complete(new CompletionState(0));
    }

    @Override
    public String name() {
        return "dummy " + id();
    }

    @Override
    public int id() {
        return id;
    }

    @Override
    public CompletableFuture<CompletionState> completionFuture() {
        return future;
    }

    @Override
    public void kill(@Nonnull Throwable throwable) {
        numKill.incrementAndGet();
        future.completeExceptionally(throwable);
    }
}
