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

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;


/// Task that wraps a single async operation and includes kill handling
public class AsyncFnTask<T> extends AbstractTask {

    private final String name;
    private final Callable<CompletableFuture<T>> run;
    private final Consumer<Throwable> kill;
    private final CompletableFuture<T> result = new CompletableFuture<>();

    /// @param kill function to interrupt the `run` operation.
    ///             Must honor the [io.crate.common.current.Killable] contract.
    public AsyncFnTask(int id,
                       String name,
                       Callable<CompletableFuture<T>> run,
                       Consumer<Throwable> kill) {
        super(0);
        this.name = name;
        this.run = run;
        this.kill = kill;
        this.result.whenComplete((_, _) -> {
            this.close();
        });
    }

    public CompletableFuture<T> result() {
        return result;
    }

    @Override
    protected CompletableFuture<Void> innerStart() {
        try {
            run.call().whenComplete((r, t) -> {
                if (t == null) {
                    result.complete(r);
                } else {
                    result.completeExceptionally(t);
                }
            });
        } catch (Throwable t) {
            result.completeExceptionally(t);
        }
        return null;
    }

    @Override
    protected void innerKill(Throwable t) {
        // `kill.accept(t)` ought to result in the completion of the `result` future
        kill.accept(t);
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public long bytesUsed() {
        return -1L;
    }
}
