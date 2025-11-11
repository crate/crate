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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import org.junit.Test;

import io.crate.exceptions.JobKilledException;

public class AsyncFnTaskTest {

    @Test
    public void test_start_runs_run_fn() throws Exception {
        Callable<CompletableFuture<Integer>> run = () -> CompletableFuture.completedFuture(1);
        AsyncFnTask<Integer> task = new AsyncFnTask<>(0, "dummy", run, _ -> {});

        assertThat(task.start()).as("task initialization itself is not async").isNull();
        assertThat(task.result()).isCompletedWithValue(1);
    }

    @Test
    public void test_task_kill_runs_kill() throws Exception {
        CompletableFuture<Integer> future = new CompletableFuture<>();
        Callable<CompletableFuture<Integer>> run = () -> future;
        Consumer<Throwable> kill = t -> future.completeExceptionally(t);
        AsyncFnTask<Integer> task = new AsyncFnTask<>(0, "dummy", run, kill);
        assertThat(task.start()).as("task initialization itself is not async").isNull();
        task.kill(JobKilledException.of("bad luck"));
        assertThat(task.result()).isCompletedExceptionally().withFailMessage("bad luck");
    }
}

