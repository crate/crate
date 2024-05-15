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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import org.elasticsearch.test.ESTestCase;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;

import io.crate.exceptions.JobKilledException;

public class AbstractTaskTest extends ESTestCase {

    private TestingTask testingTask;

    private Runnable killRunnable = new Runnable() {
        @Override
        public void run() {
            testingTask.kill(JobKilledException.of("dummy"));
        }
    };

    private Runnable closeRunnable = new Runnable() {
        @Override
        public void run() {
            testingTask.close();
        }
    };

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        testingTask = new TestingTask();
    }

    private void runAsync(Runnable task, int calls) {
        List<Thread> threads = new ArrayList<Thread>(calls);
        for (int i = 0; i < calls; i++) {
            Thread t = new Thread(task);
            t.start();
            threads.add(t);
        }
        for (Thread thread : threads) {
            try {
                thread.join(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static class TestingTask extends AbstractTask {

        final AtomicInteger numStart = new AtomicInteger();
        final AtomicInteger numClose = new AtomicInteger();
        final AtomicInteger numKill = new AtomicInteger();

        TestingTask(int id) {
            super(id);
        }

        public TestingTask() {
            this(0);
        }

        @Override
        public String name() {
            return getClass().getSimpleName();
        }

        @Override
        public long bytesUsed() {
            return -1;
        }

        @Override
        protected void innerClose() {
            numClose.incrementAndGet();
        }

        @Override
        protected void innerKill(@NotNull Throwable t) {
            numKill.incrementAndGet();
        }

        @Override
        protected CompletableFuture<Void> innerStart() {
            numStart.incrementAndGet();
            return null;
        }

        public List<Integer> stats() {
            return List.of(
                numStart.get(),
                numClose.get(),
                numKill.get()
            );
        }

    }

    @Test
    public void testNormalSequence() throws Exception {
        TestingTask task = new TestingTask();
        task.start();
        task.close();
        assertThat(task.stats()).containsExactly(1, 1, 0);
    }

    @Test
    public void testCloseAfterStart() throws Exception {
        TestingTask task = new TestingTask();
        task.close();
        task.start();
        task.close();
        assertThat(task.stats()).containsExactly(0, 1, 0);
    }

    @Test
    public void testParallelClose() throws Exception {
        testingTask.start();
        runAsync(closeRunnable, 3);
        assertThat(testingTask.stats()).containsExactly(1, 1, 0);
    }

    @Test
    public void testParallelKill() throws Exception {
        testingTask.start();
        runAsync(killRunnable, 3);
        assertThat(testingTask.stats()).containsExactly(1, 0, 1);
        assertThat(testingTask.numKill.get()).isGreaterThan(0);
    }

}
