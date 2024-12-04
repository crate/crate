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

package io.crate.execution.engine.indexing;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.IntStream;

import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.crate.data.BatchIterator;
import io.crate.data.InMemoryBatchIterator;
import io.crate.data.testing.BatchSimulatingIterator;

public class BatchIteratorBackpressureExecutorTest extends ESTestCase {

    private ExecutorService executor;
    private ScheduledExecutorService scheduler;

    @Before
    public void initExecutor() throws Exception {
        executor = Executors.newFixedThreadPool(4);
        scheduler = Executors.newSingleThreadScheduledExecutor();
    }

    @After
    public void shutdownExecutor() throws Exception {
        executor.shutdown();
        scheduler.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
        scheduler.awaitTermination(10, TimeUnit.SECONDS);
    }

    @Test
    public void testPauseOnFirstBatch() throws Exception {
        BatchIterator<Integer> numbersBi = InMemoryBatchIterator.of(() -> IntStream.range(0, 5).iterator(), -1, true);
        BatchSimulatingIterator<Integer> it = new BatchSimulatingIterator<>(numbersBi, 2, 5, executor);
        AtomicInteger numRows = new AtomicInteger(0);
        AtomicInteger numPauses = new AtomicInteger(0);
        Predicate<Integer> shouldPause = i -> {
            if (i == 0 && numPauses.get() == 0) {
                numPauses.incrementAndGet();
                return true;
            }
            return false;
        };
        BatchIteratorBackpressureExecutor<Integer, Integer> executor = new BatchIteratorBackpressureExecutor<>(
            UUID.randomUUID(),
            scheduler,
            this.executor,
            it,
            i -> CompletableFuture.supplyAsync(numRows::incrementAndGet, this.executor),
            (a, b) -> a + b,
            0,
            shouldPause,
            null,
            null,
            ignored -> 1L
        );
        CompletableFuture<Integer> result = executor.consumeIteratorAndExecute();
        result.get(10, TimeUnit.SECONDS);

        assertThat(numPauses).hasValue(1);
    }
}
