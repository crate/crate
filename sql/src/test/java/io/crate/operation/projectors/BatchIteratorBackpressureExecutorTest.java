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

package io.crate.operation.projectors;

import io.crate.testing.BatchSimulatingIterator;
import io.crate.testing.TestingBatchIterators;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.common.unit.TimeValue;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.MatcherAssert.assertThat;

public class BatchIteratorBackpressureExecutorTest {

    private ExecutorService executor;
    private ScheduledExecutorService scheduler;

    @Before
    public void setUp() throws Exception {
        executor = Executors.newFixedThreadPool(4);
        scheduler = Executors.newSingleThreadScheduledExecutor();
    }

    @After
    public void tearDown() throws Exception {
        executor.shutdown();
        scheduler.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
        scheduler.awaitTermination(10, TimeUnit.SECONDS);
    }

    @Test
    public void testPauseOnFirstBatch() throws Exception {
        BatchSimulatingIterator it = new BatchSimulatingIterator(TestingBatchIterators.range(0, 10), 2, 5, executor);
        AtomicInteger numRows = new AtomicInteger(0);
        AtomicInteger numBatches = new AtomicInteger(0);
        AtomicInteger numPauses = new AtomicInteger(0);
        BatchIteratorBackpressureExecutor<Object> executor = new BatchIteratorBackpressureExecutor<>(
            it,
            scheduler,
            r -> numRows.incrementAndGet(),
            () -> CompletableFuture.supplyAsync(() -> {
                numBatches.incrementAndGet();
                int rowsReceived = numRows.getAndSet(0);
                assertThat("rowsReceived must not be larger than the bulkSize", rowsReceived, Matchers.lessThanOrEqualTo(3));
                return null;
            }, this.executor),
            () -> {
                if (numBatches.get() == 0 && numPauses.get() == 0) {
                    numPauses.incrementAndGet();
                    return true;
                }
                return false;
            },
            3,
            BackoffPolicy.exponentialBackoff(TimeValue.timeValueNanos(10), 1000)
        );
        CompletableFuture<Void> result = executor.consumeIteratorAndExecute();

        result.get(10, TimeUnit.SECONDS);

        assertThat(numPauses.get(), Matchers.is(1));
        assertThat(numBatches.get(), Matchers.is(4));
    }
}
