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

package io.crate.data;

import static org.assertj.core.api.Assertions.fail;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

import io.crate.data.testing.BatchIteratorTester;
import io.crate.data.testing.BatchIteratorTester.ResultOrder;
import io.crate.data.testing.BatchSimulatingIterator;
import io.crate.data.testing.TestingBatchIterators;
import io.crate.data.testing.TestingRowConsumer;

class AsyncCompositeBatchIteratorTest {

    private static final List<Object[]> EXPECTED_RESULT = IntStream.range(0, 10)
        .mapToObj(i -> new Object[] {i})
        .collect(Collectors.toList());

    @Test
    void testCompositeBatchIterator() throws Exception {
        Supplier<BatchIterator<Row>> batchSimulatingItSupplier = () -> new BatchSimulatingIterator<>(
            TestingBatchIterators.range(5, 10),
            2,
            2,
            null
        );

        ExecutorService executorService = Executors.newFixedThreadPool(3);
        try {
            var tester = BatchIteratorTester.forRows(
                () -> CompositeBatchIterator.asyncComposite(
                    executorService,
                    () -> 3,
                    List.of(
                        TestingBatchIterators.range(0, 5),
                        batchSimulatingItSupplier.get()
                    )
                ), ResultOrder.EXACT
            );
            tester.verifyResultAndEdgeCaseBehaviour(EXPECTED_RESULT);
        } finally {
            executorService.shutdown();
            executorService.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    @Test
    void testIteratorDoesNotHandleRejectedExecutionException() throws Exception {
        ThreadPoolExecutor executorService = new ThreadPoolExecutor(1, 1, 0L,
            TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(1));

        try {
            Supplier<BatchIterator<Row>> batchSimulatingItSupplier = () -> new BatchSimulatingIterator<>(
                TestingBatchIterators.range(0, 5),
                2,
                2,
                null
            );
            BatchIterator<Row> batchIterator = CompositeBatchIterator.asyncComposite(
                executorService,
                () -> 3,
                List.of(
                    batchSimulatingItSupplier.get(),
                    batchSimulatingItSupplier.get(),
                    batchSimulatingItSupplier.get(),
                    batchSimulatingItSupplier.get()
                )
            );

            TestingRowConsumer consumer = new TestingRowConsumer();
            consumer.accept(batchIterator, null);
            consumer.getResult();
            fail("The AsyncBatchIterator should not handle the case when the executor rejects new tasks");
        } catch (RejectedExecutionException ignored) {
            // expected
        } finally {
            executorService.shutdown();
            executorService.awaitTermination(10, TimeUnit.SECONDS);
        }
    }
}
