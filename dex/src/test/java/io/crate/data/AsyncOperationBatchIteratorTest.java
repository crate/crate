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

package io.crate.data;

import io.crate.testing.BatchIteratorTester;
import io.crate.testing.BatchSimulatingIterator;
import io.crate.testing.TestingBatchIterators;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class AsyncOperationBatchIteratorTest {

    @Test
    public void testAsyncOperationBatchIterator() throws Exception {
        runTest(() -> TestingBatchIterators.range(0, 10));
    }

    @Test
    public void testAsyncOperationBatchIteratorWithBatchedSource() throws Exception {
        runTest(() -> new BatchSimulatingIterator(TestingBatchIterators.range(0, 10), 3, 4, null));
    }

    private void runTest(Supplier<BatchIterator> sourceSupplier) throws Exception {
        Supplier<BatchIterator> biSupplier = () -> {
            BatchIterator source = sourceSupplier.get();
            Input<?> input = source.rowData().get(0);
            BatchAccumulator<Row, Iterator<? extends Row>> accumulator = new DummyBatchAccumulator(input);
            return new AsyncOperationBatchIterator(source, 1, accumulator);
        };

        List<Object[]> expectedResult = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            expectedResult.add(new Object[] { i * 3});
        }
        BatchIteratorTester tester = new BatchIteratorTester(biSupplier, expectedResult);
        tester.run();
    }

    private static class DummyBatchAccumulator implements BatchAccumulator<Row, Iterator<? extends Row>> {

        private final Input<?> input;
        private final List<Integer> items = new ArrayList<>();

        DummyBatchAccumulator(Input<?> input) {
            this.input = input;
        }

        @Override
        public void onItem(Row item) {
            items.add((Integer) input.value() * 3);
        }

        @Override
        public int batchSize() {
            return 3;
        }

        @Override
        public CompletableFuture<Iterator<? extends Row>> processBatch(boolean isLastBatch) {
            return CompletableFuture.supplyAsync(() -> {
                List<Row1> rows = items.stream().map(Row1::new).collect(Collectors.toList());
                items.clear();
                return rows.iterator();
            });
        }

        @Override
        public void close() {
        }

        public void reset() {
            items.clear();
        }
    }
}
