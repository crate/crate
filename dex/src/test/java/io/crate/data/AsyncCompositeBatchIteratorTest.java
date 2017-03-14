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

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class AsyncCompositeBatchIteratorTest {

    private List<Object[]> expectedResult = IntStream.range(0, 10)
        .mapToObj(i -> new Object[]{i})
        .collect(Collectors.toList());

    @Test
    public void testCompositeBatchIterator() throws Exception {
        Supplier<BatchIterator> batchSimulatingItSupplier = () -> new CloseAssertingBatchIterator(
            new BatchSimulatingIterator(
                TestingBatchIterators.range(5, 10), 2, 2, null)
        );

        ExecutorService executorService = Executors.newFixedThreadPool(3);
        try {
            BatchIteratorTester tester = new BatchIteratorTester(
                () -> new AsyncCompositeBatchIterator(
                    executorService,
                    TestingBatchIterators.range(0, 5),
                    batchSimulatingItSupplier.get()
                )
            );
            tester.verifyResultAndEdgeCaseBehaviour(expectedResult);
        } finally {
            executorService.shutdown();
            executorService.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

}
