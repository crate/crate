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

import io.crate.testing.BatchIteratorTester;
import io.crate.testing.BatchSimulatingIterator;
import io.crate.testing.TestingBatchIterators;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class CollectingBatchIteratorTest {

    private List<Object[]> expectedResult = Collections.singletonList(new Object[]{45L});
    private ExecutorService executor;

    @Before
    public void setUp() throws Exception {
        executor = Executors.newFixedThreadPool(2);
    }

    @After
    public void tearDown() throws Exception {
        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.SECONDS);
    }

    @Test
    public void testCollectingBatchIterator() throws Exception {
        BatchIteratorTester tester = new BatchIteratorTester(
            () -> CollectingBatchIterator.summingLong(TestingBatchIterators.range(0L, 10L))
        );
        tester.verifyResultAndEdgeCaseBehaviour(expectedResult);
    }

    @Test
    public void testCollectingBatchIteratorWithPagedSource() throws Exception {
        BatchIteratorTester tester = new BatchIteratorTester(
            () -> CollectingBatchIterator.summingLong(
                new BatchSimulatingIterator<>(TestingBatchIterators.range(0L, 10L), 2, 5, executor)
            )
        );
        tester.verifyResultAndEdgeCaseBehaviour(expectedResult);
    }

    @Test
    public void testCollectingBatchIteratorPropagatesExceptionOnLoadNextBatch() throws Exception {
        CompletableFuture<Iterable<Row>> loadItemsFuture = new CompletableFuture<>();
        BatchIterator<Row> collectingBatchIterator = CollectingBatchIterator.newInstance(
            () -> {},
            t -> {},
            () -> loadItemsFuture,
            false);
        loadItemsFuture.completeExceptionally(new RuntimeException());
        assertThat(collectingBatchIterator.loadNextBatch().toCompletableFuture().isCompletedExceptionally(), is(true));
    }
}
