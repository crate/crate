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

import io.crate.testing.BatchSimulatingIterator;
import io.crate.testing.FailingBatchIterator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class BatchIteratorsTest {

    @Test
    public void testExceptionOnAllLoadedIsSetOntoFuture() throws Exception {
        CompletableFuture<Long> future = BatchIterators.collect(
            FailingBatchIterator.failOnAllLoaded(), Collectors.counting());
        assertThat(future.isCompletedExceptionally(), is(true));
    }


    @Test
    public void testBatchBySize() throws Exception {
        var batchIterator = InMemoryBatchIterator.of(() -> IntStream.range(0, 5).iterator(), null, false);
        BatchIterator<List<Integer>> batchedIt = BatchIterators.partition(batchIterator, 2, ArrayList::new, List::add, r -> false);

        assertThat(batchedIt.moveNext(), is(true));
        assertThat(batchedIt.currentElement(), is(Arrays.asList(0, 1)));
        assertThat(batchedIt.moveNext(), is(true));
        assertThat(batchedIt.currentElement(), is(Arrays.asList(2, 3)));
        assertThat(batchedIt.moveNext(), is(true));
        assertThat(batchedIt.currentElement(), is(Collections.singletonList(4)));
    }

    @Test
    public void testBatchBySizeWithBatchedSource() throws Exception {
        BatchIterator<Integer> batchIterator = new BatchSimulatingIterator<>(
            InMemoryBatchIterator.of(() -> IntStream.range(0, 5).iterator(), null, false),
            3,
            2,
            null
        );
        BatchIterator<List<Integer>> batchedIt = BatchIterators.partition(batchIterator, 2, ArrayList::new, List::add, r -> false);

        CompletableFuture<List<List<Integer>>> future = BatchIterators.collect(batchedIt, Collectors.toList());
        assertThat(future.get(10, TimeUnit.SECONDS), is(Arrays.asList(
            Arrays.asList(0, 1),
            Arrays.asList(2, 3),
            Collections.singletonList(4)
        )));
    }

    @Test
    public void testBatchBySizeWithDynamicLimiter() throws Exception {
        var batchIterator = InMemoryBatchIterator.of(() -> IntStream.range(0, 5).iterator(), null, false);
        final AtomicInteger rowCount = new AtomicInteger();
        BatchIterator<List<Integer>> batchedIt = BatchIterators.partition(batchIterator, 2, ArrayList::new, List::add,
            r -> rowCount.incrementAndGet() == 3);

        assertThat(batchedIt.moveNext(), is(true));
        assertThat(batchedIt.currentElement(), is(Arrays.asList(0, 1)));
        assertThat(batchedIt.moveNext(), is(true));
        assertThat(batchedIt.currentElement(), is(Collections.singletonList(2)));
        assertThat(batchedIt.moveNext(), is(true));
        assertThat(batchedIt.currentElement(), is(Arrays.asList(3, 4)));
    }
}
