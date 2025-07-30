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

package io.crate.session;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import org.elasticsearch.test.ESTestCase;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import io.crate.data.BatchIterator;
import io.crate.data.Row;
import io.crate.data.testing.BatchSimulatingIterator;
import io.crate.data.testing.FailingBatchIterator;
import io.crate.data.testing.TestingBatchIterators;

public class RowConsumerToResultReceiverTest extends ESTestCase {

    @Test
    public void testBatchedIteratorConsumption() throws Exception {
        List<Object[]> expectedResult = IntStream.range(0, 10)
            .mapToObj(i -> new Object[]{i})
            .toList();

        BatchSimulatingIterator<Row> batchSimulatingIterator =
            new BatchSimulatingIterator<>(TestingBatchIterators.range(0, 10),
                2,
                5,
                null);

        final List<Object[]> collectedRows = new ArrayList<>();
        BaseResultReceiver resultReceiver = new BaseResultReceiver() {
            @Override
            public CompletableFuture<Void> setNextRow(Row row) {
                var f = super.setNextRow(row);
                collectedRows.add(row.materialize());
                return f;
            }
        };
        RowConsumerToResultReceiver batchConsumer =
            new RowConsumerToResultReceiver(resultReceiver, 0, t -> {});

        batchConsumer.accept(batchSimulatingIterator, null);
        resultReceiver.completionFuture().get(10, TimeUnit.SECONDS);

        assertThat(collectedRows).hasSize(10);
        for (int i = 0; i < collectedRows.size(); i++) {
            assertThat(collectedRows.get(i)).isEqualTo(expectedResult.get(i));
        }
    }

    @Test
    public void testExceptionOnAllLoadedCallIsForwardedToResultReceiver() throws Exception {
        BaseResultReceiver resultReceiver = new BaseResultReceiver();
        RowConsumerToResultReceiver consumer = new RowConsumerToResultReceiver(resultReceiver, 0, t -> {});

        consumer.accept(FailingBatchIterator.failOnAllLoaded(), null);
        assertThat(resultReceiver.completionFuture().isCompletedExceptionally()).isTrue();
    }

    @Test
    public void test_consumer_pauses_and_resume_based_on_receivers_writablility() throws Exception {
        BatchSimulatingIterator<Row> batchSimulatingIterator =
            new BatchSimulatingIterator<>(TestingBatchIterators.range(0, 10),
                2,
                5,
                null);

        AtomicReference<CompletableFuture<Void>> writeFutureRef = new AtomicReference<>(new CompletableFuture<>());
        int[] rowCount = new int[1];
        BaseResultReceiver resultReceiver = new BaseResultReceiver() {
            @Override
            @Nullable
            public CompletableFuture<Void> setNextRow(Row row) {
                rowCount[0]++;
                return writeFutureRef.get();
            }
        };
        var batchConsumer = new RowConsumerToResultReceiver(resultReceiver, 1, _ -> {});

        batchConsumer.accept(batchSimulatingIterator, null);

        assertThat(rowCount[0]).isEqualTo(1);
        assertThat(batchConsumer.waitingForWrite()).isTrue();
        assertThat(batchConsumer.suspended())
            .as("Must not set suspended, as it causes further portal usage in Session to close the consumer")
            .isFalse();

        writeFutureRef.get().complete(null);
        resultReceiver.completionFuture().get(10, TimeUnit.SECONDS);

        assertThat(batchConsumer.waitingForWrite()).isFalse();
        assertThat(rowCount[0]).isEqualTo(10);
    }

    @Test
    public void test_does_not_suspend_consumer_on_last_row() throws Exception {
        {
            int numRows = randomIntBetween(1, 10);
            BatchIterator<Row> batchIterator = TestingBatchIterators.range(0, numRows);
            BaseResultReceiver resultReceiver = new BaseResultReceiver();
            var consumer = new RowConsumerToResultReceiver(resultReceiver, numRows, _ -> {});

            consumer.accept(batchIterator, null);
            assertThat(consumer.suspended()).isFalse();
            assertThat(consumer.completionFuture()).isDone();
        }
        {
            int numRows = randomIntBetween(2, 10);
            BatchIterator<Row> numbers = TestingBatchIterators.range(0, numRows);
            var batchIterator = new BatchSimulatingIterator<>(numbers, numRows - 1, 1, null);
            BaseResultReceiver resultReceiver = new BaseResultReceiver();
            var consumer = new RowConsumerToResultReceiver(resultReceiver, numRows, _ -> {});

            consumer.accept(batchIterator, null);
            assertThat(consumer.completionFuture()).succeedsWithin(5, TimeUnit.SECONDS);
            assertThat(consumer.suspended()).isFalse();
        }
    }
}
