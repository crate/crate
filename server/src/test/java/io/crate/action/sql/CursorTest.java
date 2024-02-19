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

package io.crate.action.sql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.crate.breaker.TypedRowAccounting;
import io.crate.data.BatchIterator;
import io.crate.data.Row;
import io.crate.data.RowN;
import io.crate.data.breaker.BlockBasedRamAccounting;
import io.crate.data.breaker.RamAccounting;
import io.crate.data.breaker.RowAccounting;
import io.crate.data.testing.BatchSimulatingIterator;
import io.crate.data.testing.TestingBatchIterators;
import io.crate.data.testing.TestingRowConsumer;
import io.crate.execution.engine.distribution.merge.BatchPagingIterator;
import io.crate.execution.engine.distribution.merge.KeyIterable;
import io.crate.execution.engine.distribution.merge.PagingIterator;
import io.crate.expression.symbol.InputColumn;
import io.crate.sql.tree.Declare.Hold;
import io.crate.sql.tree.Fetch.ScrollMode;
import io.crate.testing.TestingHelpers;
import io.crate.types.DataTypes;
import io.crate.types.IntegerType;

public class CursorTest extends ESTestCase {

    private ExecutorService executor;

    @Before
    public void setupExecutor() {
        executor = Executors.newFixedThreadPool(3);
    }

    @After
    public void shutdownExecutor() throws Exception {
        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.SECONDS);
    }

    @Test
    public void test_calls_consumer_with_failure_if_batchIterator_future_has_failure() throws Exception {
        CompletableFuture<BatchIterator<Row>> queryIterator = new CompletableFuture<>();
        CompletableFuture<Void> result = new CompletableFuture<>();
        IllegalStateException failure = new IllegalStateException("bad");
        queryIterator.completeExceptionally(failure);
        Cursor cursor = new Cursor(
            new NoopCircuitBreaker("dummy"),
            "c1",
            "declare c1 ... select * from dummy",
            false,
            Hold.WITHOUT,
            queryIterator,
            result,
            List.of()
        );
        TestingRowConsumer consumer = new TestingRowConsumer();
        cursor.fetch(consumer, ScrollMode.RELATIVE, 1);

        assertThatThrownBy(consumer::getBucket)
            .isSameAs(failure);
        assertThat(result).isNotCompleted();
        cursor.close();
        assertThat(result).isCompletedExceptionally();
    }

    @Test
    public void test_cant_move_backward_without_scroll() throws Exception {
        CompletableFuture<Void> result = new CompletableFuture<>();
        BatchIterator<Row> rows = rows(1, 5);
        Cursor cursor = new Cursor(
            new NoopCircuitBreaker("dummy"),
            "c1",
            "declare c1 ... select * from dummy",
            false,
            Hold.WITHOUT,
            CompletableFuture.completedFuture(rows),
            result,
            List.of()
        );
        TestingRowConsumer consumer = new TestingRowConsumer();
        cursor.fetch(consumer, ScrollMode.RELATIVE, -2);

        assertThatThrownBy(consumer::getBucket)
            .hasMessage("Cannot move backward if cursor was created with NO SCROLL");
        assertThat(result).isNotCompleted();
        cursor.close();
        assertThat(result).isCompleted();
    }

    @Test
    public void test_release_consumer_ram_accounting_after_close() throws Exception {
        CompletableFuture<Void> result = new CompletableFuture<>();
        RamAccounting ramAccounting = new BlockBasedRamAccounting(bytes -> {}, 1) ;
        result.whenComplete((v, e) -> ramAccounting.release());

        RowAccounting<Row> rowAccounting = new TypedRowAccounting(
            List.of(IntegerType.INSTANCE, IntegerType.INSTANCE),
            ramAccounting
        );
        PagingIterator<Integer, Row> pi = PagingIterator.create(
            1,
            List.of(DataTypes.INTEGER, DataTypes.INTEGER),
            true,
            null,
            () -> rowAccounting);
        RowN[] testRows = new RowN[]{
            new RowN(1, 11),
            new RowN(2, 22),
            new RowN(3, 33),
            new RowN(4, 44),
            new RowN(5, 55),
        };

        pi.merge(List.of(new KeyIterable<>(0, List.of(testRows))));
        BatchIterator<Row> batchIterator = new BatchPagingIterator<>(
            pi,
            exhaustedIt -> null,
            () -> true,
            throwable -> {}
        );

        Cursor cursor = new Cursor(
            new NoopCircuitBreaker("dummy"),
            "c1",
            "declare c1 ... select * from dummy",
            false,
            Hold.WITHOUT,
            CompletableFuture.completedFuture(batchIterator),
            result,
            List.of()
        );
        TestingRowConsumer consumer = new TestingRowConsumer();
        cursor.fetch(consumer, ScrollMode.MOVE, 2);
        assertThat(TestingHelpers.printedTable(consumer.getBucket())).isEqualTo(
            """
                1| 11
                2| 22
                """
        );
        consumer.completionFuture().complete(null);

        consumer = new TestingRowConsumer();
        cursor.fetch(consumer, ScrollMode.MOVE, 3);
        assertThat(TestingHelpers.printedTable(consumer.getBucket())).isEqualTo(
            """
                3| 33
                4| 44
                5| 55
                """
        );
        consumer.completionFuture().complete(null);

        assertThat(result).isNotCompleted();
        assertThat(ramAccounting.totalBytes()).isEqualTo(160L);
        cursor.close();
        assertThat(result).isCompleted();
        assertThat(ramAccounting.totalBytes()).isZero();
    }

    @Test
    public void test_can_jump_back_with_absolute_movement_after_moving_forward() throws Exception {
        CompletableFuture<Void> result = new CompletableFuture<>();
        BatchIterator<Row> rows = rows(1, 5);
        Cursor cursor = new Cursor(
            new NoopCircuitBreaker("dummy"),
            "c1",
            "declare c1 ... select * from dummy",
            true,
            Hold.WITHOUT,
            CompletableFuture.completedFuture(rows),
            result,
            List.of(new InputColumn(0, DataTypes.INTEGER))
        );
        TestingRowConsumer consumer = new TestingRowConsumer();
        cursor.fetch(consumer, ScrollMode.MOVE, 2);
        assertThat(TestingHelpers.printedTable(consumer.getBucket())).isEqualTo(
            """
                1
                2
                """
        );
        assertThat(cursor.cursorPosition).isEqualTo(2);

        consumer = new TestingRowConsumer();
        cursor.fetch(consumer, ScrollMode.ABSOLUTE, 1);
        assertThat(TestingHelpers.printedTable(consumer.getBucket())).isEqualTo(
            "1\n"
        );
        assertThat(cursor.cursorPosition).isEqualTo(1);

        assertThat(result).isNotCompleted();
        cursor.close();
        assertThat(result).isCompleted();
    }

    @Test
    public void test_fetching_backwards_from_cursor_positioned_at_start_returns_empty_result() throws Exception {
        CompletableFuture<Void> result = new CompletableFuture<>();
        BatchIterator<Row> rows = rows(1, 5);
        Cursor cursor = new Cursor(
            new NoopCircuitBreaker("dummy"),
            "c1",
            "declare c1 ... select * from dummy",
            true,
            Hold.WITHOUT,
            CompletableFuture.completedFuture(rows),
            result,
            List.of(new InputColumn(0, DataTypes.INTEGER))
        );
        TestingRowConsumer consumer = new TestingRowConsumer();
        cursor.fetch(consumer, ScrollMode.MOVE, -5);
        assertThat(TestingHelpers.printedTable(consumer.getBucket())).isEmpty();
        assertThat(cursor.cursorPosition).isZero();

        consumer = new TestingRowConsumer();
        cursor.fetch(consumer, ScrollMode.MOVE, 3);
        assertThat(TestingHelpers.printedTable(consumer.getBucket())).isEqualTo(
            """
                1
                2
                3
                """
        );
        assertThat(cursor.cursorPosition).isEqualTo(3);

        assertThat(result).isNotCompleted();
        cursor.close();
        assertThat(result).isCompleted();
    }

    @Test
    public void test_fetching_absolute_exceeding_first_and_last_row() throws Exception {
        CompletableFuture<Void> result = new CompletableFuture<>();
        BatchIterator<Row> rows = rows(1, 5);
        Cursor cursor = new Cursor(
            new NoopCircuitBreaker("dummy"),
            "c1",
            "declare c1 ... select * from dummy",
            true,
            Hold.WITHOUT,
            CompletableFuture.completedFuture(rows),
            result,
            List.of(new InputColumn(0, DataTypes.INTEGER))
        );

        final TestingRowConsumer errorConsumer = new TestingRowConsumer();
        cursor.fetch(errorConsumer, ScrollMode.ABSOLUTE, 6);
        assertThatThrownBy(errorConsumer::getBucket)
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Cannot return row: 6, total rows: 5");

        TestingRowConsumer consumer = new TestingRowConsumer();
        cursor.fetch(consumer, ScrollMode.ABSOLUTE, -5);
        assertThat(TestingHelpers.printedTable(consumer.getBucket())).isEmpty();

        assertThat(result).isNotCompleted();
        cursor.close();
        assertThat(result).isCompleted();
    }

    @Test
    public void test_fetching_relative_exceeding_first_and_last_row() throws Exception {
        CompletableFuture<Void> result = new CompletableFuture<>();
        BatchIterator<Row> rows = TestingBatchIterators.range(1, 6);
        Cursor cursor = new Cursor(
            new NoopCircuitBreaker("dummy"),
            "c1",
            "declare c1 ... select * from dummy",
            true,
            Hold.WITHOUT,
            CompletableFuture.completedFuture(rows),
            result,
            List.of(new InputColumn(0, DataTypes.INTEGER))
        );

        TestingRowConsumer consumer = new TestingRowConsumer();
        cursor.fetch(consumer, ScrollMode.RELATIVE, 6);
        assertThat(TestingHelpers.printedTable(consumer.getBucket())).isEmpty();

        consumer = new TestingRowConsumer();
        cursor.fetch(consumer, ScrollMode.MOVE, -3);
        assertThat(TestingHelpers.printedTable(consumer.getBucket())).isEqualTo(
            """
            5
            4
            3
            """
        );

        consumer = new TestingRowConsumer();
        cursor.fetch(consumer, ScrollMode.RELATIVE, -3);
        assertThat(TestingHelpers.printedTable(consumer.getBucket())).isEmpty();

        assertThat(result).isNotCompleted();
        cursor.close();
        assertThat(result).isCompleted();
    }

    @Test
    public void test_fetch_backward_all() throws Exception {
        CompletableFuture<Void> result = new CompletableFuture<>();
        BatchIterator<Row> rows = rows(1, 5);
        Cursor cursor = new Cursor(
            new NoopCircuitBreaker("dummy"),
            "c1",
            "declare c1 ... select * from dummy",
            true,
            Hold.WITHOUT,
            CompletableFuture.completedFuture(rows),
            result,
            List.of(new InputColumn(0, DataTypes.INTEGER))
        );

        // Move forward to last row
        TestingRowConsumer consumer = new TestingRowConsumer();
        cursor.fetch(consumer, ScrollMode.MOVE, 5);
        assertThat(TestingHelpers.printedTable(consumer.getBucket())).isEqualTo(
                """
                1
                2
                3
                4
                5
                """
        );
        assertThat(cursor.cursorPosition).isEqualTo(5);

        // FETCH BACKWARD ALL
        consumer = new TestingRowConsumer();
        cursor.fetch(consumer, ScrollMode.MOVE, - Long.MAX_VALUE);
        assertThat(TestingHelpers.printedTable(consumer.getBucket())).isEqualTo(
                """
                4
                3
                2
                1
                """
        );
        assertThat(cursor.cursorPosition).isZero();

        // Move fwd, exceeding last row
        consumer = new TestingRowConsumer();
        cursor.fetch(consumer, ScrollMode.MOVE, 100);
        assertThat(TestingHelpers.printedTable(consumer.getBucket())).isEqualTo(
                """
                1
                2
                3
                4
                5
                """
        );
        assertThat(cursor.cursorPosition).isEqualTo(100);

        // FETCH BACKWARD ALL
        consumer = new TestingRowConsumer();
        cursor.fetch(consumer, ScrollMode.MOVE, - Integer.MAX_VALUE);
        assertThat(TestingHelpers.printedTable(consumer.getBucket())).isEqualTo(
                """
                5
                4
                3
                2
                1
                """
        );
        assertThat(cursor.cursorPosition).isZero();

        assertThat(result).isNotCompleted();
        cursor.close();
        assertThat(result).isCompleted();
    }

    @Test
    public void test_fetch_forward_all() throws Exception {
        CompletableFuture<Void> result = new CompletableFuture<>();
        BatchIterator<Row> rows = rows(1, 5);
        Cursor cursor = new Cursor(
            new NoopCircuitBreaker("dummy"),
            "c1",
            "declare c1 ... select * from dummy",
            true,
            Hold.WITHOUT,
            CompletableFuture.completedFuture(rows),
            result,
            List.of(new InputColumn(0, DataTypes.INTEGER))
        );

        // Move forward to last row
        TestingRowConsumer consumer = new TestingRowConsumer();
        cursor.fetch(consumer, ScrollMode.MOVE, Long.MAX_VALUE);
        assertThat(TestingHelpers.printedTable(consumer.getBucket())).isEqualTo(
            """
            1
            2
            3
            4
            5
            """
        );
        assertThat(cursor.cursorPosition).isEqualTo(Integer.MAX_VALUE);

        // FETCH BACKWARD ALL
        consumer = new TestingRowConsumer();
        cursor.fetch(consumer, ScrollMode.RELATIVE, 10);
        assertThat(TestingHelpers.printedTable(consumer.getBucket())).isEmpty();
        assertThat(cursor.cursorPosition).isEqualTo(16); // rows.size() + 1 + count

        assertThat(result).isNotCompleted();
        cursor.close();
        assertThat(result).isCompleted();
    }

    @Test
    public void test_scrolling() throws Exception {
        CompletableFuture<Void> result = new CompletableFuture<>();
        BatchIterator<Row> rows = rows(1, 5);
        Cursor cursor = new Cursor(
            new NoopCircuitBreaker("dummy"),
            "c1",
            "declare c1 ... select * from dummy",
            true,
            Hold.WITHOUT,
            CompletableFuture.completedFuture(rows),
            result,
            List.of(new InputColumn(0, DataTypes.INTEGER))
        );

        TestingRowConsumer consumer = new TestingRowConsumer();
        cursor.fetch(consumer, ScrollMode.MOVE, 2);
        assertThat(TestingHelpers.printedTable(consumer.getBucket())).isEqualTo(
                """
                1
                2
                """
        );
        consumer = new TestingRowConsumer();
        cursor.fetch(consumer, ScrollMode.MOVE, 3);
        assertThat(TestingHelpers.printedTable(consumer.getBucket())).isEqualTo(
                """
                3
                4
                5
                """
        );

        consumer = new TestingRowConsumer();
        cursor.fetch(consumer, ScrollMode.MOVE, -3);
        assertThat(TestingHelpers.printedTable(consumer.getBucket())).isEqualTo(
                """
                4
                3
                2
                """
        );

        consumer = new TestingRowConsumer();
        cursor.fetch(consumer, ScrollMode.ABSOLUTE, 0);
        assertThat(TestingHelpers.printedTable(consumer.getBucket())).isEmpty();

        // Exceed total rows
        consumer = new TestingRowConsumer();
        cursor.fetch(consumer, ScrollMode.MOVE, 10);
        assertThat(TestingHelpers.printedTable(consumer.getBucket())).isEqualTo(
                """
                1
                2
                3
                4
                5
                """
        );
        consumer = new TestingRowConsumer();
        cursor.fetch(consumer, ScrollMode.MOVE, -3);
        assertThat(TestingHelpers.printedTable(consumer.getBucket())).isEqualTo(
                """
                5
                4
                3
                """
        );

        // Negative absolute - next fwd == 1st row
        consumer = new TestingRowConsumer();
        cursor.fetch(consumer, ScrollMode.ABSOLUTE, -20);
        assertThat(TestingHelpers.printedTable(consumer.getBucket())).isEmpty();
        consumer = new TestingRowConsumer();
        cursor.fetch(consumer, ScrollMode.MOVE, 2);
        assertThat(TestingHelpers.printedTable(consumer.getBucket())).isEqualTo(
                """
                1
                2
                """
        );

        // Continue moving fwd
        consumer = new TestingRowConsumer();
        cursor.fetch(consumer, ScrollMode.MOVE, 2);
        assertThat(TestingHelpers.printedTable(consumer.getBucket())).isEqualTo(
                """
                3
                4
                """
        );

        // Continue moving fwd
        consumer = new TestingRowConsumer();
        cursor.fetch(consumer, ScrollMode.MOVE, 2);
        assertThat(TestingHelpers.printedTable(consumer.getBucket())).isEqualTo(
                """
                5
                """
        );

        // Moving bwd
        consumer = new TestingRowConsumer();
        cursor.fetch(consumer, ScrollMode.MOVE, -2);
        assertThat(TestingHelpers.printedTable(consumer.getBucket())).isEqualTo(
                """
                5
                4
                """
        );

        // Continue moving bwd
        consumer = new TestingRowConsumer();
        cursor.fetch(consumer, ScrollMode.MOVE, -2);
        assertThat(TestingHelpers.printedTable(consumer.getBucket())).isEqualTo(
                """
                3
                2
                """
        );

        // Relative fwd
        consumer = new TestingRowConsumer();
        cursor.fetch(consumer, ScrollMode.RELATIVE, 2);
        assertThat(TestingHelpers.printedTable(consumer.getBucket())).isEqualTo(
                """
                4
                """
        );

        // Relative fwd - exceed last row
        consumer = new TestingRowConsumer();
        cursor.fetch(consumer, ScrollMode.RELATIVE, 2);
        assertThat(TestingHelpers.printedTable(consumer.getBucket())).isEmpty();

        // Relative bwd - include last row
        consumer = new TestingRowConsumer();
        cursor.fetch(consumer, ScrollMode.RELATIVE, -2);
        assertThat(TestingHelpers.printedTable(consumer.getBucket())).isEqualTo(
                """
                4
                """
        );

        // Continue relative bwd
        consumer = new TestingRowConsumer();
        cursor.fetch(consumer, ScrollMode.RELATIVE, -3);
        assertThat(TestingHelpers.printedTable(consumer.getBucket())).isEqualTo(
                """
                    1
                    """
        );

        // Relative bwd - exceed first row
        consumer = new TestingRowConsumer();
        cursor.fetch(consumer, ScrollMode.RELATIVE, -3);
        assertThat(TestingHelpers.printedTable(consumer.getBucket())).isEmpty();

        // Relative fwd - include first row
        consumer = new TestingRowConsumer();
        cursor.fetch(consumer, ScrollMode.RELATIVE, 3);
        assertThat(TestingHelpers.printedTable(consumer.getBucket())).isEqualTo(
                """
                3
                """
        );

        // Absolute bwd to start
        consumer = new TestingRowConsumer();
        cursor.fetch(consumer, ScrollMode.ABSOLUTE, 1);
        assertThat(TestingHelpers.printedTable(consumer.getBucket())).isEqualTo(
            """
                1
                """
        );


        // Relative fwd excluding first
        consumer = new TestingRowConsumer();
        cursor.fetch(consumer, ScrollMode.RELATIVE, 3);
        assertThat(TestingHelpers.printedTable(consumer.getBucket())).isEqualTo(
            """
                4
                """
        );

        assertThat(result).isNotCompleted();
        cursor.close();
        assertThat(result).isCompleted();
    }

    private BatchIterator<Row> rows(int startInclusive, int endInclusive) {
        BatchIterator<Row> batchIterator = TestingBatchIterators.range(startInclusive, endInclusive + 1);
        if (randomBoolean()) {
            int size = endInclusive - startInclusive + 1;
            int batchSize = randomIntBetween(1, size);
            int batches = Math.ceilDiv(size, batchSize);
            return new BatchSimulatingIterator<>(batchIterator, batchSize, batches, executor);
        } else {
            return batchIterator;
        }
    }
}
