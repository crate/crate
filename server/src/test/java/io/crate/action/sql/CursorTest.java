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

import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.junit.Test;

import io.crate.data.BatchIterator;
import io.crate.data.Row;
import io.crate.expression.symbol.InputColumn;
import io.crate.sql.tree.Declare.Hold;
import io.crate.sql.tree.Fetch.ScrollMode;
import io.crate.testing.TestingBatchIterators;
import io.crate.testing.TestingHelpers;
import io.crate.testing.TestingRowConsumer;
import io.crate.types.DataTypes;

public class CursorTest {

    @Test
    public void test_calls_consumer_with_failure_if_batchIterator_future_has_failure() throws Exception {
        CompletableFuture<BatchIterator<Row>> queryIterator = new CompletableFuture<>();
        IllegalStateException failure = new IllegalStateException("bad");
        queryIterator.completeExceptionally(failure);
        Cursor cursor = new Cursor(
            new NoopCircuitBreaker("dummy"),
            false,
            Hold.WITHOUT,
            queryIterator,
            List.of()
        );
        TestingRowConsumer consumer = new TestingRowConsumer();
        cursor.fetch(consumer, ScrollMode.RELATIVE, 1);

        assertThatThrownBy(() -> consumer.getBucket())
            .isSameAs(failure);
    }

    @Test
    public void test_cant_move_backward_without_scroll() throws Exception {
        BatchIterator<Row> rows = TestingBatchIterators.range(1, 6);
        Cursor cursor = new Cursor(
            new NoopCircuitBreaker("dummy"),
            false,
            Hold.WITHOUT,
            CompletableFuture.completedFuture(rows),
            List.of()
        );
        TestingRowConsumer consumer = new TestingRowConsumer();
        cursor.fetch(consumer, ScrollMode.RELATIVE, -2);

        assertThatThrownBy(() -> consumer.getBucket())
            .hasMessage("Cannot move backward if cursor was created with NO SCROLL");
    }

    @Test
    public void test_can_jump_back_with_absolute_movement_after_moving_forward() throws Exception {
        BatchIterator<Row> rows = TestingBatchIterators.range(1, 6);
        Cursor cursor = new Cursor(
            new NoopCircuitBreaker("dummy"),
            true,
            Hold.WITHOUT,
            CompletableFuture.completedFuture(rows),
            List.of(new InputColumn(0, DataTypes.INTEGER))
        );
        TestingRowConsumer consumer = new TestingRowConsumer();
        cursor.fetch(consumer, ScrollMode.RELATIVE, 2);
        assertThat(TestingHelpers.printedTable(consumer.getBucket())).isEqualTo(
            "1\n" +
            "2\n"
        );

        consumer = new TestingRowConsumer();
        cursor.fetch(consumer, ScrollMode.ABSOLUTE, 1);
        assertThat(TestingHelpers.printedTable(consumer.getBucket())).isEqualTo(
            "1\n"
        );
    }

    @Test
    public void test_fetching_backwards_from_cursor_positioned_at_start_returns_empty_result() throws Exception {
        BatchIterator<Row> rows = TestingBatchIterators.range(1, 5);
        Cursor cursor = new Cursor(
            new NoopCircuitBreaker("dummy"),
            true,
            Hold.WITHOUT,
            CompletableFuture.completedFuture(rows),
            List.of(new InputColumn(0, DataTypes.INTEGER))
        );
        TestingRowConsumer consumer = new TestingRowConsumer();
        cursor.fetch(consumer, ScrollMode.RELATIVE, -5);
        assertThat(TestingHelpers.printedTable(consumer.getBucket())).isEqualTo("");

        consumer = new TestingRowConsumer();
        cursor.fetch(consumer, ScrollMode.RELATIVE, 3);
        assertThat(TestingHelpers.printedTable(consumer.getBucket())).isEqualTo(
            """
                1
                2
                3
                """
        );
    }

    @Test
    public void test_fetching_absolute_exceeding_last_row() throws Exception {
        BatchIterator<Row> rows = TestingBatchIterators.range(1, 6);
        Cursor cursor = new Cursor(
                new NoopCircuitBreaker("dummy"),
                true,
                Hold.WITHOUT,
                CompletableFuture.completedFuture(rows),
                List.of(new InputColumn(0, DataTypes.INTEGER))
        );

        final TestingRowConsumer consumer = new TestingRowConsumer();
        cursor.fetch(consumer, ScrollMode.ABSOLUTE, 6);
        assertThatThrownBy(consumer::getBucket)
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("Cannot return row: 6, total rows: 5");
    }

    @Test
    public void test_scrolling() throws Exception {
        BatchIterator<Row> rows = TestingBatchIterators.range(1, 6);
        Cursor cursor = new Cursor(
                new NoopCircuitBreaker("dummy"),
                true,
                Hold.WITHOUT,
                CompletableFuture.completedFuture(rows),
                List.of(new InputColumn(0, DataTypes.INTEGER))
        );

        TestingRowConsumer consumer = new TestingRowConsumer();
        cursor.fetch(consumer, ScrollMode.RELATIVE, 2);
        assertThat(TestingHelpers.printedTable(consumer.getBucket())).isEqualTo(
                """
                1
                2
                """
        );
        consumer = new TestingRowConsumer();
        cursor.fetch(consumer, ScrollMode.RELATIVE, 3);
        assertThat(TestingHelpers.printedTable(consumer.getBucket())).isEqualTo(
                """
                3
                4
                5
                """
        );

        consumer = new TestingRowConsumer();
        cursor.fetch(consumer, ScrollMode.RELATIVE, -3);
        assertThat(TestingHelpers.printedTable(consumer.getBucket())).isEqualTo(
                """
                4
                3
                2
                """
        );

        consumer = new TestingRowConsumer();
        cursor.fetch(consumer, ScrollMode.ABSOLUTE, 0);
        assertThat(TestingHelpers.printedTable(consumer.getBucket())).isEqualTo("");

        // Exceed total rows
        cursor.fetch(consumer, ScrollMode.RELATIVE, 10);
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
        cursor.fetch(consumer, ScrollMode.RELATIVE, -3);
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
        assertThat(TestingHelpers.printedTable(consumer.getBucket())).isEqualTo("");
        consumer = new TestingRowConsumer();
        cursor.fetch(consumer, ScrollMode.RELATIVE, 2);
        assertThat(TestingHelpers.printedTable(consumer.getBucket())).isEqualTo(
                """
                1
                2
                """
        );

        // Continue moving fwd
        consumer = new TestingRowConsumer();
        cursor.fetch(consumer, ScrollMode.RELATIVE, 2);
        assertThat(TestingHelpers.printedTable(consumer.getBucket())).isEqualTo(
                """
                3
                4
                """
        );

        // Continue moving fwd
        consumer = new TestingRowConsumer();
        cursor.fetch(consumer, ScrollMode.RELATIVE, 2);
        assertThat(TestingHelpers.printedTable(consumer.getBucket())).isEqualTo(
                """
                5
                """
        );

        // Moving bwd
        consumer = new TestingRowConsumer();
        cursor.fetch(consumer, ScrollMode.RELATIVE, -2);
        assertThat(TestingHelpers.printedTable(consumer.getBucket())).isEqualTo(
                """
                5
                4
                """
        );

        // Continue moving bwd
        consumer = new TestingRowConsumer();
        cursor.fetch(consumer, ScrollMode.RELATIVE, -2);
        assertThat(TestingHelpers.printedTable(consumer.getBucket())).isEqualTo(
                """
                3
                2
                """
        );
    }
}
