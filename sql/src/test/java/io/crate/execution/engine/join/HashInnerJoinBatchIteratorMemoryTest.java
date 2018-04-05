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

package io.crate.execution.engine.join;

import io.crate.breaker.RowAccounting;
import io.crate.data.BatchIterator;
import io.crate.data.Row;
import io.crate.data.join.CombinedRow;
import io.crate.testing.BatchSimulatingIterator;
import io.crate.testing.TestingBatchIterators;
import io.crate.testing.TestingRowConsumer;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.junit.Test;

import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;

import static io.crate.execution.engine.join.HashInnerJoinBatchIterator.calculateBlockSize;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HashInnerJoinBatchIteratorMemoryTest {

    private static final int ROWS_TO_BE_CONSUMED = 10_000;
    private final CircuitBreaker circuitBreaker = mock(CircuitBreaker.class);

    private static Predicate<Row> getCol0EqCol1JoinCondition() {
        return row -> Objects.equals(row.get(0), row.get(1));
    }

    private static Function<Row, Integer> getHashForLeft() {
        return row -> Objects.hash(row.get(0));
    }

    private static Function<Row, Integer> getHashForRight() {
        return row -> Objects.hash(row.get(0));
    }

    @Test
    public void testReleaseAccountingRows() throws Exception {
        TestRamAccountingBatchIterator leftIterator = new TestRamAccountingBatchIterator(
            new BatchSimulatingIterator<>(TestingBatchIterators.range(0, 12), 3, 4, null),
            mock(RowAccounting.class));
        BatchIterator<Row> rightIterator = new BatchSimulatingIterator<>(TestingBatchIterators.range(0, 10), 2, 5, null);

        when(circuitBreaker.getLimit()).thenReturn(110L);
        when(circuitBreaker.getUsed()).thenReturn(10L);

        BatchIterator<Row> it = new HashInnerJoinBatchIterator<>(
            leftIterator,
            rightIterator,
            new CombinedRow(1, 1),
            getCol0EqCol1JoinCondition(),
            getHashForLeft(),
            getHashForRight(),
            circuitBreaker,
            50, // blockSize = 100/50 = 2
            100,
            Integer.MAX_VALUE
        );
        TestingRowConsumer consumer = new TestingRowConsumer();
        consumer.accept(it, null);
        consumer.getResult();
        assertThat(leftIterator.countCallsForReleaseMem, is(6));
    }

    @Test
    public void testCalculationOfBlockSize() {
        when(circuitBreaker.getLimit()).thenReturn(110L);
        when(circuitBreaker.getUsed()).thenReturn(10L);
        assertThat(calculateBlockSize(circuitBreaker, 5, 100, ROWS_TO_BE_CONSUMED), is(20));
        assertThat(calculateBlockSize(circuitBreaker, 5, 10, ROWS_TO_BE_CONSUMED), is(10));
    }

    @Test
    public void testBlockSizeIsAdjustedForUnorderedLimitJoins() {
        when(circuitBreaker.getLimit()).thenReturn(HashInnerJoinBatchIterator.DEFAULT_BLOCK_SIZE + 1L);
        when(circuitBreaker.getUsed()).thenReturn(0L);
        long numberOfRowsForLeft = HashInnerJoinBatchIterator.DEFAULT_BLOCK_SIZE + 1;
        int blockSize = calculateBlockSize(circuitBreaker, 1, numberOfRowsForLeft, 100);
        assertThat(blockSize, is(HashInnerJoinBatchIterator.DEFAULT_BLOCK_SIZE));

        // even for limit joins we don't use a block size larger than what the available memory dictates
        when(circuitBreaker.getLimit()).thenReturn(110L);
        when(circuitBreaker.getUsed()).thenReturn(10L);
        blockSize = calculateBlockSize(circuitBreaker, 1, numberOfRowsForLeft, 100);
        assertThat(blockSize, is(100));
    }

    @Test
    public void testBlockSizeIsNotAdjustedForOrderedLimitJoins() {
        when(circuitBreaker.getLimit()).thenReturn(HashInnerJoinBatchIterator.DEFAULT_BLOCK_SIZE + 1L);
        when(circuitBreaker.getUsed()).thenReturn(0L);

        long numberOfRowsForLeft = HashInnerJoinBatchIterator.DEFAULT_BLOCK_SIZE + 1;
        int blockSize = calculateBlockSize(circuitBreaker, 1, numberOfRowsForLeft, Integer.MAX_VALUE);
        assertThat(blockSize, is(HashInnerJoinBatchIterator.DEFAULT_BLOCK_SIZE + 1));
    }

    @Test
    public void testCalculationOfBlockSizeWithMissingStats() {
        when(circuitBreaker.getLimit()).thenReturn(-1L);
        assertThat(calculateBlockSize(circuitBreaker, 10, 10, ROWS_TO_BE_CONSUMED), is(HashInnerJoinBatchIterator.DEFAULT_BLOCK_SIZE));

        when(circuitBreaker.getLimit()).thenReturn(110L);
        when(circuitBreaker.getUsed()).thenReturn(10L);
        assertThat(calculateBlockSize(circuitBreaker, 10, -1, ROWS_TO_BE_CONSUMED), is(HashInnerJoinBatchIterator.DEFAULT_BLOCK_SIZE));
        assertThat(calculateBlockSize(circuitBreaker, -1, 10, ROWS_TO_BE_CONSUMED), is(HashInnerJoinBatchIterator.DEFAULT_BLOCK_SIZE));
    }

    @Test
    public void testCalculationOfBlockSizeWithNoMemLeft() {
        when(circuitBreaker.getLimit()).thenReturn(110L);
        when(circuitBreaker.getUsed()).thenReturn(110L);
        assertThat(calculateBlockSize(circuitBreaker, 10, 10, ROWS_TO_BE_CONSUMED), is(10));
    }

    @Test
    public void testCalculationOfBlockSizeWithIntegerOverflow() {
        when(circuitBreaker.getLimit()).thenReturn(Integer.MAX_VALUE + 1L);
        when(circuitBreaker.getUsed()).thenReturn(0L);
        assertThat(calculateBlockSize(circuitBreaker, 1, 1, -1, false), is(1));
    }

    private class TestRamAccountingBatchIterator extends RamAccountingBatchIterator<Row> {

        private int countCallsForReleaseMem = 0;

        private TestRamAccountingBatchIterator(BatchIterator<Row> delegatePagingIterator, RowAccounting rowAccounting) {
            super(delegatePagingIterator, rowAccounting);
        }

        @Override
        public void releaseAccountedRows() {
            countCallsForReleaseMem++;
            super.releaseAccountedRows();
        }
    }
}
