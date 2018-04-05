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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HashInnerJoinBatchIteratorMemoryTest {

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
            () -> 2
        );
        TestingRowConsumer consumer = new TestingRowConsumer();
        consumer.accept(it, null);
        consumer.getResult();
        assertThat(leftIterator.countCallsForReleaseMem, is(6));
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
