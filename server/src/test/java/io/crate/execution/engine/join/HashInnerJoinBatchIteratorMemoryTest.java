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

package io.crate.execution.engine.join;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Objects;
import java.util.function.Predicate;
import java.util.function.ToIntFunction;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import io.crate.data.BatchIterator;
import io.crate.data.Row;
import io.crate.data.breaker.RowAccounting;
import io.crate.data.join.CombinedRow;
import io.crate.data.testing.BatchSimulatingIterator;
import io.crate.data.testing.TestingBatchIterators;
import io.crate.data.testing.TestingRowConsumer;

public class HashInnerJoinBatchIteratorMemoryTest {

    @Rule
    public MockitoRule initRule = MockitoJUnit.rule();

    private final CircuitBreaker circuitBreaker = mock(CircuitBreaker.class);

    private static Predicate<Row> getCol0EqCol1JoinCondition() {
        return row -> Objects.equals(row.get(0), row.get(1));
    }

    private static ToIntFunction<Row> getHashForLeft() {
        return row -> Objects.hash(row.get(0));
    }

    private static ToIntFunction<Row> getHashForRight() {
        return row -> Objects.hash(row.get(0));
    }

    @Test
    public void testReleaseAccountingRows() throws Exception {
        BatchSimulatingIterator<Row> leftIterator = new BatchSimulatingIterator<>(
            TestingBatchIterators.range(0, 12),
            3,
            3,
            null
        );
        BatchIterator<Row> rightIterator = new BatchSimulatingIterator<>(TestingBatchIterators.range(0, 10), 2, 4, null);

        when(circuitBreaker.getLimit()).thenReturn(110L);
        when(circuitBreaker.getUsed()).thenReturn(10L);

        RowAccounting<Object[]> rowAccounting = mock(RowAccounting.class);
        BatchIterator<Row> it = new HashInnerJoinBatchIterator(
            leftIterator,
            rightIterator,
            rowAccounting,
            new CombinedRow(1, 1),
            getCol0EqCol1JoinCondition(),
            getHashForLeft(),
            getHashForRight(),
            ignored -> 2
        );
        TestingRowConsumer consumer = new TestingRowConsumer();
        consumer.accept(it, null);
        consumer.getResult();
        verify(rowAccounting, times(8)).release();
        verify(rowAccounting, times(12)).accountForAndMaybeBreak(Mockito.any(Object[].class));
    }
}
