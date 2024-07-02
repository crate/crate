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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.crate.data.BatchIterator;
import io.crate.data.Paging;
import io.crate.data.Row;
import io.crate.data.breaker.RowAccounting;
import io.crate.data.join.CombinedRow;
import io.crate.data.testing.BatchSimulatingIterator;
import io.crate.data.testing.TestingBatchIterators;
import io.crate.data.testing.TestingRowConsumer;

public class HashInnerJoinBatchIteratorBehaviouralTest {

    private int originalPageSize = Paging.PAGE_SIZE;

    @Before
    public void setUp() {
        Paging.PAGE_SIZE = 2;
    }

    @After
    public void tearDown() {
        Paging.PAGE_SIZE = originalPageSize;
    }

    @Test
    public void testDistributed_SwitchToRightEvenIfLeftBatchDoesNotDeliverAllRowsExpectedByOneBatch() throws Exception {
        BatchSimulatingIterator<Row> leftIterator = new BatchSimulatingIterator<>(
            TestingBatchIterators.ofValues(Arrays.asList(1, 2, 4)), 1, 2, null);
        BatchSimulatingIterator<Row> rightIterator = new BatchSimulatingIterator<>(
            TestingBatchIterators.ofValues(Arrays.asList(2, 0, 4, 5)), 2, 1, null);

        BatchIterator<Row> batchIterator = new HashInnerJoinBatchIterator(
                leftIterator,
                rightIterator,
                mock(RowAccounting.class),
                new CombinedRow(1, 1),
                row -> Objects.equals(row.get(0), row.get(1)),
                row -> Objects.hash(row.get(0)),
                row -> Objects.hash(row.get(0)),
                ignored -> 2
            );

        TestingRowConsumer consumer = new TestingRowConsumer();
        consumer.accept(batchIterator, null);
        List<Object[]> result = consumer.getResult();
        assertThat(result).containsExactly(new Object[]{2, 2}, new Object[]{4, 4});

        // as the blocksize is defined of 2 but the left batch size 1, normally it would call left loadNextBatch until
        // the blocksize is reached. we don't want that as parallel running hash iterators must call loadNextBatch always
        // on the same side synchronously as the upstreams will only send new data after all downstreams responded.
        // to validate this, the right must be repeated 3 times
        assertThat(rightIterator.getMovetoStartCalls()).isEqualTo(2);
    }

    @Test
    public void test_SwitchToRightWhenLeftExhausted() throws Exception {
        BatchSimulatingIterator<Row> leftIterator = new BatchSimulatingIterator<>(
            TestingBatchIterators.ofValues(Arrays.asList(1, 2, 3, 4)), 2, 1, null);
        BatchSimulatingIterator<Row> rightIterator = new BatchSimulatingIterator<>(
            TestingBatchIterators.ofValues(Arrays.asList(2, 0, 4, 5)), 2, 1, null);

        BatchIterator<Row> batchIterator = new HashInnerJoinBatchIterator(
            leftIterator,
            rightIterator,
            mock(RowAccounting.class),
            new CombinedRow(1, 1),
            row -> Objects.equals(row.get(0), row.get(1)),
            row -> Objects.hash(row.get(0)),
            row -> Objects.hash(row.get(0)),
            ignored -> 500000
        );

        TestingRowConsumer consumer = new TestingRowConsumer();
        consumer.accept(batchIterator, null);
        List<Object[]> result = consumer.getResult();
        assertThat(result).containsExactly(new Object[]{2, 2}, new Object[]{4, 4});
    }
}
