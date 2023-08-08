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

package io.crate.execution.engine.sort;

import static io.crate.testing.TestingHelpers.isRow;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.MemoryCircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import io.crate.breaker.ConcurrentRamAccounting;
import io.crate.breaker.RowCellsAccountingWithEstimators;
import io.crate.data.BatchIterator;
import io.crate.data.Bucket;
import io.crate.data.Projector;
import io.crate.data.Row;
import io.crate.data.breaker.RowAccounting;
import io.crate.data.testing.TestingBatchIterators;
import io.crate.data.testing.TestingRowConsumer;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.execution.engine.collect.RowCollectExpression;
import io.crate.expression.symbol.Literal;
import io.crate.types.DataTypes;

public class SortingProjectorTest extends ESTestCase {

    private TestingRowConsumer consumer = new TestingRowConsumer();

    private SortingProjector createProjector(RowAccounting<Object[]> rowAccounting, int numOutputs, int offset) {
        RowCollectExpression input = new RowCollectExpression(0);
        return new SortingProjector(
            rowAccounting,
            List.of(input, Literal.of(true)),
            List.<CollectExpression<Row, ?>>of(input),
            numOutputs,
            OrderingByPosition.arrayOrdering(DataTypes.INTEGER, 0, false, false),
            offset
        );
    }

    private SortingProjector createProjector(int numOutputs, int offset) {
        return createProjector(new IgnoreRowCellsAccounting(), numOutputs, offset);
    }

    @Test
    public void testOrderBy() throws Exception {
        SortingProjector projector = createProjector(2, 0);

        BatchIterator batchIterator = projector.apply(TestingBatchIterators.range(1, 11));
        consumer.accept(batchIterator, null);
        Bucket rows = consumer.getBucket();
        assertThat(rows.size(), is(10));
        int iterateLength = 1;
        for (Row row : rows) {
            assertThat(row, isRow(iterateLength++, true));
        }
    }

    @Test
    public void testOrderByWithOffset() throws Exception {
        SortingProjector projector = createProjector(2, 5);

        BatchIterator batchIterator = projector.apply(TestingBatchIterators.range(1, 11));
        consumer.accept(batchIterator, null);
        Bucket rows = consumer.getBucket();

        assertThat(rows.size(), is(5));
        int iterateLength = 6;
        for (Row row : rows) {
            assertThat(row, isRow(iterateLength++, true));
        }
    }

    @Test
    public void testInvalidOffset() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("invalid offset -1");

        new SortingProjector(null, null, null, 2, null, -1);
    }

    @Test
    public void testUsedMemoryIsAccountedFor() throws Exception {
        MemoryCircuitBreaker circuitBreaker = new MemoryCircuitBreaker(new ByteSizeValue(30, ByteSizeUnit.BYTES),
                                                                       1,
                                                                       LogManager.getLogger(SortingProjectorTest.class)
        );
        RowCellsAccountingWithEstimators rowAccounting =
            new RowCellsAccountingWithEstimators(
                List.of(DataTypes.INTEGER, DataTypes.BOOLEAN),
                ConcurrentRamAccounting.forCircuitBreaker("testContext", circuitBreaker, 0),
                0);

        Projector projector = createProjector(rowAccounting, 1, 0);
        consumer.accept(projector.apply(TestingBatchIterators.range(1, 11)), null);

        expectedException.expect(CircuitBreakingException.class);
        consumer.getResult();
    }
}
