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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Comparator;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.MemoryCircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import io.crate.breaker.ConcurrentRamAccounting;
import io.crate.breaker.TypedCellsAccounting;
import io.crate.data.BatchIterator;
import io.crate.data.Bucket;
import io.crate.data.Input;
import io.crate.data.Projector;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.data.RowN;
import io.crate.data.breaker.RowAccounting;
import io.crate.data.testing.TestingBatchIterators;
import io.crate.data.testing.TestingRowConsumer;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.execution.engine.collect.RowCollectExpression;
import io.crate.execution.engine.pipeline.LimitAndOffset;
import io.crate.expression.symbol.Literal;
import io.crate.types.DataTypes;

public class SortingLimitAndOffsetProjectorTest extends ESTestCase {

    private static final RowCollectExpression INPUT = new RowCollectExpression(0);
    private static final Literal<Boolean> TRUE_LITERAL = Literal.of(true);
    private static final List<Input<?>> INPUT_LITERAL_LIST = List.of(INPUT, TRUE_LITERAL);
    private static final List<CollectExpression<Row, ?>> COLLECT_EXPRESSIONS = List.of(INPUT);
    private static final Comparator<Object[]> FIRST_CELL_ORDERING = OrderingByPosition.arrayOrdering(DataTypes.INTEGER, 0, false, false);

    private TestingRowConsumer consumer = new TestingRowConsumer();

    private Projector getProjector(RowAccounting<Object[]> rowAccounting, int numOutputs, int limit, int offset, Comparator<Object[]> ordering) {
        int unboundedCollectorThreshold = 10_000;
        if (random().nextFloat() >= 0.5f) {
            unboundedCollectorThreshold = 1;
        }
        logger.info("Creating SortingLimitAndOffsetProjector projector with unbounded collector threshold {}",
                    unboundedCollectorThreshold);

        return new SortingLimitAndOffsetProjector(
            rowAccounting,
            INPUT_LITERAL_LIST,
            COLLECT_EXPRESSIONS,
            numOutputs,
            ordering,
            limit,
            offset,
            unboundedCollectorThreshold
        );
    }

    private Projector getProjector(int numOutputs, int limit, int offset) {
        return getProjector(new IgnoreRowCellsAccounting(), numOutputs, limit, offset, FIRST_CELL_ORDERING);
    }

    @Test
    public void testOrderBy() throws Exception {
        Projector projector = getProjector(1, 3, 5);
        consumer.accept(projector.apply(TestingBatchIterators.range(1, 11)), null);

        Bucket rows = consumer.getBucket();
        assertThat(rows).hasSize(3);

        int iterateLength = 0;
        for (Row row : rows) {
            assertThat(row).isEqualTo(new Row1(iterateLength + 6));
            iterateLength++;
        }
        assertThat(iterateLength).isEqualTo(3);
    }

    @Test
    public void testOrderByWithLimitMuchHigherThanExpectedRowsCount() throws Exception {
        Projector projector = getProjector(new IgnoreRowCellsAccounting(), 1, 100_000, LimitAndOffset.NO_OFFSET, FIRST_CELL_ORDERING);
        consumer.accept(projector.apply(TestingBatchIterators.range(1, 11)), null);

        Bucket rows = consumer.getBucket();
        assertThat(rows).hasSize(10);
    }

    @Test
    public void testOrderByWithoutOffset() throws Exception {
        Projector projector = getProjector(2, 10, LimitAndOffset.NO_OFFSET);
        consumer.accept(projector.apply(TestingBatchIterators.range(1, 11)), null);

        Bucket rows = consumer.getBucket();
        assertThat(rows).hasSize(10);
        int iterateLength = 0;
        for (Row row : consumer.getBucket()) {
            assertThat(row).isEqualTo(new RowN(iterateLength + 1, true));
            iterateLength++;
        }
        assertThat(iterateLength).isEqualTo(10);
    }

    @Test
    public void testUsedMemoryIsAccountedFor() throws Exception {
        MemoryCircuitBreaker circuitBreaker = new MemoryCircuitBreaker(new ByteSizeValue(30, ByteSizeUnit.BYTES),
                                                                       1,
                                                                       LogManager.getLogger(
                                                                           SortingLimitAndOffsetProjectorTest.class)
        );
        TypedCellsAccounting rowAccounting =
            new TypedCellsAccounting(
                List.of(DataTypes.INTEGER, DataTypes.BOOLEAN),
                ConcurrentRamAccounting.forCircuitBreaker("testContext", circuitBreaker, 0),
                0);

        Projector projector = getProjector(rowAccounting, 1, 100_000, LimitAndOffset.NO_OFFSET, FIRST_CELL_ORDERING);
        consumer.accept(projector.apply(TestingBatchIterators.range(1, 11)), null);

        expectedException.expect(CircuitBreakingException.class);
        consumer.getResult();
    }

    @Test
    public void testWithHighOffset() throws Exception {
        Projector projector = getProjector(2, 2, 30);
        consumer.accept(projector.apply(TestingBatchIterators.range(1, 10)), null);
        assertThat(consumer.getBucket()).hasSize(0);
    }

    @Test
    public void testInvalidNegativeLimit() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Invalid LIMIT: value must be > 0; got: -1");

        getProjector(2, -1, 0);
    }

    @Test
    public void test_zero_limit() throws Exception {
        Projector projector = getProjector(2, 0, 0);
        BatchIterator<Row> it = projector.apply(TestingBatchIterators.range(1, 6));
        assertThat(it.moveNext()).isFalse();
        assertThat(it.allLoaded()).isTrue();
    }

    @Test
    public void testInvalidOffset() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Invalid OFFSET: value must be >= 0; got: -1");

        getProjector(2, 1, -1);
    }

    @Test
    public void testInvalidMaxSize() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Invalid LIMIT + OFFSET: value must be <= 2147483630; got: 2147483646");

        int i = Integer.MAX_VALUE / 2;
        getProjector(2, i, i);
    }

    @Test
    public void testInvalidMaxSizeExceedsIntegerRange() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Invalid LIMIT + OFFSET: value must be <= 2147483630; got: -2147483648");

        int i = Integer.MAX_VALUE / 2 + 1;
        getProjector(2, i, i);
    }
}
