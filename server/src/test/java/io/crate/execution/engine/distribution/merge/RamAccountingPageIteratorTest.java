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

package io.crate.execution.engine.distribution.merge;

import io.crate.analyze.OrderBy;
import io.crate.breaker.ConcurrentRamAccounting;
import io.crate.breaker.RamAccounting;
import io.crate.breaker.RowAccountingWithEstimators;
import io.crate.breaker.RowAccountingWithEstimatorsTest;
import io.crate.data.Row;
import io.crate.data.RowN;
import io.crate.expression.symbol.Literal;
import io.crate.planner.PositionalOrderBy;
import org.elasticsearch.test.ESTestCase;
import io.crate.testing.TestingHelpers;
import io.crate.types.DataTypes;
import org.apache.logging.log4j.LogManager;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.MemoryCircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.instanceOf;

public class RamAccountingPageIteratorTest extends ESTestCase {

    private static RowN[] TEST_ROWS = new RowN[]{
        new RowN("a", "b", "c"),
        new RowN("d", "e", "f")
    };

    @Test
    public void testNoRamAccountingWrappingAppliedForNullOrderByAndNonRepeat() {
        PagingIterator<Integer, Row> pagingIterator1 = PagingIterator.create(
            2,
            false,
            null,
            () -> null);

        assertThat(pagingIterator1, instanceOf(PassThroughPagingIterator.class));
    }

    @Test
    public void testRamAccountingWrappingAppliedForRepeatableIterator() {
        PagingIterator<Integer, Row> repeatableIterator = PagingIterator.create(
            2,
            true,
            null,
            () -> null);
        assertThat(repeatableIterator, instanceOf(RamAccountingPageIterator.class));
        assertThat(((RamAccountingPageIterator) repeatableIterator).delegatePagingIterator,
            instanceOf(PassThroughPagingIterator.class));
    }

    @Test
    public void testRamAccountingWrappingAppliedForOrderedIterators() {
        PositionalOrderBy orderBy = PositionalOrderBy.of(
            new OrderBy(Collections.singletonList(Literal.of(1)), new boolean[]{false}, new boolean[]{false}),
            Collections.singletonList(Literal.of(1)));

        PagingIterator<Integer, Row> repeatingSortedPagingIterator = PagingIterator.create(
            2,
            true,
            orderBy,
            () -> null);

        assertThat(repeatingSortedPagingIterator, instanceOf(RamAccountingPageIterator.class));
        assertThat(((RamAccountingPageIterator) repeatingSortedPagingIterator).delegatePagingIterator,
            instanceOf(SortedPagingIterator.class));

        PagingIterator<Integer, Row> nonRepeatingSortedPagingIterator = PagingIterator.create(
            2,
            false,
            orderBy,
            () -> null);
        assertThat(nonRepeatingSortedPagingIterator, instanceOf(RamAccountingPageIterator.class));
        assertThat(((RamAccountingPageIterator) nonRepeatingSortedPagingIterator).delegatePagingIterator,
            instanceOf(SortedPagingIterator.class));
    }

    @Test
    public void testNoCircuitBreaking() {
        PagingIterator<Integer, Row> pagingIterator = PagingIterator.create(
            2,
            true,
            null,
            () -> new RowAccountingWithEstimators(List.of(DataTypes.STRING, DataTypes.STRING, DataTypes.STRING),
                                                  RamAccounting.NO_ACCOUNTING));
        assertThat(pagingIterator, instanceOf(RamAccountingPageIterator.class));
        assertThat(((RamAccountingPageIterator) pagingIterator).delegatePagingIterator,
                   instanceOf(PassThroughPagingIterator.class));

        pagingIterator.merge(Arrays.asList(
            new KeyIterable<>(0, Collections.singletonList(TEST_ROWS[0])),
            new KeyIterable<>(1, Collections.singletonList(TEST_ROWS[1]))));
        pagingIterator.finish();

        var rows = new ArrayList<Row>();
        pagingIterator.forEachRemaining(rows::add);
        assertThat(rows.get(0), TestingHelpers.isRow("a", "b", "c"));
        assertThat(rows.get(1), TestingHelpers.isRow("d", "e", "f"));
    }

    @Test
    public void testCircuitBreaking() throws Exception {
        PagingIterator<Integer, Row> pagingIterator = PagingIterator.create(
            2,
            true,
            null,
            () -> new RowAccountingWithEstimators(
                List.of(DataTypes.STRING, DataTypes.STRING, DataTypes.STRING),
                ConcurrentRamAccounting.forCircuitBreaker(
                    "test",
                    new MemoryCircuitBreaker(
                        new ByteSizeValue(197, ByteSizeUnit.BYTES),
                        1,
                        LogManager.getLogger(RowAccountingWithEstimatorsTest.class)))));

        expectedException.expect(CircuitBreakingException.class);
        expectedException.expectMessage(
            "Data too large, data for field [test] would be [288/288b], which is larger than the limit of [197/197b]");
        pagingIterator.merge(Arrays.asList(
            new KeyIterable<>(0, Collections.singletonList(TEST_ROWS[0])),
            new KeyIterable<>(1, Collections.singletonList(TEST_ROWS[1]))));
    }
}
