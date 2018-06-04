/*
 * Licensed to Crate.IO GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import io.crate.analyze.OrderBy;
import io.crate.breaker.RamAccountingContext;
import io.crate.breaker.RowAccountingWithEstimators;
import io.crate.breaker.RowAccountingWithEstimatorsTest;
import io.crate.data.Row;
import io.crate.data.RowN;
import io.crate.expression.symbol.Literal;
import io.crate.planner.PositionalOrderBy;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.TestingHelpers;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.MemoryCircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.hamcrest.Matchers.instanceOf;

public class RamAccountingPageIteratorTest extends CrateUnitTest {

    private static NoopCircuitBreaker NOOP_CIRCUIT_BREAKER = new NoopCircuitBreaker("dummy");
    private static RowN[] TEST_ROWS = new RowN[]{
        new RowN(new BytesRef[]{new BytesRef("a"), new BytesRef("b"), new BytesRef("c")}),
        new RowN(new BytesRef[]{new BytesRef("d"), new BytesRef("e"), new BytesRef("f")})
    };

    private long originalBufferSize;

    @Before
    public void reduceFlushBufferSize() throws Exception {
        originalBufferSize = RamAccountingContext.FLUSH_BUFFER_SIZE;
        RamAccountingContext.FLUSH_BUFFER_SIZE = 20;
    }

    @After
    public void resetFlushBufferSize() throws Exception {
        RamAccountingContext.FLUSH_BUFFER_SIZE = originalBufferSize;
    }

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
            new OrderBy(Collections.singletonList(Literal.of(1)), new boolean[]{false}, new Boolean[]{false}),
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
            () -> new RowAccountingWithEstimators(ImmutableList.of(DataTypes.STRING, DataTypes.STRING, DataTypes.STRING),
                                    new RamAccountingContext("test", NOOP_CIRCUIT_BREAKER)));
        assertThat(pagingIterator, instanceOf(RamAccountingPageIterator.class));
        assertThat(((RamAccountingPageIterator) pagingIterator).delegatePagingIterator,
                   instanceOf(PassThroughPagingIterator.class));

        pagingIterator.merge(Arrays.asList(
            new KeyIterable<>(0, Collections.singletonList(TEST_ROWS[0])),
            new KeyIterable<>(1, Collections.singletonList(TEST_ROWS[1]))));
        pagingIterator.finish();

        Row[] rows = Iterators.toArray(pagingIterator, Row.class);
        assertThat(rows[0], TestingHelpers.isRow("a", "b", "c"));
        assertThat(rows[1], TestingHelpers.isRow("d", "e", "f"));
    }

    @Test
    public void testCircuitBreaking() throws Exception {
        PagingIterator<Integer, Row> pagingIterator = PagingIterator.create(
            2,
            true,
            null,
            () -> new RowAccountingWithEstimators(ImmutableList.of(DataTypes.STRING, DataTypes.STRING, DataTypes.STRING),
                                    new RamAccountingContext(
                                        "test",
                                        new MemoryCircuitBreaker(
                                            new ByteSizeValue(197, ByteSizeUnit.BYTES),
                                            1,
                                            Loggers.getLogger(RowAccountingWithEstimatorsTest.class)))));

        expectedException.expect(CircuitBreakingException.class);
        expectedException.expectMessage(
            "Data too large, data for field [test] would be [198/198b], which is larger than the limit of [197/197b]");
        pagingIterator.merge(Arrays.asList(
            new KeyIterable<>(0, Collections.singletonList(TEST_ROWS[0])),
            new KeyIterable<>(1, Collections.singletonList(TEST_ROWS[1]))));
    }
}
