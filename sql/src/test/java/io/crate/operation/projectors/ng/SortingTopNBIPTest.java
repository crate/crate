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

package io.crate.operation.projectors.ng;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import io.crate.analyze.symbol.Literal;
import io.crate.data.BatchIterator;
import io.crate.data.BatchIteratorProjector;
import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.operation.collect.CollectExpression;
import io.crate.operation.collect.InputCollectExpression;
import io.crate.operation.projectors.sorting.OrderingByPosition;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.BatchIteratorTester;
import io.crate.testing.SingleColumnBatchIterator;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.LongStream;


/**
 * Equivalent copy of {@link io.crate.operation.projectors.SortingTopNProjectorTest}
 */
public class SortingTopNBIPTest extends CrateUnitTest {

    private static final InputCollectExpression INPUT = new InputCollectExpression(0);
    private static final Literal<Boolean> TRUE_LITERAL = Literal.of(true);
    private static final List<Input<?>> INPUT_LITERAL_LIST = ImmutableList.of(INPUT, TRUE_LITERAL);
    private static final List<CollectExpression<Row, ?>> COLLECT_EXPRESSIONS = ImmutableList.of(INPUT);
    private static final Ordering<Object[]> FIRST_CELL_ORDERING = OrderingByPosition.arrayOrdering(0, false, null);

    private BatchIteratorProjector getProjector(int numOutputs, int limit, int offset, Ordering<Object[]> ordering) {
        return new SortingTopNBIP(INPUT_LITERAL_LIST, COLLECT_EXPRESSIONS, numOutputs, ordering, limit, offset);
    }

    private BatchIteratorProjector getProjector(int numOutputs, int limit, int offset) {
        return getProjector(numOutputs, limit, offset, FIRST_CELL_ORDERING);
    }

    private void assertAndTest(BatchIteratorProjector p, Supplier<BatchIterator> upstream, List<Object[]> expectedResult) throws Exception {
        BatchIteratorTester tester = new BatchIteratorTester(
            () -> p.apply(upstream.get()),
            expectedResult
        );
        tester.run();
    }

    private void assertAndTest(BatchIteratorProjector p, List<Object[]> expectedResult) throws Exception {
        assertAndTest(p, ()-> SingleColumnBatchIterator.range(0L, 100L), expectedResult);
    }

    private List<Object[]> rowsOfLongs(LongStream stream){
        return stream.mapToObj(l -> new Object[]{l}).collect(Collectors.toList());
    }

    @Test
    public void testOrderBy() throws Exception {
        BatchIteratorProjector p = getProjector(1, 3, 5);
        assertAndTest(p, rowsOfLongs(LongStream.of(5L, 6L, 7L)));
    }

    @Test
    public void testOrderByWithoutOffset() throws Exception {
        BatchIteratorProjector p = getProjector(1, 10, 0);
        assertAndTest(p, rowsOfLongs(LongStream.range(0L, 10L)));
    }

    @Test
    public void testWithHighOffset() throws Exception {
        BatchIteratorProjector p = getProjector(1, 2, 130);
        assertAndTest(p, Collections.emptyList());
    }

    @Test
    public void testInvalidNegativeLimit() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("invalid limit -1, this collector only supports positive limits");
        new SortingTopNBIP(INPUT_LITERAL_LIST, COLLECT_EXPRESSIONS, 2, FIRST_CELL_ORDERING, -1, 0);
    }

    @Test
    public void testInvalidZeroLimit() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("invalid limit 0, this collector only supports positive limits");
        new SortingTopNBIP(INPUT_LITERAL_LIST, COLLECT_EXPRESSIONS, 2, FIRST_CELL_ORDERING, 0, 0);
    }

    @Test
    public void testInvalidOffset() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("invalid offset -1");
        new SortingTopNBIP(INPUT_LITERAL_LIST, COLLECT_EXPRESSIONS, 2, FIRST_CELL_ORDERING, 1, -1);
    }
}
