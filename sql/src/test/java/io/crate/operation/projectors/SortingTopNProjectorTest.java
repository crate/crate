/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.operation.projectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import io.crate.analyze.symbol.Literal;
import io.crate.core.collections.ArrayRow;
import io.crate.core.collections.Bucket;
import io.crate.core.collections.Row;
import io.crate.operation.Input;
import io.crate.operation.collect.CollectExpression;
import io.crate.operation.collect.InputCollectExpression;
import io.crate.operation.projectors.sorting.OrderingByPosition;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.CollectingRowReceiver;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static io.crate.testing.TestingHelpers.isNullRow;
import static io.crate.testing.TestingHelpers.isRow;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.core.Is.is;

public class SortingTopNProjectorTest extends CrateUnitTest {

    private static final InputCollectExpression INPUT = new InputCollectExpression(0);
    private static final Literal<Boolean> TRUE_LITERAL = Literal.newLiteral(true);
    private static final List<Input<?>> INPUT_LITERAL_LIST = ImmutableList.of(INPUT, TRUE_LITERAL);
    private static final List<CollectExpression<Row, ?>> COLLECT_EXPRESSIONS = ImmutableList.<CollectExpression<Row, ?>>of(INPUT);
    private static final Ordering<Object[]> FIRST_CELL_ORDERING = OrderingByPosition.arrayOrdering(0, false, null);

    private final ArrayRow spare = new ArrayRow();

    private Row spare(Object... cells) {
        if (cells == null) {
            cells = new Object[]{null};
        }
        spare.cells(cells);
        return spare;
    }

    private Projector getProjector(int numOutputs, int limit, int offset, RowReceiver rowReceiver, Ordering<Object[]> ordering) {
        Projector pipe = new SortingTopNProjector(
                INPUT_LITERAL_LIST,
                COLLECT_EXPRESSIONS,
                numOutputs,
                ordering,
                limit,
                offset
        );
        pipe.downstream(rowReceiver);
        return pipe;
    }

    private Projector getProjector(int numOutputs, int limit, int offset, RowReceiver rowReceiver) {
        return getProjector(numOutputs, limit, offset, rowReceiver, FIRST_CELL_ORDERING);
    }

    @Test
    public void testOrderByWithoutLimitAndOffset() throws Exception {
        CollectingRowReceiver rowReceiver = new CollectingRowReceiver();
        Projector pipe = getProjector(2, TopN.NO_LIMIT, TopN.NO_OFFSET, rowReceiver);
        int i;
        for (i = 10; i > 0; i--) {   // 10 --> 1
            if (!pipe.setNextRow(spare(i))) {
                break;
            }
        }
        assertThat(i, is(0)); // needs to collect all it can get
        pipe.finish();
        Bucket rows = rowReceiver.result();
        assertThat(rows.size(), is(10));
        int iterateLength = 0;
        for (Row row : rowReceiver.result()) {
            assertThat(row, isRow(iterateLength + 1, true));
            iterateLength++;
        }
        assertThat(iterateLength, is(10));
    }

    @Test
    public void testWithHighOffset() throws Exception {
        CollectingRowReceiver rowReceiver = new CollectingRowReceiver();
        Projector pipe = getProjector(2, 2, 30, rowReceiver);

        for (int i = 0; i < 10; i++) {
            if (!pipe.setNextRow(spare(i))) {
                break;
            }
        }

        pipe.finish();
        assertThat(rowReceiver.result().size(), is(0));
    }

    @Test
    public void testOrderByWithoutLimit() throws Exception {
        CollectingRowReceiver rowReceiver = new CollectingRowReceiver();
        Projector pipe = getProjector(2, TopN.NO_LIMIT, 5, rowReceiver);
        int i;
        for (i = 10; i > 0; i--) {   // 10 --> 1
            if (!pipe.setNextRow(spare(i))) {
                break;
            }
        }
        assertThat(i, is(0)); // needs to collect all it can get
        pipe.finish();
        Bucket rows = rowReceiver.result();
        assertThat(rows.size(), is(5));
        int iterateLength = 0;
        for (Row row : rows) {
            assertThat(row, isRow(iterateLength + 6, true));
            iterateLength++;
        }
        assertThat(iterateLength, is(5));
    }

    @Test
    public void testOrderBy() throws Exception {
        CollectingRowReceiver rowReceiver = new CollectingRowReceiver();
        Projector pipe = getProjector(1, 3, 5, rowReceiver);

        int i;
        for (i = 10; i > 0; i--) {   // 10 --> 1
            pipe.setNextRow(spare(i));
        }
        assertThat(i, is(0)); // needs to collect all it can get
        pipe.finish();
        Bucket rows = rowReceiver.result();
        assertThat(rows.size(), is(3));

        int iterateLength = 0;
        for (Row row : rows) {
            assertThat(row, isRow(iterateLength + 6));
            iterateLength++;
        }
        assertThat(iterateLength, is(3));
    }

    @Test
    public void testOrderByAscNullsFirst() throws Exception {
        CollectingRowReceiver rowReceiver = new CollectingRowReceiver();
        Projector pipe = getProjector(1, 100, 0, rowReceiver, OrderingByPosition.arrayOrdering(0, false, true));
        pipe.setNextRow(spare(1));
        pipe.setNextRow(spare(new Object[]{null}));
        pipe.finish();

        Bucket rows = rowReceiver.result();
        assertThat(rows, contains(isNullRow(), isRow(1)));
    }

    @Test
    public void testOrderByAscNullsLast() throws Exception {
        CollectingRowReceiver rowReceiver = new CollectingRowReceiver();
        Projector pipe = getProjector(1, 100, 0, rowReceiver, OrderingByPosition.arrayOrdering(0, false, false));

        pipe.setNextRow(spare(1));
        pipe.setNextRow(spare(new Object[]{null}));
        pipe.finish();

        Bucket rows = rowReceiver.result();
        assertThat(rows, contains(isRow(1), isNullRow()));
    }

    @Test
    public void testOrderByDescNullsLast() throws Exception {
        CollectingRowReceiver rowReceiver = new CollectingRowReceiver();
        Projector pipe = getProjector(1, 100, 0, rowReceiver, OrderingByPosition.arrayOrdering(0, true, false));

        pipe.setNextRow(spare(1));
        pipe.setNextRow(spare(new Object[]{null}));
        pipe.finish();

        Bucket rows = rowReceiver.result();
        assertThat(rows, contains(isRow(1), isNullRow()));
    }

    @Test
    public void testOrderByDescNullsFirst() throws Exception {
        CollectingRowReceiver rowReceiver = new CollectingRowReceiver();
        Projector pipe = getProjector(1, 100, 0, rowReceiver, OrderingByPosition.arrayOrdering(0, true, true));

        pipe.setNextRow(spare(1));
        pipe.setNextRow(spare(new Object[]{null}));
        pipe.finish();

        Bucket rows = rowReceiver.result();
        assertThat(rows, contains(isNullRow(), isRow(1)));
    }

    @Test
    public void testOrderByAsc() throws Exception {
        CollectingRowReceiver rowReceiver = new CollectingRowReceiver();
        Projector pipe = getProjector(2, 3, 5, rowReceiver, OrderingByPosition.arrayOrdering(0, true, null));

        int i;
        for (i = 0; i < 10; i++) {   // 0 --> 9
            pipe.setNextRow(spare(i));
        }
        assertThat(i, is(10)); // needs to collect all it can get
        pipe.finish();
        Bucket rows = rowReceiver.result();
        assertThat(rows, contains(
                isRow(4, true),
                isRow(3, true),
                isRow(2, true)
        ));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNegativeOffset() {
        new SortingTopNProjector(INPUT_LITERAL_LIST, COLLECT_EXPRESSIONS, 2, FIRST_CELL_ORDERING, TopN.NO_LIMIT, -10);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNegativeLimit() {
        new SortingTopNProjector(INPUT_LITERAL_LIST, COLLECT_EXPRESSIONS, 2, FIRST_CELL_ORDERING, -100, 10);
    }

    @Test
    public void testMultipleOrderBy() throws Exception {
        // select modulo(bla, 4), bla from x order by modulo(bla, 4), bla
        InputCollectExpression input = new InputCollectExpression(1);
        CollectingRowReceiver rowReceiver = new CollectingRowReceiver();
        SortingTopNProjector pipe = new SortingTopNProjector(
                Arrays.asList(input, INPUT, /* order By input */input, INPUT),
                Arrays.<CollectExpression<Row, ?>>asList(INPUT, input),
                2,
                Ordering.compound(Arrays.asList(
                        OrderingByPosition.arrayOrdering(2, false, null),
                        OrderingByPosition.arrayOrdering(3, false, null)
                )),
                TopN.NO_LIMIT,
                TopN.NO_OFFSET);

        pipe.downstream(rowReceiver);
        int i;
        for (i = 0; i < 7; i++) {
            pipe.setNextRow(spare(i, i % 4));
        }
        pipe.finish();
        Bucket rows = rowReceiver.result();
        assertThat(rows, contains(
                isRow(0, 0),
                isRow(0, 4),
                isRow(1, 1),
                isRow(1, 5),
                isRow(2, 2),
                isRow(2, 6),
                isRow(3, 3)
        ));
    }
}
