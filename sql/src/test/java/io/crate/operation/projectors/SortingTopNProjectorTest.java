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

import io.crate.core.collections.Bucket;
import io.crate.core.collections.Row;
import io.crate.core.collections.RowN;
import io.crate.operation.Input;
import io.crate.operation.collect.CollectExpression;
import io.crate.operation.collect.InputCollectExpression;
import io.crate.planner.symbol.Literal;
import org.hamcrest.Matcher;
import org.junit.Test;

import static io.crate.testing.TestingHelpers.isRow;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.core.Is.is;

@SuppressWarnings({"unchecked", "NullArgumentToVariableArgMethod"})
public class SortingTopNProjectorTest {

    private static final Input<Integer> INPUT = new InputCollectExpression<>(0);
    private static final Literal<Boolean> TRUE_LITERAL = Literal.newLiteral(true);

    private final RowN spare = new RowN(new Object[]{});

    private Row spare(Object... cells) {
        if (cells == null) {
            cells = new Object[]{null};
        }
        spare.cells(cells);
        return spare;
    }

    @Test
    public void testOrderByWithoutLimitAndOffset() throws Exception {
        SortingTopNProjector projector = new SortingTopNProjector(
                new Input<?>[]{INPUT, TRUE_LITERAL},
                new CollectExpression[]{(CollectExpression<?>) INPUT},
                2,
                new int[]{0},
                new boolean[]{false},
                new Boolean[]{null},
                TopN.NO_LIMIT,
                TopN.NO_OFFSET);
        projector.registerUpstream(null);
        projector.startProjection();
        int i;
        for (i = 10; i > 0; i--) {   // 10 --> 1
            if (!projector.setNextRow(spare(i))) {
                break;
            }
        }
        assertThat(i, is(0)); // needs to collect all it can get
        projector.upstreamFinished();
        Bucket rows = projector.result().get();
        assertThat(rows.size(), is(10));
        int iterateLength = 0;
        for (Row row : projector.result().get()) {
            assertThat(row, isRow(iterateLength + 1, true));
            iterateLength++;
        }
        assertThat(iterateLength, is(10));
    }

    @Test
    public void testDownstreamResultEqualsResultProviderResult() throws Exception {
        SortingTopNProjector[] projectors = new SortingTopNProjector[2];
        CollectingProjector collector = new CollectingProjector();
        for (int i = 0; i < projectors.length; i++) {
            SortingTopNProjector projector = new SortingTopNProjector(
                    new Input<?>[]{INPUT},
                    new CollectExpression[]{(CollectExpression<?>) INPUT},
                    1,
                    new int[]{0},
                    new boolean[]{false},
                    new Boolean[]{null},
                    2,
                    1
            );
            projectors[i] = projector;
            projector.registerUpstream(null);

            if (i == 1) {
                projector.downstream(collector);
            }

            projector.startProjection();
            projector.setNextRow(spare(1));
            projector.setNextRow(spare(2));
            projector.setNextRow(spare(3));
            projector.setNextRow(spare(4));
            projector.upstreamFinished();
        }

        Matcher<Iterable<? extends Row>> expected = contains(
                isRow(2),
                isRow(3)
        );


        Bucket rowsFromBucket = projectors[0].result().get();
        assertThat(rowsFromBucket, expected);

        Bucket rowsFromIter = collector.result().get();
        assertThat(rowsFromIter, expected);


    }


    @Test
    public void testWithHighOffset() throws Exception {
        SortingTopNProjector projector = new SortingTopNProjector(
                new Input<?>[]{INPUT, TRUE_LITERAL},
                new CollectExpression[]{(CollectExpression<?>) INPUT},
                2,
                new int[]{0},
                new boolean[]{false},
                new Boolean[]{null},
                2,
                30
        );
        projector.registerUpstream(null);
        projector.startProjection();
        for (int i = 0; i < 10; i++) {
            if (!projector.setNextRow(spare(i))) {
                break;
            }
        }

        projector.upstreamFinished();
        assertThat(projector.result().get().size(), is(0));
    }

    @Test
    public void testOrderByWithoutLimit() throws Exception {
        SortingTopNProjector projector = new SortingTopNProjector(
                new Input<?>[]{INPUT, TRUE_LITERAL},
                new CollectExpression[]{(CollectExpression<?>) INPUT},
                2,
                new int[]{0},
                new boolean[]{false},
                new Boolean[]{null},
                TopN.NO_LIMIT,
                5);
        projector.registerUpstream(null);
        projector.startProjection();
        int i;
        for (i = 10; i > 0; i--) {   // 10 --> 1
            if (!projector.setNextRow(spare(i))) {
                break;
            }
        }
        assertThat(i, is(0)); // needs to collect all it can get
        projector.upstreamFinished();
        Bucket rows = projector.result().get();
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
        SortingTopNProjector projector = new SortingTopNProjector(
                new Input<?>[]{INPUT, TRUE_LITERAL},
                new CollectExpression[]{(CollectExpression<?>) INPUT},
                1,
                new int[]{0},
                new boolean[]{false},
                new Boolean[]{null},
                3,
                5);
        projector.registerUpstream(null);
        projector.startProjection();
        int i;
        for (i = 10; i > 0; i--) {   // 10 --> 1
            projector.setNextRow(spare(i));
        }
        assertThat(i, is(0)); // needs to collect all it can get
        projector.upstreamFinished();
        Bucket rows = projector.result().get();
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
        SortingTopNProjector projector = new SortingTopNProjector(
                new Input<?>[]{INPUT},
                new CollectExpression[]{(CollectExpression<?>) INPUT},
                1,
                new int[]{0},
                new boolean[]{false},
                new Boolean[]{true},
                100,
                0);
        projector.registerUpstream(null);
        projector.startProjection();
        projector.setNextRow(spare(1));
        projector.setNextRow(spare(null));
        projector.upstreamFinished();

        Bucket rows = projector.result().get();
        assertThat(rows, contains(isRow(null), isRow(1)));
    }

    @Test
    public void testOrderByAscNullsLast() throws Exception {
        SortingTopNProjector projector = new SortingTopNProjector(
                new Input<?>[]{INPUT},
                new CollectExpression[]{(CollectExpression<?>) INPUT},
                1,
                new int[]{0},
                new boolean[]{false},
                new Boolean[]{false},
                100,
                0);
        projector.registerUpstream(null);
        projector.startProjection();
        projector.setNextRow(spare(1));
        projector.setNextRow(spare(null));
        projector.upstreamFinished();

        Bucket rows = projector.result().get();
        assertThat(rows, contains(isRow(1), isRow(null)));
    }

    @Test
    public void testOrderByDescNullsLast() throws Exception {
        SortingTopNProjector projector = new SortingTopNProjector(
                new Input<?>[]{INPUT},
                new CollectExpression[]{(CollectExpression<?>) INPUT},
                1,
                new int[]{0},
                new boolean[]{true},
                new Boolean[]{false},
                100,
                0);
        projector.registerUpstream(null);
        projector.startProjection();
        projector.setNextRow(spare(1));
        projector.setNextRow(spare(null));
        projector.upstreamFinished();

        Bucket rows = projector.result().get();
        assertThat(rows, contains(isRow(1), isRow(null)));
    }

    @Test
    public void testOrderByDescNullsFirst() throws Exception {
        SortingTopNProjector projector = new SortingTopNProjector(
                new Input<?>[]{INPUT},
                new CollectExpression[]{(CollectExpression<?>) INPUT},
                1,
                new int[]{0},
                new boolean[]{true},
                new Boolean[]{true},
                100,
                0);
        projector.registerUpstream(null);
        projector.startProjection();
        projector.setNextRow(spare(1));
        projector.setNextRow(spare(null));
        projector.upstreamFinished();

        Bucket rows = projector.result().get();
        assertThat(rows, contains(isRow(null), isRow(1)));
    }

    @Test
    public void testOrderByAsc() throws Exception {
        SortingTopNProjector projector = new SortingTopNProjector(
                new Input<?>[]{INPUT, TRUE_LITERAL},
                new CollectExpression[]{(CollectExpression<?>) INPUT},
                2,
                new int[]{0},
                new boolean[]{true},
                new Boolean[]{null},
                3,
                5);
        projector.registerUpstream(null);
        projector.startProjection();
        int i;
        for (i = 0; i < 10; i++) {   // 0 --> 9
            projector.setNextRow(spare(i));
        }
        assertThat(i, is(10)); // needs to collect all it can get
        projector.upstreamFinished();
        Bucket rows = projector.result().get();
        assertThat(rows, contains(
                isRow(4, true),
                isRow(3, true),
                isRow(2, true)
        ));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNegativeOffset() {
        new SortingTopNProjector(new Input<?>[]{INPUT, TRUE_LITERAL}, new CollectExpression[]{(CollectExpression<?>) INPUT}, 2,
                new int[]{0},
                new boolean[]{true},
                new Boolean[]{null},
                TopN.NO_LIMIT,
                -10);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNegativeLimit() {
        new SortingTopNProjector(new Input<?>[]{INPUT, TRUE_LITERAL}, new CollectExpression[]{(CollectExpression<?>) INPUT}, 2,
                new int[]{0},
                new boolean[]{true},
                new Boolean[]{null},
                -100,
                TopN.NO_OFFSET);
    }


    @Test
    public void testNoUpstreams() throws Exception {
        SortingTopNProjector projector = new SortingTopNProjector(
                new Input<?>[]{INPUT, TRUE_LITERAL},
                new CollectExpression[]{(CollectExpression<?>) INPUT},
                2,
                new int[]{0},
                new boolean[]{false},
                new Boolean[]{null},
                2,
                0
        );
        projector.startProjection();
        assertThat(projector.result().get(), emptyIterable());
    }

    @Test
    public void testMultipleOrderBy() throws Exception {
        // select modulo(bla, 4), bla from x order by modulo(bla, 4), bla
        Input<Integer> input = new InputCollectExpression<>(1);
        SortingTopNProjector projector = new SortingTopNProjector(
                new Input<?>[]{input, INPUT, /* order By input */input, INPUT},
                new CollectExpression[]{(CollectExpression<?>) INPUT, (CollectExpression<?>) input},
                2,
                new int[]{2, 3},
                new boolean[]{false, false},
                new Boolean[]{null, null},
                TopN.NO_LIMIT,
                TopN.NO_OFFSET);
        projector.registerUpstream(null);
        projector.startProjection();
        int i;
        for (i = 0; i < 7; i++) {
            projector.setNextRow(spare(i, i % 4));
        }
        projector.upstreamFinished();
        Bucket rows = projector.result().get();
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
