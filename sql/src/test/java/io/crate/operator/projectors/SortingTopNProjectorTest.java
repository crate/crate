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

package io.crate.operator.projectors;

import io.crate.operator.Input;
import io.crate.operator.InputCollectExpression;
import io.crate.operator.aggregation.CollectExpression;
import io.crate.planner.symbol.BooleanLiteral;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.number.OrderingComparison.lessThanOrEqualTo;

public class SortingTopNProjectorTest {

    private static final Input<Integer> INPUT = new InputCollectExpression<>(0);
    private static final BooleanLiteral TRUE_LITERAL = new BooleanLiteral(true);

    @Test
    public void testOrderByWithoutLimitAndOffset() throws Exception {
        SortingTopNProjector projector = new SortingTopNProjector(
                new Input<?>[]{INPUT, TRUE_LITERAL},
                new CollectExpression[]{(CollectExpression<?>)INPUT},
                2,
                new int[]{0},
                new boolean[]{false},
                TopN.NO_LIMIT,
                TopN.NO_OFFSET);
        projector.startProjection();
        int i;
        for (i = 10; i>0; i--) {   // 10 --> 1
            if (!projector.setNextRow(i)) {
                break;
            }
        }
        assertThat(i, is(0)); // needs to collect all it can get
        projector.finishProjection();
        Object[][] rows = projector.getRows();
        assertThat(rows.length, is(10));
        for (int j = 0; j<10; j++) {
            // 1 --> 10
            assertThat((Integer)rows[j][0], is(j+1));
        }

        int iterateLength = 0;
        for (Object[] row : projector) {
            assertThat((Integer)row[0], is(iterateLength+1));
            iterateLength++;
        }
        assertThat(iterateLength, is(10));
    }

    @Test
    public void testWithHighOffset() throws Exception {
        SortingTopNProjector projector = new SortingTopNProjector(
                new Input<?>[] { INPUT, TRUE_LITERAL },
                new CollectExpression[] { (CollectExpression<?>)INPUT },
                2,
                new int[] { 0 },
                new boolean[] { false },
                2,
                30
        );

        projector.startProjection();
        for (int i = 0; i < 10; i++) {
            if (!projector.setNextRow(i)) {
                break;
            }
        }

        projector.finishProjection();
        assertThat(projector.getRows().length, is(0));
    }

    @Test
    public void testOrderByWithoutLimit() throws Exception {
        SortingTopNProjector projector = new SortingTopNProjector(
                new Input<?>[]{INPUT, TRUE_LITERAL},
                new CollectExpression[]{(CollectExpression<?>)INPUT},
                2,
                new int[]{0},
                new boolean[]{false},
                TopN.NO_LIMIT,
                5);
        projector.startProjection();
        int i;
        for (i = 10; i>0; i--) {   // 10 --> 1
            if (!projector.setNextRow(i)) {
                break;
            }
        }
        assertThat(i, is(0)); // needs to collect all it can get
        projector.finishProjection();
        Object[][] rows = projector.getRows();
        assertThat(rows.length, is(5));
        for (int j = 0; j<5; j++) {
            assertThat((Integer)rows[j][0], is(j+6));
        }

        int iterateLength = 0;
        for (Object[] row : projector) {
            assertThat((Integer)row[0], is(iterateLength+6));
            iterateLength++;
        }
        assertThat(iterateLength, is(5));
    }

    @Test
    public void testOrderBy() throws Exception {
        SortingTopNProjector projector = new SortingTopNProjector(
                new Input<?>[]{INPUT, TRUE_LITERAL},
                new CollectExpression[]{(CollectExpression<?>)INPUT},
                1,
                new int[]{0}, new boolean[]{false},
                3,
                5);
        projector.startProjection();
        int i;
        for (i = 10; i>0; i--) {   // 10 --> 1
            projector.setNextRow(i);
        }
        assertThat(i, is(0)); // needs to collect all it can get
        projector.finishProjection();
        Object[][] rows = projector.getRows();
        assertThat(rows.length, is(3));
        assertThat(rows[0].length, is(1));
        for (int j = 0; j<3; j++) {
            assertThat((Integer)rows[j][0], is(j+6));
        }

        int iterateLength = 0;
        for (Object[] row : projector) {
            assertThat((Integer)row[0], is(iterateLength+6));
            iterateLength++;
        }
        assertThat(iterateLength, is(3));
    }

    @Test
    public void testOrderByAsc() throws Exception {
        SortingTopNProjector projector = new SortingTopNProjector(
                new Input<?>[]{INPUT, TRUE_LITERAL},
                new CollectExpression[]{(CollectExpression<?>)INPUT},
                2,
                new int[]{0},
                new boolean[]{true},
                3,
                5);
        projector.startProjection();
        int i;
        for (i = 0; i<10; i++) {   // 0 --> 9
            projector.setNextRow(i);
        }
        assertThat(i, is(10)); // needs to collect all it can get
        projector.finishProjection();
        Object[][] rows = projector.getRows();
        assertThat(rows.length, is(3));

        assertThat((Integer)rows[0][0], is(4));
        assertThat((Integer)rows[1][0], is(3));
        assertThat((Integer)rows[2][0], is(2));

        int iterateLength = 0;
        int value = 4;
        for (Object[] row : projector) {
            assertThat((Integer)row[0], is(value--));
            iterateLength++;
        }
        assertThat(iterateLength, is(3));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNegativeOffset() {
        new SortingTopNProjector(new Input<?>[]{INPUT, TRUE_LITERAL}, new CollectExpression[]{(CollectExpression<?>)INPUT}, 2,
                new int[]{0}, new boolean[]{true}, TopN.NO_LIMIT, -10);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNegativeLimit() {
        new SortingTopNProjector(new Input<?>[]{INPUT, TRUE_LITERAL}, new CollectExpression[]{(CollectExpression<?>)INPUT}, 2,
                new int[]{0}, new boolean[]{true}, -100, TopN.NO_OFFSET);
    }

    @Test
    public void testMultipleOrderBy() throws Exception {
        // select modulo(bla, 4), bla from x order by modulo(bla, 4), bla
        Input<Integer> input = new InputCollectExpression<>(1);
        SortingTopNProjector projector = new SortingTopNProjector(
                new Input<?>[]{input, INPUT, /* order By input */input, INPUT},
                new CollectExpression[]{(CollectExpression<?>)INPUT, (CollectExpression<?>)input},
                2,
                new int[]{2, 3},
                new boolean[]{false, false},
                TopN.NO_LIMIT,
                TopN.NO_OFFSET);
        projector.startProjection();
        int i;
        for (i=0; i<20; i++) {
            projector.setNextRow(i, i%4);
        }
        assertThat(i, is(20));
        projector.finishProjection();
        Object[][] rows = projector.getRows();
        assertThat(rows.length, is(20));

        Object[] formerRow = null;
        for (Object[] row : rows) {
            if (formerRow==null) { formerRow = row; continue; }
            assertThat((Integer)formerRow[0], lessThanOrEqualTo((Integer)row[0]));
            if ((formerRow[0]).equals(row[0])) {
                assertThat((Integer)formerRow[1], lessThanOrEqualTo((Integer)row[1]));
            }
            formerRow = row;
        }

        int iterateLength = 0;
        formerRow = null;
        for (Object[] row : projector) {
            iterateLength++;
            if (formerRow==null) { formerRow = row; continue; }
            assertThat((Integer)formerRow[0], lessThanOrEqualTo((Integer)row[0]));
            if ((formerRow[0]).equals(row[0])) {
                assertThat((Integer)formerRow[1], lessThanOrEqualTo((Integer)row[1]));
            }
            formerRow = row;
        }
        assertThat(iterateLength, is(20));

    }
}
