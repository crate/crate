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

import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.operator.Input;
import io.crate.operator.InputCollectExpression;
import io.crate.operator.aggregation.CollectExpression;
import io.crate.operator.aggregation.FunctionExpression;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Symbol;
import org.cratedb.Constants;
import org.cratedb.DataType;
import org.junit.Test;

import java.util.Arrays;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;

public class SimpleTopNProjectorTest {

    private static final Input<Integer> input = new InputCollectExpression<>(0);
    private static final Object[] row = new Object[]{42};

    private static class TestFunction implements Scalar<Integer, Integer> {

        public static final String NAME = "signum";
        public static final FunctionInfo INFO = new FunctionInfo(
                new FunctionIdent(NAME, Arrays.asList(DataType.INTEGER)), DataType.INTEGER);

        @Override
        public Integer evaluate(Input<Integer>... args) {
            Integer result = null;
            if (args != null && args.length > 0) {
                Integer value = args[0].value();
                if (value != null) {
                    result = (int) Math.signum(value);
                }
            }
            return result;
        }


        @Override
        public FunctionInfo info() {
            return INFO;
        }

        @Override
        public Symbol normalizeSymbol(Function symbol) {
            return symbol;
        }
    }


    @Test
    public void testProjectLimitOnly() {
        SimpleTopNProjector projector = new SimpleTopNProjector(new Input<?>[]{input},
                new CollectExpression[]{(CollectExpression)input}, 10, TopN.NO_OFFSET);
        projector.startProjection();
        int i;
        for (i = 0; i<12; i++) {
            if (!projector.setNextRow(row)) {
                break;
            }
        }
        assertThat(i, is(10));
        projector.finishProjection();
        Object[][] projected = projector.getRows();
        assertThat(projected.length, is(10));

        int iterateLength = 0;
        for (Object[] row : projector) {
            iterateLength++;
        }
        assertThat(iterateLength, is(10));

    }

    @Test
    public void testProjectLimitOnlyLessThanLimit() {
        SimpleTopNProjector projector = new SimpleTopNProjector(new Input<?>[]{input},
                new CollectExpression[]{(CollectExpression)input}, 10, TopN.NO_OFFSET);
        projector.startProjection();
        int i;
        for (i = 0; i<5; i++) {
            if (!projector.setNextRow(row)) {
                break;
            }
        }
        assertThat(i, is(5));
        projector.finishProjection();
        Object[][] projected = projector.getRows();
        assertThat(projected.length, is(5));

        int iterateLength = 0;
        for (Object[] row : projector) {
            iterateLength++;
        }
        assertThat(iterateLength, is(5));

    }

    @Test
    public void testProjectLimitOnlyExactlyLimit() {
        SimpleTopNProjector projector = new SimpleTopNProjector(new Input<?>[]{input},
                new CollectExpression[]{(CollectExpression)input}, 10, TopN.NO_OFFSET);
        projector.startProjection();
        int i;
        for (i = 0; i<10; i++) {
            if (!projector.setNextRow(row)) {
                break;
            }
        }
        assertThat(i, is(10));
        projector.finishProjection();
        Object[][] projected = projector.getRows();
        assertThat(projected.length, is(10));

        int iterateLength = 0;
        for (Object[] row : projector) {
            iterateLength++;
        }
        assertThat(iterateLength, is(10));

    }

    @Test
    public void testProjectLimitOnly0() {
        SimpleTopNProjector projector = new SimpleTopNProjector(new Input<?>[]{input},
                new CollectExpression[]{(CollectExpression)input}, 10, TopN.NO_OFFSET);
        projector.startProjection();
        projector.finishProjection();
        Object[][] projected = projector.getRows();
        assertArrayEquals(Constants.EMPTY_RESULT, projected);

        int iterateLength = 0;
        for (Object[] row : projector) {
            iterateLength++;
        }
        assertThat(iterateLength, is(0));
    }

    @Test
    public void testProjectLimitOnly1() {
        SimpleTopNProjector projector = new SimpleTopNProjector(new Input<?>[]{input},
                new CollectExpression[]{(CollectExpression)input}, 1, TopN.NO_OFFSET);
        projector.startProjection();
        int i;
        for (i = 0; i<10; i++) {
            if (!projector.setNextRow(row)) {
                break;
            }
        }
        assertThat(i, is(1));
        projector.finishProjection();
        Object[][] projected = projector.getRows();
        assertThat(projected.length, is(1));

        int iterateLength = 0;
        for (Object[] row : projector) {
            iterateLength++;
        }
        assertThat(iterateLength, is(1));
    }

    @Test
    public void testProjectOffsetBigger0() {
        SimpleTopNProjector projector = new SimpleTopNProjector(new Input<?>[]{input},
                new CollectExpression[]{(CollectExpression)input}, 100, 10);
        projector.startProjection();
        int i;
        for (i = 0; i<100;i++) {
            projector.setNextRow(row);
        }
        assertThat(i, is(100));
        projector.finishProjection();
        Object[][] projected = projector.getRows();
        assertThat(projected.length, is(90));

        int iterateLength = 0;
        for (Object[] row : projector) {
            iterateLength++;
        }
        assertThat(iterateLength, is(90));
    }

    @Test
    public void testProjectNoLimitNoOffset() {
        SimpleTopNProjector projector = new SimpleTopNProjector(new Input<?>[]{input},
                new CollectExpression[]{(CollectExpression)input},
                TopN.NO_LIMIT, TopN.NO_OFFSET);
        projector.startProjection();
        int i = 0;
        boolean carryOn;
        do {
            i++;
            carryOn = projector.setNextRow(row);
        } while(carryOn);
        assertThat(i, is(Constants.DEFAULT_SELECT_LIMIT+1));
        projector.finishProjection();
        assertThat(projector.getRows().length, is(Constants.DEFAULT_SELECT_LIMIT));

        int iterateLength = 0;
        for (Object[] row : projector) {
            iterateLength++;
        }
        assertThat(iterateLength, is(Constants.DEFAULT_SELECT_LIMIT));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNegativeOffset() {
        new SimpleTopNProjector(new Input<?>[]{input}, new CollectExpression[]{(CollectExpression)input},
                TopN.NO_LIMIT, -10);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNegativeLimit() {
        new SimpleTopNProjector(new Input<?>[]{input}, new CollectExpression[]{(CollectExpression)input},
                -100, TopN.NO_OFFSET);
    }

    @Test
    public void testFunctionExpression() {
        FunctionExpression<Integer, ?> funcExpr = new FunctionExpression<>(new TestFunction(), new Input[]{input});
        SimpleTopNProjector projector = new SimpleTopNProjector(new Input<?>[]{funcExpr}, new CollectExpression[]{(CollectExpression)input}, 10, TopN.NO_OFFSET);
        projector.startProjection();
        int i;
        for (i = 0; i<12;i++) {
            if (!projector.setNextRow(row)) {
                break;
            }
        }
        assertThat(i, is(10));
        projector.finishProjection();
        Object[][] rows = projector.getRows();
        assertThat(rows.length, is(10));
        assertThat((Integer)rows[0][0], is(1));

        int iterateLength = 0;
        for (Object[] row : projector) {
            iterateLength++;
        }
        assertThat(iterateLength, is(10));

    }

    @Test
    public void testProjectLimitOnlyUpStream() {
        SimpleTopNProjector projector = new SimpleTopNProjector(new Input<?>[]{input},
                new CollectExpression[]{(CollectExpression)input}, 10, TopN.NO_OFFSET);
        NoopProjector noop = new NoopProjector();
        projector.setDownStream(noop);
        projector.startProjection();
        int i;
        for (i = 0; i<12; i++) {
            if (!projector.setNextRow(row)) {
                break;
            }
        }
        assertThat(i, is(10));
        projector.finishProjection();
        Object[][] projected = noop.getRows();
        assertThat(projected.length, is(10));

        assertFalse(projector.iterator().hasNext());

        int iterateLength = 0;
        for (Object[] row : noop) {
            iterateLength++;
        }
        assertThat(iterateLength, is(10));
    }

    @Test
    public void testProjectLimitLessThanLimitUpStream() {
        SimpleTopNProjector projector = new SimpleTopNProjector(new Input<?>[]{input},
                new CollectExpression[]{(CollectExpression)input}, 10, TopN.NO_OFFSET);
        NoopProjector noop = new NoopProjector();
        projector.setDownStream(noop);
        projector.startProjection();
        int i;
        for (i = 0; i<5; i++) {
            if (!projector.setNextRow(row)) {
                break;
            }
        }
        assertThat(i, is(5));
        projector.finishProjection();
        Object[][] projected = noop.getRows();
        assertThat(projected.length, is(5));

        assertFalse(projector.iterator().hasNext());

        int iterateLength = 0;
        for (Object[] row : noop) {
            iterateLength++;
        }
        assertThat(iterateLength, is(5));
    }

    @Test
    public void testProjectLimitOnly0UpStream() {
        SimpleTopNProjector projector = new SimpleTopNProjector(new Input<?>[]{input},
                new CollectExpression[]{(CollectExpression)input}, 10, TopN.NO_OFFSET);
        NoopProjector noop = new NoopProjector();
        projector.setDownStream(noop);
        projector.startProjection();
        projector.finishProjection();
        Object[][] projected = noop.getRows();
        assertArrayEquals(Constants.EMPTY_RESULT, projected);

        assertFalse(projector.iterator().hasNext());

        int iterateLength = 0;
        for (Object[] row : noop) {
            iterateLength++;
        }
        assertThat(iterateLength, is(0));
    }

    @Test
    public void testProjectOffsetBigger0UpStream() {
        SimpleTopNProjector projector = new SimpleTopNProjector(new Input<?>[]{input},
                new CollectExpression[]{(CollectExpression)input}, 100, 10);
        NoopProjector noop = new NoopProjector();
        projector.setDownStream(noop);
        projector.startProjection();
        int i;
        for (i = 0; i<100;i++) {
            projector.setNextRow(row);
        }
        assertThat(i, is(100));
        projector.finishProjection();
        Object[][] projected = noop.getRows();
        assertThat(projected.length, is(90));

        assertFalse(projector.iterator().hasNext());

        int iterateLength = 0;
        for (Object[] row : noop) {
            iterateLength++;
        }
        assertThat(iterateLength, is(90));
    }

    @Test
    public void testProjectNoLimitNoOffsetUpStream() {
        SimpleTopNProjector projector = new SimpleTopNProjector(new Input<?>[]{input},
                new CollectExpression[]{(CollectExpression)input},
                TopN.NO_LIMIT, TopN.NO_OFFSET);
        NoopProjector noop = new NoopProjector();
        projector.setDownStream(noop);
        projector.startProjection();
        int i = 0;
        boolean carryOn;
        do {
            i++;
            carryOn = projector.setNextRow(row);
        } while(carryOn);
        assertThat(i, is(Constants.DEFAULT_SELECT_LIMIT+1));
        projector.finishProjection();
        assertThat(noop.getRows().length, is(Constants.DEFAULT_SELECT_LIMIT));

        assertFalse(projector.iterator().hasNext());

        int iterateLength = 0;
        for (Object[] row : noop) {
            iterateLength++;
        }
        assertThat(iterateLength, is(Constants.DEFAULT_SELECT_LIMIT));
    }

}
