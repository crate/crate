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
import io.crate.Constants;
import io.crate.core.collections.Bucket;
import io.crate.core.collections.Row;
import io.crate.core.collections.Row1;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.operation.Input;
import io.crate.operation.aggregation.FunctionExpression;
import io.crate.operation.collect.CollectExpression;
import io.crate.operation.collect.InputCollectExpression;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Symbol;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.CollectingProjector;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.junit.Test;

import java.util.Arrays;

import static io.crate.testing.TestingHelpers.isRow;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.is;

public class SimpleTopNProjectorTest extends CrateUnitTest {

    private static final Input<Integer> input = new InputCollectExpression<>(0);
    private static final Row row = new Row1(42);

    private static class TestFunction extends Scalar<Integer, Integer> {

        public static final String NAME = "signum";
        public static final FunctionInfo INFO = new FunctionInfo(
                new FunctionIdent(NAME, Arrays.<DataType>asList(DataTypes.INTEGER)), DataTypes.INTEGER);

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
    public void testProjectLimitOnly() throws Throwable {
        CollectingProjector collectingProjector = new CollectingProjector();
        SimpleTopNProjector projector = new SimpleTopNProjector(ImmutableList.<Input<?>>of(input),
                new CollectExpression[]{(CollectExpression)input}, 10, TopN.NO_OFFSET);
        projector.downstream(collectingProjector);
        projector.registerUpstream(null);
        projector.startProjection();
        int i;
        for (i = 0; i<12; i++) {
            if (!projector.setNextRow(row)) {
                break;
            }
        }
        assertThat(i, is(9));
        projector.finish();
        Bucket projected = collectingProjector.result().get();
        assertThat(projected.size(), is(10));

        int iterateLength = 0;
        for (Row row : collectingProjector.result().get()) {
            iterateLength++;
        }
        assertThat(iterateLength, is(10));

    }

    @Test
    public void testProjectLimitOnlyLessThanLimit() throws Throwable {
        CollectingProjector collectingProjector = new CollectingProjector();
        SimpleTopNProjector projector = new SimpleTopNProjector(ImmutableList.<Input<?>>of(input),
                new CollectExpression[]{(CollectExpression)input}, 10, TopN.NO_OFFSET);
        projector.downstream(collectingProjector);
        projector.registerUpstream(null);
        projector.startProjection();
        int i;
        for (i = 0; i<5; i++) {
            if (!projector.setNextRow(row)) {
                break;
            }
        }
        assertThat(i, is(5));
        projector.finish();
        Bucket projected = collectingProjector.result().get();
        assertThat(projected.size(), is(5));

        int iterateLength = 0;
        for (Row row : collectingProjector.result().get()) {
            iterateLength++;
        }
        assertThat(iterateLength, is(5));

    }

    @Test
    public void testProjectLimitOnlyExactlyLimit() throws Throwable {
        CollectingProjector collectingProjector = new CollectingProjector();
        SimpleTopNProjector projector = new SimpleTopNProjector(ImmutableList.<Input<?>>of(input),
                new CollectExpression[]{(CollectExpression)input}, 10, TopN.NO_OFFSET);
        projector.downstream(collectingProjector);
        projector.registerUpstream(null);
        projector.startProjection();
        int i;
        for (i = 0; i<10; i++) {
            if (!projector.setNextRow(row)) {
                break;
            }
        }
        assertThat(i, is(9));
        projector.finish();
        Bucket projected = collectingProjector.result().get();
        assertThat(projected.size(), is(10));

        int iterateLength = 0;
        for (Row row : collectingProjector.result().get()) {
            iterateLength++;
        }
        assertThat(iterateLength, is(10));

    }

    @Test
    public void testProjectLimitOnly0() throws Throwable {
        CollectingProjector collectingProjector = new CollectingProjector();
        SimpleTopNProjector projector = new SimpleTopNProjector(ImmutableList.<Input<?>>of(input),
                new CollectExpression[]{(CollectExpression)input}, 10, TopN.NO_OFFSET);
        projector.downstream(collectingProjector);
        projector.registerUpstream(null);
        projector.startProjection();
        projector.finish();
        Bucket projected = collectingProjector.result().get();
        assertThat(projected, emptyIterable());

        int iterateLength = 0;
        for (Row row : collectingProjector.result().get()) {
            iterateLength++;
        }
        assertThat(iterateLength, is(0));
    }

    @Test
    public void testProjectLimitOnly1() throws Throwable {
        CollectingProjector collectingProjector = new CollectingProjector();
        SimpleTopNProjector projector = new SimpleTopNProjector(ImmutableList.<Input<?>>of(input),
                new CollectExpression[]{(CollectExpression)input}, 1, TopN.NO_OFFSET);
        projector.downstream(collectingProjector);
        projector.registerUpstream(null);
        projector.startProjection();
        int i;
        for (i = 0; i<10; i++) {
            if (!projector.setNextRow(row)) {
                break;
            }
        }
        assertThat(i, is(0));
        projector.finish();
        Bucket projected = collectingProjector.result().get();
        assertThat(projected.size(), is(1));

        int iterateLength = 0;
        for (Row row : collectingProjector.result().get()) {
            iterateLength++;
        }
        assertThat(iterateLength, is(1));
    }

    @Test
    public void testProjectOffsetBigger0() throws Throwable {
        CollectingProjector collectingProjector = new CollectingProjector();
        SimpleTopNProjector projector = new SimpleTopNProjector(ImmutableList.<Input<?>>of(input),
                new CollectExpression[]{(CollectExpression)input}, 100, 10);
        projector.downstream(collectingProjector);
        projector.registerUpstream(null);
        projector.startProjection();
        int i;
        for (i = 0; i<100;i++) {
            projector.setNextRow(row);
        }
        assertThat(i, is(100));
        projector.finish();
        Bucket projected = collectingProjector.result().get();
        assertThat(projected.size(), is(90));

        int iterateLength = 0;
        for (Row row : collectingProjector.result().get()) {
            iterateLength++;
        }
        assertThat(iterateLength, is(90));
    }

    @Test
    public void testProjectNoLimitNoOffset() throws Throwable {
        CollectingProjector collectingProjector = new CollectingProjector();
        SimpleTopNProjector projector = new SimpleTopNProjector(ImmutableList.<Input<?>>of(input),
                new CollectExpression[]{(CollectExpression)input},
                TopN.NO_LIMIT, TopN.NO_OFFSET);
        projector.registerUpstream(null);
        projector.downstream(collectingProjector);
        projector.startProjection();
        int i = 0;
        boolean carryOn;
        do {
            i++;
            carryOn = projector.setNextRow(row);
        } while(carryOn);
        assertThat(i, is(Constants.DEFAULT_SELECT_LIMIT));
        projector.finish();
        assertThat(collectingProjector.result().get().size(), is(Constants.DEFAULT_SELECT_LIMIT));

        int iterateLength = 0;
        for (Row row : collectingProjector.result().get()) {
            iterateLength++;
        }
        assertThat(iterateLength, is(Constants.DEFAULT_SELECT_LIMIT));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNegativeOffset() {
        new SimpleTopNProjector(ImmutableList.<Input<?>>of(input), new CollectExpression[]{(CollectExpression)input},
                TopN.NO_LIMIT, -10);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNegativeLimit() {
        new SimpleTopNProjector(ImmutableList.<Input<?>>of(input), new CollectExpression[]{(CollectExpression)input},
                -100, TopN.NO_OFFSET);
    }

    @Test
    public void testFunctionExpression() throws Throwable {
        FunctionExpression<Integer, ?> funcExpr = new FunctionExpression<>(new TestFunction(), new Input[]{input});
        CollectingProjector collectingProjector = new CollectingProjector();
        SimpleTopNProjector projector = new SimpleTopNProjector(ImmutableList.<Input<?>>of(funcExpr),
                new CollectExpression[]{(CollectExpression)input}, 10, TopN.NO_OFFSET);
        projector.downstream(collectingProjector);
        projector.registerUpstream(null);
        projector.startProjection();
        int i;
        for (i = 0; i<12;i++) {
            if (!projector.setNextRow(row)) {
                break;
            }
        }
        assertThat(i, is(9));
        projector.finish();
        Bucket rows = collectingProjector.result().get();
        assertThat(rows.size(), is(10));
        assertThat(rows.iterator().next(), isRow(1));

    }

    @Test
    public void testProjectLimitOnlyUpStream() throws Throwable {
        SimpleTopNProjector projector = new SimpleTopNProjector(ImmutableList.<Input<?>>of(input),
                new CollectExpression[]{(CollectExpression)input}, 10, TopN.NO_OFFSET);
        CollectingProjector noop = new CollectingProjector();
        projector.downstream(noop);
        projector.registerUpstream(null);
        projector.startProjection();
        int i;
        for (i = 0; i<12; i++) {
            if (!projector.setNextRow(row)) {
                break;
            }
        }
        assertThat(i, is(9));
        projector.finish();
        Bucket projected = noop.result().get();
        assertThat(projected.size(), is(10));

        int iterateLength = 0;
        for (Row row : noop.result().get()) {
            iterateLength++;
        }
        assertThat(iterateLength, is(10));
    }

    @Test
    public void testProjectLimitLessThanLimitUpStream() throws Throwable {
        SimpleTopNProjector projector = new SimpleTopNProjector(ImmutableList.<Input<?>>of(input),
                new CollectExpression[]{(CollectExpression)input}, 10, TopN.NO_OFFSET);
        CollectingProjector noop = new CollectingProjector();
        projector.downstream(noop);
        projector.registerUpstream(null);
        projector.startProjection();
        int i;
        for (i = 0; i<5; i++) {
            if (!projector.setNextRow(row)) {
                break;
            }
        }
        assertThat(i, is(5));
        projector.finish();
        Bucket projected = noop.result().get();
        assertThat(projected.size(), is(5));

        int iterateLength = 0;
        for (Row row : noop.result().get()) {
            iterateLength++;
        }
        assertThat(iterateLength, is(5));
    }

    @Test
    public void testProjectLimitOnly0UpStream() throws Throwable {
        SimpleTopNProjector projector = new SimpleTopNProjector(ImmutableList.<Input<?>>of(input),
                new CollectExpression[]{(CollectExpression)input}, 10, TopN.NO_OFFSET);
        CollectingProjector noop = new CollectingProjector();
        projector.downstream(noop);
        projector.registerUpstream(null);
        projector.startProjection();
        projector.finish();
        Bucket projected = noop.result().get();
        assertThat(projected, emptyIterable());
    }

    @Test
    public void testProjectOffsetBigger0UpStream() throws Throwable {
        SimpleTopNProjector projector = new SimpleTopNProjector(ImmutableList.<Input<?>>of(input),
                new CollectExpression[]{(CollectExpression)input}, 100, 10);
        CollectingProjector noop = new CollectingProjector();
        projector.downstream(noop);
        projector.registerUpstream(null);
        projector.startProjection();
        int i;
        for (i = 0; i<100;i++) {
            projector.setNextRow(row);
        }
        assertThat(i, is(100));
        projector.finish();
        Bucket projected = noop.result().get();
        assertThat(projected.size(), is(90));

        int iterateLength = 0;
        for (Row row : noop.result().get()) {
            iterateLength++;
        }
        assertThat(iterateLength, is(90));
    }

    @Test
    public void testProjectNoLimitNoOffsetUpStream() throws Throwable {
        SimpleTopNProjector projector = new SimpleTopNProjector(ImmutableList.<Input<?>>of(input),
                new CollectExpression[]{(CollectExpression)input},
                TopN.NO_LIMIT, TopN.NO_OFFSET);
        CollectingProjector noop = new CollectingProjector();
        projector.downstream(noop);
        projector.registerUpstream(null);
        projector.startProjection();
        int i = 0;
        boolean carryOn;
        do {
            i++;
            carryOn = projector.setNextRow(row);
        } while(carryOn);
        assertThat(i, is(Constants.DEFAULT_SELECT_LIMIT));
        projector.finish();
        assertThat(noop.result().get().size(), is(Constants.DEFAULT_SELECT_LIMIT));

        int iterateLength = 0;
        for (Row row : noop.result().get()) {
            iterateLength++;
        }
        assertThat(iterateLength, is(Constants.DEFAULT_SELECT_LIMIT));
    }
}
