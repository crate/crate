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
import com.google.common.collect.Iterables;
import io.crate.core.collections.Bucket;
import io.crate.core.collections.Row;
import io.crate.core.collections.Row1;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.Scalar;
import io.crate.operation.Input;
import io.crate.operation.aggregation.FunctionExpression;
import io.crate.operation.collect.CollectExpression;
import io.crate.operation.collect.InputCollectExpression;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.CollectingRowReceiver;
import io.crate.testing.RowSender;
import io.crate.testing.TestingHelpers;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static io.crate.testing.TestingHelpers.isRow;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.is;

public class SimpleTopNProjectorTest extends CrateUnitTest {

    private static final InputCollectExpression input = new InputCollectExpression(0);
    private static final ImmutableList<Input<?>> INPUTS = ImmutableList.<Input<?>>of(input);
    private static final List<CollectExpression<Row, ?>> COLLECT_EXPRESSIONS = Collections.<CollectExpression<Row, ?>>singletonList(input);
    private static final Row row = new Row1(42.3);

    private SimpleTopNProjector preparePipe(int limit, int offset, CollectingRowReceiver rowReceiver) {
        SimpleTopNProjector pipe = new SimpleTopNProjector(INPUTS, COLLECT_EXPRESSIONS, limit, offset);
        pipe.downstream(rowReceiver);
        return pipe;
    }

    @Test
    public void testProjectLimitOnly() throws Throwable {
        CollectingRowReceiver rowReceiver = new CollectingRowReceiver();
        Projector pipe = preparePipe(10, TopN.NO_OFFSET, rowReceiver);
        RowSender.generateRowsInRangeAndEmit(0, 12, pipe);

        Bucket projected = rowReceiver.result();
        assertThat(projected.size(), is(10));

        int iterateLength = Iterables.size(rowReceiver.result());
        assertThat(iterateLength, is(10));
    }

    @Test
    public void testProjectLimitOnlyLessThanLimit() throws Throwable {
        CollectingRowReceiver rowReceiver = new CollectingRowReceiver();
        Projector pipe = preparePipe(10, TopN.NO_OFFSET, rowReceiver);
        RowSender.generateRowsInRangeAndEmit(0, 5, pipe);

        pipe.finish(RepeatHandle.UNSUPPORTED);
        Bucket projected = rowReceiver.result();
        assertThat(projected.size(), is(5));

        int iterateLength = Iterables.size(rowReceiver.result());
        assertThat(iterateLength, is(5));
    }

    @Test
    public void testProjectLimitOnlyExactlyLimit() throws Throwable {
        CollectingRowReceiver rowReceiver = new CollectingRowReceiver();
        Projector pipe = preparePipe(10, TopN.NO_OFFSET, rowReceiver);
        RowSender.generateRowsInRangeAndEmit(0, 10, pipe);
        Bucket projected = rowReceiver.result();
        assertThat(projected.size(), is(10));

        int iterateLength = Iterables.size(rowReceiver.result());
        assertThat(iterateLength, is(10));

    }

    @Test
    public void testProjectLimitOnly0() throws Throwable {
        CollectingRowReceiver rowReceiver = new CollectingRowReceiver();
        Projector pipe = preparePipe(10, TopN.NO_OFFSET, rowReceiver);

        pipe.finish(RepeatHandle.UNSUPPORTED);
        Bucket projected = rowReceiver.result();
        assertThat(projected, emptyIterable());

        int iterateLength = Iterables.size(rowReceiver.result());
        assertThat(iterateLength, is(0));
    }

    @Test
    public void testProjectLimitOnly1() throws Throwable {
        CollectingRowReceiver rowReceiver = new CollectingRowReceiver();
        Projector pipe = preparePipe(1, TopN.NO_OFFSET, rowReceiver);
        RowSender.generateRowsInRangeAndEmit(0, 10, pipe);

        Bucket projected = rowReceiver.result();
        assertThat(projected.size(), is(1));

        int iterateLength = Iterables.size(rowReceiver.result());
        assertThat(iterateLength, is(1));
    }

    @Test
    public void testProjectOffsetBigger0() throws Throwable {
        CollectingRowReceiver rowReceiver = new CollectingRowReceiver();
        Projector pipe = preparePipe(100, 10, rowReceiver);
        RowSender.generateRowsInRangeAndEmit(0, 100, pipe);

        Bucket projected = rowReceiver.result();
        assertThat(projected.size(), is(90));

        int iterateLength = Iterables.size(rowReceiver.result());
        assertThat(iterateLength, is(90));
    }

    @Test
    public void testNegativeOffset() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Invalid OFFSET");
        new SimpleTopNProjector(INPUTS, COLLECT_EXPRESSIONS, 10, -10);
    }

    @Test
    public void testNegativeLimit() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Invalid LIMIT");
        new SimpleTopNProjector(INPUTS, COLLECT_EXPRESSIONS, -100, TopN.NO_OFFSET);
    }

    @Test
    public void testFunctionExpression() throws Throwable {
        Scalar floor = (Scalar) TestingHelpers.getFunctions().get(
            new FunctionIdent("floor", Collections.<DataType>singletonList(DataTypes.DOUBLE)));
        FunctionExpression<Number, ?> funcExpr = new FunctionExpression<>(floor, new Input[]{input});
        CollectingRowReceiver rowReceiver = new CollectingRowReceiver();
        Projector pipe = new SimpleTopNProjector(ImmutableList.<Input<?>>of(funcExpr), COLLECT_EXPRESSIONS, 10, TopN.NO_OFFSET);
        pipe.downstream(rowReceiver);
        int i;
        for (i = 0; i < 12; i++) {
            if (pipe.setNextRow(row) == RowReceiver.Result.STOP) {
                break;
            }
        }
        assertThat(i, is(9));
        pipe.finish(RepeatHandle.UNSUPPORTED);
        Bucket rows = rowReceiver.result();
        assertThat(rows.size(), is(10));
        assertThat(rows.iterator().next(), isRow(42L));
    }

    @Test
    public void testProjectLimitOnlyUpStream() throws Throwable {
        CollectingRowReceiver rowReceiver = new CollectingRowReceiver();
        Projector pipe = preparePipe(10, TopN.NO_OFFSET, rowReceiver);
        RowSender.generateRowsInRangeAndEmit(0, 12, pipe);
        Bucket projected = rowReceiver.result();
        assertThat(projected.size(), is(10));

        int iterateLength = Iterables.size(rowReceiver.result());
        assertThat(iterateLength, is(10));
    }

    @Test
    public void testProjectLimitLessThanLimitUpStream() throws Throwable {
        CollectingRowReceiver rowReceiver = new CollectingRowReceiver();
        Projector pipe = preparePipe(10, TopN.NO_OFFSET, rowReceiver);
        RowSender.generateRowsInRangeAndEmit(0, 5, pipe);
        Bucket projected = rowReceiver.result();
        assertThat(projected.size(), is(5));

        int iterateLength = Iterables.size(rowReceiver.result());
        assertThat(iterateLength, is(5));
    }


    @Test
    public void testProjectLimitOnly0UpStream() throws Throwable {
        CollectingRowReceiver rowReceiver = new CollectingRowReceiver();
        Projector pipe = preparePipe(10, TopN.NO_OFFSET, rowReceiver);
        pipe.finish(RepeatHandle.UNSUPPORTED);
        Bucket projected = rowReceiver.result();
        assertThat(projected, emptyIterable());
    }

    @Test
    public void testProjectOffsetBigger0UpStream() throws Throwable {
        CollectingRowReceiver rowReceiver = new CollectingRowReceiver();
        Projector pipe = preparePipe(100, 10, rowReceiver);
        RowSender.generateRowsInRangeAndEmit(0, 100, pipe);
        Bucket projected = rowReceiver.result();
        assertThat(projected.size(), is(90));

        int iterateLength = Iterables.size(rowReceiver.result());
        assertThat(iterateLength, is(90));
    }

    @Test
    public void testProjectNoLimitNoOffset() throws Throwable {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Invalid LIMIT");
        CollectingRowReceiver rowReceiver = new CollectingRowReceiver();

        preparePipe(TopN.NO_LIMIT, TopN.NO_OFFSET, rowReceiver);
    }
}
