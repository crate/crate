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
import io.crate.data.*;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.Scalar;
import io.crate.operation.aggregation.FunctionExpression;
import io.crate.operation.collect.CollectExpression;
import io.crate.operation.collect.InputCollectExpression;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.RowGenerator;
import io.crate.testing.TestingBatchConsumer;
import io.crate.testing.TestingBatchIterators;
import io.crate.testing.TestingHelpers;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;

import static io.crate.testing.TestingHelpers.isRow;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.is;

public class SimpleTopNProjectorTest extends CrateUnitTest {

    private static final InputCollectExpression input = new InputCollectExpression(0);
    private static final ImmutableList<Input<?>> INPUTS = ImmutableList.<Input<?>>of(input);
    private static final List<CollectExpression<Row, ?>> COLLECT_EXPRESSIONS = Collections.<CollectExpression<Row, ?>>singletonList(input);

    private TestingBatchConsumer consumer = new TestingBatchConsumer();

    private SimpleTopNProjector prepareProjector(int limit, int offset) {
        return new SimpleTopNProjector(INPUTS, COLLECT_EXPRESSIONS, limit, offset);
    }

    @Test
    public void testProjectLimitOnly() throws Throwable {
        Projector projector = prepareProjector(10, TopN.NO_OFFSET);

        BatchIterator batchIterator = projector.apply(TestingBatchIterators.range(0, 12));
        consumer.accept(batchIterator, null);

        Bucket projected = consumer.getBucket();
        assertThat(projected.size(), is(10));

        int iterateLength = Iterables.size(consumer.getBucket());
        assertThat(iterateLength, is(10));
    }

    @Test
    public void testProjectLimitOnlyLessThanLimit() throws Throwable {
        Projector projector = prepareProjector(10, TopN.NO_OFFSET);
        BatchIterator batchIterator = projector.apply(TestingBatchIterators.range(0, 5));
        consumer.accept(batchIterator, null);

        Bucket projected = consumer.getBucket();
        assertThat(projected.size(), is(5));

        int iterateLength = Iterables.size(consumer.getBucket());
        assertThat(iterateLength, is(5));
    }

    @Test
    public void testProjectLimitOnlyExactlyLimit() throws Throwable {
        Projector projector = prepareProjector(10, TopN.NO_OFFSET);
        BatchIterator batchIterator = projector.apply(TestingBatchIterators.range(0, 10));
        consumer.accept(batchIterator, null);
        Bucket projected = consumer.getBucket();
        assertThat(projected.size(), is(10));

        int iterateLength = Iterables.size(consumer.getBucket());
        assertThat(iterateLength, is(10));

    }

    @Test
    public void testProjectLimitOnly0() throws Throwable {
        Projector projector = prepareProjector(10, TopN.NO_OFFSET);
        consumer.accept(projector.apply(RowsBatchIterator.empty()), null);

        Bucket projected = consumer.getBucket();
        assertThat(projected, emptyIterable());

        int iterateLength = Iterables.size(consumer.getBucket());
        assertThat(iterateLength, is(0));
    }

    @Test
    public void testProjectLimitOnly1() throws Throwable {
        Projector projector = prepareProjector(1, TopN.NO_OFFSET);
        BatchIterator batchIterator = projector.apply(TestingBatchIterators.range(0, 10));
        consumer.accept(batchIterator, null);

        Bucket projected = consumer.getBucket();
        assertThat(projected.size(), is(1));

        int iterateLength = Iterables.size(consumer.getBucket());
        assertThat(iterateLength, is(1));
    }

    @Test
    public void testProjectOffsetBigger0() throws Throwable {
        Projector projector = prepareProjector(100, 10);
        BatchIterator batchIterator = projector.apply(TestingBatchIterators.range(0, 100));
        consumer.accept(batchIterator, null);

        Bucket projected = consumer.getBucket();
        assertThat(projected.size(), is(90));

        int iterateLength = Iterables.size(consumer.getBucket());
        assertThat(iterateLength, is(90));
    }

    @Test
    public void testNegativeOffset() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("invalid offset");
        new SimpleTopNProjector(INPUTS, COLLECT_EXPRESSIONS, 10, -10);
    }

    @Test
    public void testNegativeLimit() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("invalid limit");
        new SimpleTopNProjector(INPUTS, COLLECT_EXPRESSIONS, -100, TopN.NO_OFFSET);
    }

    @Test
    public void testFunctionExpression() throws Throwable {
        Scalar floor = (Scalar) TestingHelpers.getFunctions().get(
            new FunctionIdent("floor", Collections.<DataType>singletonList(DataTypes.DOUBLE)));
        FunctionExpression<Number, ?> funcExpr = new FunctionExpression<>(floor, new Input[]{input});
        Projector projector = new SimpleTopNProjector(ImmutableList.<Input<?>>of(funcExpr), COLLECT_EXPRESSIONS, 10, TopN.NO_OFFSET);

        Iterable<Row> rows = RowGenerator.fromSingleColValues(
            () -> IntStream.range(0, 12).mapToDouble(i -> 42.3d).iterator());

        BatchIterator batchIterator = projector.apply(RowsBatchIterator.newInstance(rows, 1));
        consumer.accept(batchIterator, null);
        Bucket result = consumer.getBucket();
        assertThat(result.size(), is(10));
        assertThat(result.iterator().next(), isRow(42L));
    }

    @Test
    public void testProjectLimitOnlyUpStream() throws Throwable {
        Projector projector = prepareProjector(10, TopN.NO_OFFSET);
        BatchIterator batchIterator = projector.apply(TestingBatchIterators.range(0, 12));
        consumer.accept(batchIterator, null);
        Bucket projected = consumer.getBucket();
        assertThat(projected.size(), is(10));

        int iterateLength = Iterables.size(consumer.getBucket());
        assertThat(iterateLength, is(10));
    }

    @Test
    public void testProjectLimitLessThanLimitUpStream() throws Throwable {
        Projector projector = prepareProjector(10, TopN.NO_OFFSET);
        BatchIterator batchIterator = projector.apply(TestingBatchIterators.range(0, 5));
        consumer.accept(batchIterator, null);
        Bucket projected = consumer.getBucket();
        assertThat(projected.size(), is(5));

        int iterateLength = Iterables.size(consumer.getBucket());
        assertThat(iterateLength, is(5));
    }


    @Test
    public void testProjectLimitOnly0UpStream() throws Throwable {
        Projector projector = prepareProjector(10, TopN.NO_OFFSET);
        consumer.accept(projector.apply(RowsBatchIterator.empty()), null);
        Bucket projected = consumer.getBucket();
        assertThat(projected, emptyIterable());
    }

    @Test
    public void testProjectOffsetBigger0UpStream() throws Throwable {
        Projector projector = prepareProjector(100, 10);
        BatchIterator batchIterator = projector.apply(TestingBatchIterators.range(0, 100));
        consumer.accept(batchIterator, null);
        Bucket projected = consumer.getBucket();
        assertThat(projected.size(), is(90));

        int iterateLength = Iterables.size(consumer.getBucket());
        assertThat(iterateLength, is(90));
    }

    @Test
    public void testProjectNoLimitNoOffset() throws Throwable {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("invalid limit");

        prepareProjector(TopN.NO_LIMIT, TopN.NO_OFFSET);
    }
}
