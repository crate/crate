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

package io.crate.execution.engine.pipeline;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.crate.data.BatchIterator;
import io.crate.data.Bucket;
import io.crate.data.InMemoryBatchIterator;
import io.crate.data.Input;
import io.crate.data.Projector;
import io.crate.data.Row;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.execution.engine.collect.InputCollectExpression;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.TestingBatchIterators;
import io.crate.testing.TestingRowConsumer;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static io.crate.data.SentinelRow.SENTINEL;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.is;

public class SimpleTopNProjectorTest extends CrateUnitTest {

    private static final InputCollectExpression input = new InputCollectExpression(0);
    private static final ImmutableList<Input<?>> INPUTS = ImmutableList.of(input);
    private static final List<CollectExpression<Row, ?>> COLLECT_EXPRESSIONS = Collections.singletonList(input);

    private TestingRowConsumer consumer = new TestingRowConsumer();

    private SimpleTopNProjector prepareProjector(int limit, int offset) {
        return new SimpleTopNProjector(limit, offset);
    }

    @Test
    public void testProjectLimitOnly() throws Throwable {
        Projector projector = prepareProjector(10, TopN.NO_OFFSET);

        BatchIterator<Row> batchIterator = projector.apply(TestingBatchIterators.range(0, 12));
        consumer.accept(batchIterator, null);

        Bucket projected = consumer.getBucket();
        assertThat(projected.size(), is(10));

        int iterateLength = Iterables.size(consumer.getBucket());
        assertThat(iterateLength, is(10));
    }

    @Test
    public void testProjectLimitOnlyLessThanLimit() throws Throwable {
        Projector projector = prepareProjector(10, TopN.NO_OFFSET);
        BatchIterator<Row> batchIterator = projector.apply(TestingBatchIterators.range(0, 5));
        consumer.accept(batchIterator, null);

        Bucket projected = consumer.getBucket();
        assertThat(projected.size(), is(5));

        int iterateLength = Iterables.size(consumer.getBucket());
        assertThat(iterateLength, is(5));
    }

    @Test
    public void testProjectLimitOnlyExactlyLimit() throws Throwable {
        Projector projector = prepareProjector(10, TopN.NO_OFFSET);
        BatchIterator<Row> batchIterator = projector.apply(TestingBatchIterators.range(0, 10));
        consumer.accept(batchIterator, null);
        Bucket projected = consumer.getBucket();
        assertThat(projected.size(), is(10));

        int iterateLength = Iterables.size(consumer.getBucket());
        assertThat(iterateLength, is(10));

    }

    @Test
    public void testProjectLimitOnly0() throws Throwable {
        Projector projector = prepareProjector(10, TopN.NO_OFFSET);
        consumer.accept(projector.apply(InMemoryBatchIterator.empty(SENTINEL)), null);

        Bucket projected = consumer.getBucket();
        assertThat(projected, emptyIterable());

        int iterateLength = Iterables.size(consumer.getBucket());
        assertThat(iterateLength, is(0));
    }

    @Test
    public void testProjectLimitOnly1() throws Throwable {
        Projector projector = prepareProjector(1, TopN.NO_OFFSET);
        BatchIterator<Row> batchIterator = projector.apply(TestingBatchIterators.range(0, 10));
        consumer.accept(batchIterator, null);

        Bucket projected = consumer.getBucket();
        assertThat(projected.size(), is(1));

        int iterateLength = Iterables.size(consumer.getBucket());
        assertThat(iterateLength, is(1));
    }

    @Test
    public void testProjectOffsetBigger0() throws Throwable {
        Projector projector = prepareProjector(100, 10);
        BatchIterator<Row> batchIterator = projector.apply(TestingBatchIterators.range(0, 100));
        consumer.accept(batchIterator, null);

        Bucket projected = consumer.getBucket();
        assertThat(projected.size(), is(90));

        int iterateLength = Iterables.size(consumer.getBucket());
        assertThat(iterateLength, is(90));
    }

    @Test
    public void testNegativeOffset() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Invalid OFFSET");
        new SimpleTopNProjector(10, -10);
    }

    @Test
    public void testNegativeLimit() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Invalid LIMIT");
        new SimpleTopNProjector(-100, TopN.NO_OFFSET);
    }

    @Test
    public void testProjectLimitOnlyUpStream() throws Throwable {
        Projector projector = prepareProjector(10, TopN.NO_OFFSET);
        BatchIterator<Row> batchIterator = projector.apply(TestingBatchIterators.range(0, 12));
        consumer.accept(batchIterator, null);
        Bucket projected = consumer.getBucket();
        assertThat(projected.size(), is(10));

        int iterateLength = Iterables.size(consumer.getBucket());
        assertThat(iterateLength, is(10));
    }

    @Test
    public void testProjectLimitLessThanLimitUpStream() throws Throwable {
        Projector projector = prepareProjector(10, TopN.NO_OFFSET);
        BatchIterator<Row> batchIterator = projector.apply(TestingBatchIterators.range(0, 5));
        consumer.accept(batchIterator, null);
        Bucket projected = consumer.getBucket();
        assertThat(projected.size(), is(5));

        int iterateLength = Iterables.size(consumer.getBucket());
        assertThat(iterateLength, is(5));
    }


    @Test
    public void testProjectLimitOnly0UpStream() throws Throwable {
        Projector projector = prepareProjector(10, TopN.NO_OFFSET);
        consumer.accept(projector.apply(InMemoryBatchIterator.empty(SENTINEL)), null);
        Bucket projected = consumer.getBucket();
        assertThat(projected, emptyIterable());
    }

    @Test
    public void testProjectOffsetBigger0UpStream() throws Throwable {
        Projector projector = prepareProjector(100, 10);
        BatchIterator<Row> batchIterator = projector.apply(TestingBatchIterators.range(0, 100));
        consumer.accept(batchIterator, null);
        Bucket projected = consumer.getBucket();
        assertThat(projected.size(), is(90));

        int iterateLength = Iterables.size(consumer.getBucket());
        assertThat(iterateLength, is(90));
    }

    @Test
    public void testProjectNoLimitNoOffset() throws Throwable {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Invalid LIMIT");

        prepareProjector(TopN.NO_LIMIT, TopN.NO_OFFSET);
    }
}
