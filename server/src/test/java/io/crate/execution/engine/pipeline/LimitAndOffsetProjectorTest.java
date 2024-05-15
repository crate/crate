/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.execution.engine.pipeline;

import static io.crate.data.SentinelRow.SENTINEL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.emptyIterable;
import static org.junit.Assert.assertThat;

import java.util.stream.StreamSupport;

import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import io.crate.data.BatchIterator;
import io.crate.data.Bucket;
import io.crate.data.InMemoryBatchIterator;
import io.crate.data.Projector;
import io.crate.data.Row;
import io.crate.data.testing.TestingBatchIterators;
import io.crate.data.testing.TestingRowConsumer;

public class LimitAndOffsetProjectorTest extends ESTestCase {

    private final TestingRowConsumer consumer = new TestingRowConsumer();

    private LimitAndOffsetProjector prepareProjector(int limit, int offset) {
        return new LimitAndOffsetProjector(limit, offset);
    }

    @Test
    public void testProjectLimitOnly() throws Throwable {
        Projector projector = prepareProjector(10, LimitAndOffset.NO_OFFSET);

        BatchIterator<Row> batchIterator = projector.apply(TestingBatchIterators.range(0, 12));
        consumer.accept(batchIterator, null);

        Bucket projected = consumer.getBucket();
        assertThat(projected).hasSize(10);

        long iterateLength = StreamSupport.stream(consumer.getBucket().spliterator(), false).count();
        assertThat(iterateLength).isEqualTo(10L);
    }

    @Test
    public void testProjectLimitOnlyLessThanLimit() throws Throwable {
        Projector projector = prepareProjector(10, LimitAndOffset.NO_OFFSET);
        BatchIterator<Row> batchIterator = projector.apply(TestingBatchIterators.range(0, 5));
        consumer.accept(batchIterator, null);

        Bucket projected = consumer.getBucket();
        assertThat(projected).hasSize(5);

        long iterateLength = StreamSupport.stream(consumer.getBucket().spliterator(), false).count();
        assertThat(iterateLength).isEqualTo(5L);
    }

    @Test
    public void testProjectLimitOnlyExactlyLimit() throws Throwable {
        Projector projector = prepareProjector(10, LimitAndOffset.NO_OFFSET);
        BatchIterator<Row> batchIterator = projector.apply(TestingBatchIterators.range(0, 10));
        consumer.accept(batchIterator, null);
        Bucket projected = consumer.getBucket();
        assertThat(projected).hasSize(10);

        long iterateLength = StreamSupport.stream(consumer.getBucket().spliterator(), false).count();
        assertThat(iterateLength).isEqualTo(10L);

    }

    @Test
    public void testProjectLimitOnly0() throws Throwable {
        Projector projector = prepareProjector(10, LimitAndOffset.NO_OFFSET);
        consumer.accept(projector.apply(InMemoryBatchIterator.empty(SENTINEL)), null);

        Bucket projected = consumer.getBucket();
        assertThat(projected, emptyIterable());

        long iterateLength = StreamSupport.stream(consumer.getBucket().spliterator(), false).count();
        assertThat(iterateLength).isEqualTo(0L);
    }

    @Test
    public void testProjectLimitOnly1() throws Throwable {
        Projector projector = prepareProjector(1, LimitAndOffset.NO_OFFSET);
        BatchIterator<Row> batchIterator = projector.apply(TestingBatchIterators.range(0, 10));
        consumer.accept(batchIterator, null);

        Bucket projected = consumer.getBucket();
        assertThat(projected).hasSize(1);

        long iterateLength = StreamSupport.stream(consumer.getBucket().spliterator(), false).count();
        assertThat(iterateLength).isEqualTo(1L);
    }

    @Test
    public void testProjectOffsetBigger0() throws Throwable {
        Projector projector = prepareProjector(100, 10);
        BatchIterator<Row> batchIterator = projector.apply(TestingBatchIterators.range(0, 100));
        consumer.accept(batchIterator, null);

        Bucket projected = consumer.getBucket();
        assertThat(projected).hasSize(90);

        long iterateLength = StreamSupport.stream(consumer.getBucket().spliterator(), false).count();
        assertThat(iterateLength).isEqualTo(90L);
    }

    @Test
    public void testNegativeOffset() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Invalid OFFSET");
        new LimitAndOffsetProjector(10, -10);
    }

    @Test
    public void testNegativeLimit() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Invalid LIMIT");
        new LimitAndOffsetProjector(-100, LimitAndOffset.NO_OFFSET);
    }

    @Test
    public void testProjectLimitOnlyUpStream() throws Throwable {
        Projector projector = prepareProjector(10, LimitAndOffset.NO_OFFSET);
        BatchIterator<Row> batchIterator = projector.apply(TestingBatchIterators.range(0, 12));
        consumer.accept(batchIterator, null);
        Bucket projected = consumer.getBucket();
        assertThat(projected).hasSize(10);

        long iterateLength = StreamSupport.stream(consumer.getBucket().spliterator(), false).count();
        assertThat(iterateLength).isEqualTo(10L);
    }

    @Test
    public void testProjectLimitLessThanLimitUpStream() throws Throwable {
        Projector projector = prepareProjector(10, LimitAndOffset.NO_OFFSET);
        BatchIterator<Row> batchIterator = projector.apply(TestingBatchIterators.range(0, 5));
        consumer.accept(batchIterator, null);
        Bucket projected = consumer.getBucket();
        assertThat(projected).hasSize(5);

        long iterateLength = StreamSupport.stream(consumer.getBucket().spliterator(), false).count();
        assertThat(iterateLength).isEqualTo(5L);
    }


    @Test
    public void testProjectLimitOnly0UpStream() throws Throwable {
        Projector projector = prepareProjector(10, LimitAndOffset.NO_OFFSET);
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
        assertThat(projected).hasSize(90);

        long iterateLength = StreamSupport.stream(consumer.getBucket().spliterator(), false).count();
        assertThat(iterateLength).isEqualTo(90L);
    }

    @Test
    public void testProjectNoLimitNoOffset() throws Throwable {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Invalid LIMIT");

        prepareProjector(LimitAndOffset.NO_LIMIT, LimitAndOffset.NO_OFFSET);
    }
}
