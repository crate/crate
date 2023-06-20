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
import static io.crate.execution.engine.pipeline.LimitAndOffset.NO_OFFSET;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

public class OffsetProjectorTest extends ESTestCase {

    private final TestingRowConsumer consumer = new TestingRowConsumer();

    private OffsetProjector prepareProjector(int offset) {
        return new OffsetProjector(offset);
    }

    @Test
    public void testProjectLimitLessThanOffsetUpStream() throws Exception {
        Projector projector = prepareProjector(10);
        BatchIterator<Row> batchIterator = projector.apply(TestingBatchIterators.range(0, 5));
        consumer.accept(batchIterator, null);
        Bucket projected = consumer.getBucket();
        assertThat(projected).isEmpty();
    }

    @Test
    public void testProjectOffsetOnly0UpStream() throws Exception {
        Projector projector = prepareProjector(10);
        consumer.accept(projector.apply(InMemoryBatchIterator.empty(SENTINEL)), null);
        Bucket projected = consumer.getBucket();
        assertThat(projected).isEmpty();
    }

    @Test
    public void testProjectOffsetBigger0UpStream() throws Exception {
        Projector projector = prepareProjector(10);
        BatchIterator<Row> batchIterator = projector.apply(TestingBatchIterators.range(0, 100));
        consumer.accept(batchIterator, null);
        Bucket projected = consumer.getBucket();
        assertThat(projected).hasSize(90);

        long iterateLength = StreamSupport.stream(consumer.getBucket().spliterator(), false).count();
        assertThat(iterateLength).isEqualTo(90L);
    }

    @Test
    public void testInvalidOffset() {
        assertThatThrownBy(() -> prepareProjector(NO_OFFSET))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Invalid OFFSET: value must be > 0; got: 0");
    }
}
