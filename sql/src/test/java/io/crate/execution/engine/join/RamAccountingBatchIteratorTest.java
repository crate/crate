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

package io.crate.execution.engine.join;

import com.google.common.collect.ImmutableList;
import io.crate.breaker.RamAccountingContext;
import io.crate.breaker.RowAccounting;
import io.crate.breaker.RowAccountingTest;
import io.crate.data.BatchIterator;
import io.crate.data.Row;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.TestingBatchIterators;
import io.crate.testing.TestingRowConsumer;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.MemoryCircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

public class RamAccountingBatchIteratorTest extends CrateUnitTest {

    private static NoopCircuitBreaker NOOP_CIRCUIT_BREAKER = new NoopCircuitBreaker("dummy");

    private long originalBufferSize;

    @Before
    public void reduceFlushBufferSize() {
        originalBufferSize = RamAccountingContext.FLUSH_BUFFER_SIZE;
        RamAccountingContext.FLUSH_BUFFER_SIZE = 10;
    }

    @After
    public void resetFlushBufferSize() {
        RamAccountingContext.FLUSH_BUFFER_SIZE = originalBufferSize;
    }

    @Test
    public void testNoCircuitBreaking() throws Exception {
        BatchIterator<Row> batchIterator = new RamAccountingBatchIterator<>(
            TestingBatchIterators.ofValues(Arrays.asList(new BytesRef("a"), new BytesRef("b"), new BytesRef("c"))),
            new RowAccounting(
                ImmutableList.of(DataTypes.STRING),
                new RamAccountingContext("test", NOOP_CIRCUIT_BREAKER)));

        TestingRowConsumer consumer = new TestingRowConsumer();
        consumer.accept(batchIterator, null);
        assertThat(
            consumer.getResult(),
            Matchers.contains(
                new Object[] {new BytesRef("a")}, new Object[] {new BytesRef("b")}, new Object[] {new BytesRef("c")}));
    }

    @Test
    public void testCircuitBreaking() throws Exception {
        BatchIterator<Row> batchIterator = new RamAccountingBatchIterator<>(
            TestingBatchIterators.ofValues(Arrays.asList(new BytesRef("aaa"), new BytesRef("bbb"), new BytesRef("ccc"),
                                                         new BytesRef("ddd"), new BytesRef("eee"), new BytesRef("fff"))),
            new RowAccounting(
                ImmutableList.of(DataTypes.STRING),
                new RamAccountingContext(
                    "test",
                    new MemoryCircuitBreaker(
                        new ByteSizeValue(34, ByteSizeUnit.BYTES),
                        1,
                        Loggers.getLogger(RowAccountingTest.class)))));

        expectedException.expect(CircuitBreakingException.class);
        expectedException.expectMessage(
            "Data too large, data for field [test] would be [35/35b], which is larger than the limit of [34/34b]");

        TestingRowConsumer consumer = new TestingRowConsumer();
        consumer.accept(batchIterator, null);
        consumer.getResult();
    }
}
