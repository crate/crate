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

package io.crate.executor.transport.distributed;

import io.crate.Streamer;
import io.crate.data.BatchIterator;
import io.crate.data.Bucket;
import io.crate.data.RowsBatchIterator;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.BatchSimulatingIterator;
import io.crate.testing.FailingBatchIterator;
import io.crate.testing.RowGenerator;
import io.crate.testing.TestingHelpers;
import io.crate.types.DataTypes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

@SuppressWarnings("ConstantConditions")
public class SingleBucketBuilderTest extends CrateUnitTest {

    private SingleBucketBuilder bucketBuilder = new SingleBucketBuilder(new Streamer[]{DataTypes.LONG.streamer()});
    private ExecutorService executor;

    @Before
    public void setupExecutor() throws Exception {
        executor = Executors.newFixedThreadPool(2);
    }

    @After
    public void tearDownExecutor() throws Exception {
        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
    }

    @Test
    public void testSingleBucketBuilderConsumer() throws Exception {
        bucketBuilder.asConsumer().accept(RowsBatchIterator.newInstance(RowGenerator.range(0, 4)), null);
        Bucket rows = bucketBuilder.completionFuture().get(10, TimeUnit.SECONDS);
        assertThat(TestingHelpers.printedTable(rows),
            is("0\n" +
               "1\n" +
               "2\n" +
               "3\n"));
    }

    @Test
    public void testSingleBucketBuilderWithBatchedSource() throws Exception {
        BatchIterator iterator = RowsBatchIterator.newInstance(RowGenerator.range(0, 4));
        bucketBuilder.asConsumer().accept(new BatchSimulatingIterator(iterator, 2, 2, executor), null);
        Bucket rows = bucketBuilder.completionFuture().get(10, TimeUnit.SECONDS);
        assertThat(TestingHelpers.printedTable(rows),
            is("0\n" +
               "1\n" +
               "2\n" +
               "3\n"));
    }

    @Test
    public void testConsumeFailingBatchIterator() throws Exception {
        FailingBatchIterator iterator = new FailingBatchIterator(
            RowsBatchIterator.newInstance(RowGenerator.range(0, 4)), 2);
        bucketBuilder.asConsumer().accept(iterator, null);

        expectedException.expectCause(instanceOf(UnsupportedOperationException.class));
        expectedException.expectMessage("Fail after 2 moveNext calls");
        bucketBuilder.completionFuture().get(10, TimeUnit.SECONDS);
    }
}
