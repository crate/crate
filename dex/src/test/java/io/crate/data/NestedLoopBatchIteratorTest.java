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

package io.crate.data;

import io.crate.testing.BatchIteratorTester;
import io.crate.testing.CollectingBatchConsumer;
import io.crate.testing.TestingBatchIterators;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertThat;

public class NestedLoopBatchIteratorTest {

    @Test
    public void testNestedLoopBatchIterator() throws Exception {
        List<Object[]> expectedResult = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            for (int j = 0; j < 3; j++) {
                expectedResult.add(new Object[] { i, j });
            }
        }
        BatchIteratorTester tester = new BatchIteratorTester(
            () -> new NestedLoopBatchIterator(
                TestingBatchIterators.range(0, 3),
                TestingBatchIterators.range(0, 3)
            ),
            expectedResult
        );
        tester.run();
    }

    @Test
    public void testNestedLoopLeftAndRightEmpty() throws Exception {
        NestedLoopBatchIterator iterator = new NestedLoopBatchIterator(
            RowsBatchIterator.empty(),
            RowsBatchIterator.empty()
        );
        CollectingBatchConsumer consumer = new CollectingBatchConsumer();
        consumer.accept(iterator, null);
        assertThat(consumer.getResult(), Matchers.empty());
    }

    @Test
    public void testNestedLoopLeftEmpty() throws Exception {
        NestedLoopBatchIterator iterator = new NestedLoopBatchIterator(
            RowsBatchIterator.empty(),
            TestingBatchIterators.range(0, 5)
        );
        CollectingBatchConsumer consumer = new CollectingBatchConsumer();
        consumer.accept(iterator, null);
        assertThat(consumer.getResult(), Matchers.empty());
    }

    @Test
    public void testNestedLoopRightEmpty() throws Exception {
        NestedLoopBatchIterator iterator = new NestedLoopBatchIterator(
            TestingBatchIterators.range(0, 5),
            RowsBatchIterator.empty()
        );
        CollectingBatchConsumer consumer = new CollectingBatchConsumer();
        consumer.accept(iterator, null);
        assertThat(consumer.getResult(), Matchers.empty());
    }

}
