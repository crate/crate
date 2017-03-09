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
import io.crate.testing.BatchSimulatingIterator;
import io.crate.testing.TestingBatchIterators;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class LimitingBatchIteratorTest {

    private int limit = 5;
    private List<Object[]> expectedResult = IntStream.range(0, 10).limit(limit)
        .mapToObj(l -> new Object[]{l}).collect(Collectors.toList());

    @Test
    public void testLimitingBatchIterator() throws Exception {
        BatchIteratorTester tester = new BatchIteratorTester(
            () -> LimitingBatchIterator.newInstance(TestingBatchIterators.range(0, 10), limit),
            expectedResult
        );
        tester.run();
    }

    @Test
    public void testLimitingBatchIteratorWithBatchedSource() throws Exception {
        BatchIteratorTester tester = new BatchIteratorTester(
            () -> {
                BatchSimulatingIterator batchSimulatingIt = new BatchSimulatingIterator(
                    TestingBatchIterators.range(0, 10), 2, 5, null);
                return LimitingBatchIterator.newInstance(batchSimulatingIt, limit);
            },
            expectedResult
        );
        tester.run();
    }
}
