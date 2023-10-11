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

package io.crate.data;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Test;

import io.crate.data.testing.BatchIteratorTester;
import io.crate.data.testing.BatchSimulatingIterator;
import io.crate.data.testing.TestingBatchIterators;

public class LimitingBatchIteratorTest {

    private static final int LIMIT = 5;
    private static final List<Object[]> EXPECTED_RESULT = IntStream.range(0, 10).limit(LIMIT)
        .mapToObj(l -> new Object[] {l}).collect(Collectors.toList());

    @Test
    public void testLimitingBatchIterator() throws Exception {
        var tester = BatchIteratorTester.forRows(
            () -> LimitingBatchIterator.newInstance(TestingBatchIterators.range(0, 10), LIMIT)
        );
        tester.verifyResultAndEdgeCaseBehaviour(EXPECTED_RESULT);
    }

    @Test
    public void testLimitingBatchIteratorWithBatchedSource() throws Exception {
        var tester = BatchIteratorTester.forRows(
            () -> {
                BatchSimulatingIterator<Row> batchSimulatingIt = new BatchSimulatingIterator<>(
                    TestingBatchIterators.range(0, 10), 2, 5, null);
                return LimitingBatchIterator.newInstance(batchSimulatingIt, LIMIT);
            }
        );
        tester.verifyResultAndEdgeCaseBehaviour(EXPECTED_RESULT);
    }
}
