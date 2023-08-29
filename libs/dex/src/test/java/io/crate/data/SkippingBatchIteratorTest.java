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

public class SkippingBatchIteratorTest {

    private static final int OFFSET = 3;

    private static final List<Object[]> EXPECTED_RESULT = IntStream.range(3, 10)
        .mapToObj(i -> new Object[] {i})
        .collect(Collectors.toList());

    @Test
    public void testSkippingBatchIterator() throws Exception {
        var tester = BatchIteratorTester.forRows(
            () -> new SkippingBatchIterator<>(TestingBatchIterators.range(0, 10), OFFSET)
        );
        tester.verifyResultAndEdgeCaseBehaviour(EXPECTED_RESULT);
    }

    @Test
    public void testSkippingBatchIteratorWithBatchedSource() throws Exception {
        var tester = BatchIteratorTester.forRows(
            () -> {
                BatchIterator<Row> source = TestingBatchIterators.range(0, 10);
                source = new BatchSimulatingIterator<>(source, 2, 5, null);
                return new SkippingBatchIterator<>(source, OFFSET);
            }
        );
        tester.verifyResultAndEdgeCaseBehaviour(EXPECTED_RESULT);
    }
}
