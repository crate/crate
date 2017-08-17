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
import io.crate.testing.TestingBatchIterators;
import org.junit.Test;

import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class FilteringBatchIteratorTest {

    private Predicate<Row> evenRow = row -> (int) row.get(0) % 2 == 0;

    @Test
    public void testFilteringBatchIterator() throws Exception {
        List<Object[]> expectedResult = IntStream.iterate(0, l -> l + 2).limit(10).mapToObj(
            l -> new Object[]{l}).collect(Collectors.toList());

        BatchIteratorTester tester = new BatchIteratorTester(
            () -> new FilteringBatchIterator(TestingBatchIterators.range(0, 20), evenRow));
        tester.verifyResultAndEdgeCaseBehaviour(expectedResult);
    }
}
