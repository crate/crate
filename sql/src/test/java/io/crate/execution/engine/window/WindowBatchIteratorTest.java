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

package io.crate.execution.engine.window;

import io.crate.analyze.WindowDefinition;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.testing.BatchIteratorTester;
import io.crate.testing.BatchSimulatingIterator;
import io.crate.testing.TestingBatchIterators;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class WindowBatchIteratorTest {

    private List<Object[]> expectedResult = IntStream.range(0, 10)
        .mapToObj(l -> new Object[]{45}).collect(Collectors.toList());

    @Test
    public void testSumWindowBatchIterator() throws Exception {
        BatchIteratorTester tester = new BatchIteratorTester(
            () -> new WindowBatchIterator(
                emptyWindow(),
                TestingBatchIterators.range(0, 10),
                getIntSummingCollector()
            )
        );

        tester.verifyResultAndEdgeCaseBehaviour(expectedResult);
    }

    @Test
    public void testSumWindowBatchIteratorWithBatchSimulatingSource() throws Exception {
        BatchIteratorTester tester = new BatchIteratorTester(
            () -> new WindowBatchIterator(
                emptyWindow(),
                new BatchSimulatingIterator<>(
                    TestingBatchIterators.range(0, 10), 4, 2, null),
                getIntSummingCollector()
            )
        );

        tester.verifyResultAndEdgeCaseBehaviour(expectedResult);
    }

    private Collector<Row, ?, Iterable<Row>> getIntSummingCollector() {
        return Collectors.collectingAndThen(
            Collectors.summingInt(r -> (int) r.get(0)),
            sum -> Collections.singletonList(new Row1(sum)));
    }

    private static WindowDefinition emptyWindow() {
        return new WindowDefinition(Collections.emptyList(), null, null);
    }

}
