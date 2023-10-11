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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Test;

import io.crate.data.testing.BatchIteratorTester;
import io.crate.data.testing.BatchSimulatingIterator;
import io.crate.data.testing.TestingBatchIterators;

public class CompositeBatchIteratorTest {

    private static final List<Object[]> EXPECTED_RESULT = IntStream.concat(IntStream.range(0, 5),
                                                                           IntStream.range(5, 10))
        .mapToObj(i -> new Object[] {i})
        .collect(Collectors.toList());

    @Test
    public void testDataRowInputsCanBeRetrievedEagerly() {
        BatchIterator<Row> iterator = CompositeBatchIterator.seqComposite(
            TestingBatchIterators.range(0, 1),
            TestingBatchIterators.range(1, 2)
        );
        assertThat(iterator.moveNext()).isTrue();
        assertThat(iterator.currentElement().get(0)).isEqualTo(0);

        assertThat(iterator.moveNext()).isTrue();
        assertThat(iterator.currentElement().get(0)).isEqualTo(1);
    }

    @Test
    public void testCompositeBatchIterator() throws Exception {
        var tester = BatchIteratorTester.forRows(
            () -> CompositeBatchIterator.seqComposite(
                TestingBatchIterators.range(0, 5),
                TestingBatchIterators.range(5, 10))
        );
        tester.verifyResultAndEdgeCaseBehaviour(EXPECTED_RESULT);
    }

    @Test
    public void testCompositeBatchIteratorWithBatchedSources() throws Exception {
        List<Object[]> expectedResult = new ArrayList<>();
        // consumes the unbatched/loaded iterator first
        expectedResult.add(new Object[] { 5 });
        expectedResult.add(new Object[] { 6 });
        expectedResult.add(new Object[] { 7 });
        expectedResult.add(new Object[] { 8 });
        expectedResult.add(new Object[] { 9 });
        expectedResult.add(new Object[] { 0 });
        expectedResult.add(new Object[] { 1 });
        expectedResult.add(new Object[] { 2 });
        expectedResult.add(new Object[] { 3 });
        expectedResult.add(new Object[] { 4 });
        var tester = BatchIteratorTester.forRows(
            () -> CompositeBatchIterator.seqComposite(
                new BatchSimulatingIterator<>(TestingBatchIterators.range(0, 5), 2, 6, null),
                TestingBatchIterators.range(5, 10)
            )
        );
        tester.verifyResultAndEdgeCaseBehaviour(expectedResult);
    }
}
