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

import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.junit.jupiter.api.Test;

import io.crate.data.testing.BatchIteratorTester;
import io.crate.data.testing.BatchIteratorTester.ResultOrder;
import io.crate.data.testing.BatchSimulatingIterator;
import io.crate.data.testing.RowGenerator;

class InMemoryBatchIteratorTest {

    @Test
    void testCollectRows() throws Exception {
        List<Row> rows = Arrays.asList(new Row1(10), new Row1(20));
        Supplier<BatchIterator<Row>> batchIteratorSupplier = () -> InMemoryBatchIterator.of(rows, SentinelRow.SENTINEL, false);
        var tester = BatchIteratorTester.forRows(batchIteratorSupplier, ResultOrder.EXACT);
        List<Object[]> expectedResult = Arrays.asList(new Object[]{10}, new Object[]{20});
        tester.verifyResultAndEdgeCaseBehaviour(expectedResult);
    }

    @Test
    void testCollectRowsWithSimulatedBatches() throws Exception {
        Iterable<Row> rows = RowGenerator.range(0, 50);
        Supplier<BatchIterator<Row>> batchIteratorSupplier = () -> new BatchSimulatingIterator<>(
            InMemoryBatchIterator.of(rows, SentinelRow.SENTINEL, false),
            10,
            5,
            null
        );
        var tester = BatchIteratorTester.forRows(batchIteratorSupplier, ResultOrder.EXACT);
        List<Object[]> expectedResult = StreamSupport.stream(rows.spliterator(), false)
            .map(Row::materialize)
            .collect(Collectors.toList());
        tester.verifyResultAndEdgeCaseBehaviour(expectedResult);
    }
}
