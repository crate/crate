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

package io.crate.operation.merge;

import io.crate.data.Row;
import io.crate.testing.BatchIteratorTester;
import io.crate.testing.RowGenerator;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class BatchPagingIteratorTest {


    @Test
    public void testBatchPagingIterator() throws Exception {
        Iterable<Row> rows = RowGenerator.range(0, 3);
        List<Object[]> expectedResult = StreamSupport.stream(rows.spliterator(), false)
            .map(Row::materialize)
            .collect(Collectors.toList());
        BatchIteratorTester tester = new BatchIteratorTester(() -> {
            PassThroughPagingIterator<Integer, Row> pagingIterator = PassThroughPagingIterator.repeatable();
            pagingIterator.merge(Collections.singletonList(new KeyIterable<>(0, rows)));
            return new BatchPagingIterator<>(
                pagingIterator,
                exhaustedIt -> false,
                () -> true,
                () -> {
                }
            );
        });
        tester.verifyResultAndEdgeCaseBehaviour(expectedResult);
    }

    @Test
    public void testCompleteLoadCanBeCalledWithoutHavingCalledLoadNextBatch() throws Exception {
        PassThroughPagingIterator<Integer, Row> pagingIterator = PassThroughPagingIterator.repeatable();
        BatchPagingIterator<Integer> iterator = new BatchPagingIterator<>(
            pagingIterator,
            exhaustedIt -> false,
            () -> true,
            () -> {
            }
        );
        // must not throw an exception
        iterator.completeLoad(new IllegalStateException("Dummy"));
    }
}
