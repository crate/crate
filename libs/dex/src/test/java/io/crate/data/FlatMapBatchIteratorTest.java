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

import io.crate.testing.BatchIteratorTester;
import io.crate.testing.TestingBatchIterators;
import org.junit.Test;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertThat;

public class FlatMapBatchIteratorTest {

    @Test
    public void testFlatMap() throws Exception {
        InMemoryBatchIterator<Integer> source = new InMemoryBatchIterator<>(Arrays.asList(1, 2, 3), null, false);
        FlatMapBatchIterator<Integer, Integer[]> twiceAsArray =
            new FlatMapBatchIterator<>(source, x -> Arrays.asList(new Integer[] {x, x}, new Integer[] {x, x}).iterator());

        List<Integer[]> integers = BatchIterators.collect(twiceAsArray, Collectors.toList()).get(1, TimeUnit.SECONDS);
        assertThat(integers, contains(
            new Integer[] {1, 1},
            new Integer[] {1, 1},
            new Integer[] {2, 2},
            new Integer[] {2, 2},
            new Integer[] {3, 3},
            new Integer[] {3, 3}
        ));
    }

    @Test
    public void testFlatMapBatchIteratorFullFillsContracts() throws Exception {
        Function<Row, Iterator<Row>> duplicateRow =
            row -> Arrays.<Row>asList(new RowN(row.materialize()), new RowN(row.materialize())).iterator();
        BatchIteratorTester tester = new BatchIteratorTester(() -> {
            BatchIterator<Row> source = TestingBatchIterators.range(1, 4);
            return new FlatMapBatchIterator<>(source, duplicateRow);
        });
        tester.verifyResultAndEdgeCaseBehaviour(
            Arrays.asList(
                new Object[] { 1 },
                new Object[] { 1 },
                new Object[] { 2 },
                new Object[] { 2 },
                new Object[] { 3 },
                new Object[] { 3 }
            )
        );
    }
}
