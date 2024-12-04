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

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import io.crate.data.testing.BatchIteratorTester;
import io.crate.data.testing.BatchIteratorTester.ResultOrder;

class TopNDistinctBatchIteratorTest {

    @Test
    void test_topN_distinct_bi_outputs_n_distinct_items() throws Throwable {
        var source = InMemoryBatchIterator.of(List.of(1, 1, 1, 2, 3), null, false);
        var topNDistinct = new TopNDistinctBatchIterator<>(source, 2, x -> x);

        List<Integer> integers = topNDistinct.toList().get(5, TimeUnit.SECONDS);
        assertThat(integers).containsExactly(1, 2);
    }

    @Test
    void test_topN_distinct_bi_outputs_lt_n_distinct_items_if_source_contains_less() throws Throwable {
        var source = InMemoryBatchIterator.of(List.of(1, 1, 1, 2, 3), null, false);
        var topNDistinct = new TopNDistinctBatchIterator<>(source, 5, x -> x);

        List<Integer> integers = topNDistinct.toList().get(5, TimeUnit.SECONDS);
        assertThat(integers).containsExactly(1, 2, 3);
    }

    @Test
    void test_topN_distinct_fulfills_bi_contracts() throws Throwable {
        var tester = BatchIteratorTester.forRows(() -> {
            var source = InMemoryBatchIterator.of(
                List.<Row>of(
                    new Row1(1),
                    new Row1(1),
                    new Row1(1),
                    new Row1(2),
                    new Row1(3),
                    new Row1(4),
                    new Row1(4)
                ),
                null,
                false
            );
            return new TopNDistinctBatchIterator<>(source, 3, x -> x);
        }, ResultOrder.EXACT);
        tester.verifyResultAndEdgeCaseBehaviour(
            List.of(
                new Object[] { 1 },
                new Object[] { 2 },
                new Object[] { 3 }
            )
        );
    }
}
