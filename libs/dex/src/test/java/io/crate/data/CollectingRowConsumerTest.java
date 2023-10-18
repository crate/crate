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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

import io.crate.data.testing.BatchSimulatingIterator;
import io.crate.data.testing.TestingBatchIterators;

public class CollectingRowConsumerTest {

    @Test
    public void testBatchedIteratorConsumption() throws Exception {
        List<Object[]> expectedResult = IntStream.range(0, 10)
            .mapToObj(i -> new Object[]{i})
            .collect(Collectors.toList());

        BatchSimulatingIterator<Row> batchSimulatingIterator =
            new BatchSimulatingIterator<>(TestingBatchIterators.range(0, 10),
                                          2,
                                          5,
                                          null);

        CollectingRowConsumer<?, List<Object[]>> batchConsumer =
            new CollectingRowConsumer<>(Collectors.mapping(Row::materialize, Collectors.toList()));

        batchConsumer.accept(batchSimulatingIterator, null);

        CompletableFuture<List<Object[]>> result = batchConsumer.completionFuture();
        List<Object[]> consumedRows = result.get(10, TimeUnit.SECONDS);

        assertThat(consumedRows).hasSize(10);
        assertThat(consumedRows).containsExactlyElementsOf(expectedResult);
    }
}
