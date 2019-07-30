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

package io.crate.execution.engine.aggregation;

import io.crate.breaker.RamAccountingContext;
import io.crate.data.BatchIterator;
import io.crate.data.BatchIterators;
import io.crate.data.InMemoryBatchIterator;
import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.execution.engine.aggregation.impl.AggregationImplModule;
import io.crate.execution.engine.aggregation.impl.SumAggregation;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.execution.engine.collect.InputCollectExpression;
import io.crate.expression.symbol.AggregateMode;
import io.crate.expression.symbol.Literal;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.Functions;
import io.crate.test.integration.CrateUnitTest;
import io.crate.types.DataTypes;
import org.elasticsearch.Version;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.util.BigArrays;
import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.crate.data.SentinelRow.SENTINEL;
import static org.hamcrest.Matchers.is;

public class GroupBySingleNumberCollectorTest extends CrateUnitTest {

    private static final RamAccountingContext RAM_ACCOUNTING_CONTEXT =
        new RamAccountingContext("dummy", new NoopCircuitBreaker(CircuitBreaker.FIELDDATA));

    @Test
    public void testIterateOverResultTwice() throws Exception {
        Functions functions = new ModulesBuilder().add(new AggregationImplModule())
            .createInjector().getInstance(Functions.class);
        AggregationFunction sumAgg = (AggregationFunction) functions.getQualified(
            new FunctionIdent(SumAggregation.NAME, Arrays.asList(DataTypes.INTEGER)));
        InputCollectExpression keyInput = new InputCollectExpression(0);

        GroupBySingleNumberCollector groupBySingleNumberCollector = new GroupBySingleNumberCollector(
            DataTypes.INTEGER,
            new CollectExpression[]{keyInput},
            AggregateMode.ITER_FINAL,
            new AggregationFunction[]{sumAgg},
            new Input[][]{new Input[]{keyInput}},
            new Input[] {Literal.BOOLEAN_TRUE},
            RAM_ACCOUNTING_CONTEXT,
            keyInput,
            Version.CURRENT,
            BigArrays.NON_RECYCLING_INSTANCE
        );

        BatchIterator<Row> inputRowsIterator = InMemoryBatchIterator.of(
            IntStream.range(0, 10).mapToObj(Row1::new).collect(Collectors.toList()),
            SENTINEL
        );

        Iterable<Row> rows = BatchIterators.collect(inputRowsIterator, groupBySingleNumberCollector)
            .get(10, TimeUnit.SECONDS);
        IntStream.range(0, 2).forEach(i -> {
            int index = 0;
            for (Row row : rows) {
                assertThat(row.get(0), is(index));
                index++;
            }
        });
    }
}
