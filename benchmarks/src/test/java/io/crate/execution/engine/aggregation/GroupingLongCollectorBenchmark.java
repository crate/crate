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

import com.google.inject.Guice;
import com.google.inject.Stage;
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
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.Functions;
import io.crate.types.DataTypes;
import org.elasticsearch.Version;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static io.crate.data.SentinelRow.SENTINEL;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class GroupingLongCollectorBenchmark {

    private static final RamAccountingContext RAM_ACCOUNTING_CONTEXT =
        new RamAccountingContext("dummy", new NoopCircuitBreaker(CircuitBreaker.FIELDDATA));

    private GroupingCollector groupBySumCollector;
    private BatchIterator<Row> rowsIterator;
    private List<Row> rows;
    private GroupBySingleNumberCollector groupBySumSingleLongCollector;

    @Setup
    public void createGroupingCollector() {
        Functions functions = Guice.createInjector(Stage.PRODUCTION, new AggregationImplModule())
            .getInstance(Functions.class);
        AggregationFunction sumAgg = (AggregationFunction) functions.getQualified(
            new FunctionIdent(SumAggregation.NAME, Arrays.asList(DataTypes.INTEGER)));
        groupBySumCollector = createGroupBySumCollector(sumAgg);
        groupBySumSingleLongCollector = createOptimizedCollector(sumAgg);

        rows = new ArrayList<>(20_000_000);
        for (int i = 0; i < 20_000_000; i++) {
            rows.add(new Row1((long) i % 200));
        }
    }

    private GroupBySingleNumberCollector createOptimizedCollector(AggregationFunction sumAgg) {
        InputCollectExpression keyInput = new InputCollectExpression(0);
        return new GroupBySingleNumberCollector(
            DataTypes.LONG,
            new CollectExpression[] { keyInput },
            AggregateMode.ITER_FINAL,
            new AggregationFunction[] { sumAgg },
            new Input[][] { new Input[] { keyInput }},
            RAM_ACCOUNTING_CONTEXT,
            keyInput,
            Version.CURRENT,
            BigArrays.NON_RECYCLING_INSTANCE
        );
    }

    private static GroupingCollector createGroupBySumCollector(AggregationFunction sumAgg) {
        InputCollectExpression keyInput = new InputCollectExpression(0);
        List<Input<?>> keyInputs = Arrays.<Input<?>>asList(keyInput);
        CollectExpression[] collectExpressions = new CollectExpression[]{keyInput};

        return GroupingCollector.singleKey(
            collectExpressions,
            AggregateMode.ITER_FINAL,
            new AggregationFunction[] { sumAgg },
            new Input[][] { new Input[] { keyInput }},
            RAM_ACCOUNTING_CONTEXT,
            keyInputs.get(0),
            DataTypes.LONG,
            Version.CURRENT,
            BigArrays.NON_RECYCLING_INSTANCE
        );
    }

    @Benchmark
    public void measureGroupBySumLong(Blackhole blackhole) throws Exception {
        rowsIterator = InMemoryBatchIterator.of(rows, SENTINEL);
        blackhole.consume(BatchIterators.collect(rowsIterator, groupBySumCollector).get());
    }

    @Benchmark
    public void measureGroupBySumLongOptimized(Blackhole blackhole) throws Exception {
        rowsIterator = InMemoryBatchIterator.of(rows, SENTINEL);
        blackhole.consume(BatchIterators.collect(rowsIterator, groupBySumSingleLongCollector).get());
    }
}
