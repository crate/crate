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

package io.crate.operation.projectors;

import io.crate.analyze.symbol.AggregateMode;
import io.crate.breaker.RamAccountingContext;
import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.operation.aggregation.AggregationFunction;
import io.crate.operation.aggregation.impl.SumAggregation;
import io.crate.operation.collect.InputCollectExpression;
import io.crate.types.DataTypes;
import org.elasticsearch.Version;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.crate.testing.TestingHelpers.getFunctions;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
public class AggregateCollectorBenchmark {

    private final RamAccountingContext RAM_ACCOUNTING_CONTEXT =
        new RamAccountingContext("dummy", new NoopCircuitBreaker("dummy"));
    private final List<Row> rows = IntStream.range(0, 10_000).mapToObj(Row1::new).collect(Collectors.toList());

    private AggregateCollector collector;

    @Setup
    public void setup() {
        InputCollectExpression inExpr0 = new InputCollectExpression(0);
        SumAggregation sumAggregation = ((SumAggregation) getFunctions().getBuiltin(
            SumAggregation.NAME, Collections.singletonList(DataTypes.INTEGER)));
        collector = new AggregateCollector(
            Collections.singletonList(inExpr0),
            RAM_ACCOUNTING_CONTEXT,
            AggregateMode.ITER_FINAL,
            new AggregationFunction[] { sumAggregation },
            Version.CURRENT,
            BigArrays.NON_RECYCLING_INSTANCE,
            new Input[] { inExpr0 }
        );
    }

    @Benchmark
    public Object[] measureAggregateCollector() {
        Object[] state = collector.supplier().get();
        BiConsumer<Object[], Row> accumulator = collector.accumulator();
        Function<Object[], Object[]> finisher = collector.finisher();
        for (int i = 0; i < rows.size(); i++) {
            accumulator.accept(state, rows.get(i));
        }
        return finisher.apply(state);
    }
}
