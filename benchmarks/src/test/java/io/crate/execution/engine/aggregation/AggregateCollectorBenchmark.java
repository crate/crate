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

package io.crate.execution.engine.aggregation;

import io.crate.breaker.RamAccounting;
import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.execution.engine.aggregation.impl.SumAggregation;
import io.crate.execution.engine.collect.InputCollectExpression;
import io.crate.expression.symbol.AggregateMode;
import io.crate.expression.symbol.Literal;
import io.crate.memory.OnHeapMemoryManager;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataTypes;
import org.elasticsearch.Version;
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

import static io.crate.testing.TestingHelpers.createNodeContext;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
public class AggregateCollectorBenchmark {

    private final List<Row> rows = IntStream.range(0, 10_000).mapToObj(Row1::new).collect(Collectors.toList());

    private AggregateCollector collector;

    @Setup
    public void setup() {
        InputCollectExpression inExpr0 = new InputCollectExpression(0);
        SumAggregation<?> sumAggregation = (SumAggregation<?>) createNodeContext().functions().getQualified(
            Signature.aggregate(
                SumAggregation.NAME,
                DataTypes.INTEGER.getTypeSignature(),
                DataTypes.LONG.getTypeSignature()
            ),
            List.of(DataTypes.INTEGER),
            DataTypes.INTEGER
        );
        var memoryManager = new OnHeapMemoryManager(bytes -> {});
        collector = new AggregateCollector(
            Collections.singletonList(inExpr0),
            RamAccounting.NO_ACCOUNTING,
            memoryManager,
            Version.CURRENT,
            AggregateMode.ITER_FINAL,
            new AggregationFunction[] { sumAggregation },
            Version.CURRENT,
            new Input[][] { {inExpr0 } },
            new Input[] { Literal.BOOLEAN_TRUE }
        );
    }

    @Benchmark
    public Iterable<Row> measureAggregateCollector() {
        Object[] state = collector.supplier().get();
        BiConsumer<Object[], Row> accumulator = collector.accumulator();
        Function<Object[], Iterable<Row>> finisher = collector.finisher();
        for (int i = 0; i < rows.size(); i++) {
            accumulator.accept(state, rows.get(i));
        }
        return finisher.apply(state);
    }
}
