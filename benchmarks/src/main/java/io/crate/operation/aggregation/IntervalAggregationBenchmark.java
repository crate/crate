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

package io.crate.operation.aggregation;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.IntStream;

import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;
import org.joda.time.Period;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.data.breaker.RamAccounting;
import io.crate.execution.engine.aggregation.AggregateCollector;
import io.crate.execution.engine.aggregation.AggregationFunction;
import io.crate.execution.engine.aggregation.impl.IntervalSumAggregation;
import io.crate.execution.engine.aggregation.impl.average.AverageAggregation;
import io.crate.execution.engine.aggregation.impl.average.IntervalAverageAggregation;
import io.crate.execution.engine.collect.RowCollectExpression;
import io.crate.expression.symbol.AggregateMode;
import io.crate.expression.symbol.Literal;
import io.crate.memory.OnHeapMemoryManager;
import io.crate.metadata.Functions;
import io.crate.metadata.Scalar;
import io.crate.metadata.functions.Signature;
import io.crate.metadata.settings.session.SessionSettingRegistry;
import io.crate.types.DataTypes;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 2)
@Fork(value = 2)
@State(Scope.Benchmark)
public class IntervalAggregationBenchmark {

    private final List<Row1> rows = IntStream.range(0, 10_000)
        .mapToObj(i -> {
            int mod = i % 3;
            if (mod == 0) {
                return new Row1(Period.minutes(i).withSeconds(i + 1));
            } else if (mod == 1) {
                return new Row1(Period.hours(i).withMinutes(i + 1).withSeconds(i + 2));
            } else {
                return new Row1(Period.days(i).withHours(i + 1).withMinutes(i + 2).withSeconds(i + 3));
            }
        }).toList();

    private OnHeapMemoryManager onHeapMemoryManager;
    private AggregateCollector onHeapCollectorSum;
    private AggregateCollector onHeapCollectorAvg;

    @Setup
    @SuppressWarnings("unchecked")
    public void setUp() throws Exception {
        final RowCollectExpression inExpr0 = new RowCollectExpression(0);
        Functions functions = Functions.load(Settings.EMPTY, new SessionSettingRegistry(Set.of()));

        final IntervalSumAggregation intervalSumAggregation = (IntervalSumAggregation) functions.getQualified(
            Signature.aggregate(
                    IntervalSumAggregation.NAME,
                    DataTypes.INTERVAL.getTypeSignature(),
                    DataTypes.INTERVAL.getTypeSignature())
                .withFeature(Scalar.Feature.DETERMINISTIC),
            List.of(DataTypes.INTERVAL),
                DataTypes.INTERVAL
            );

        final IntervalAverageAggregation intervalAvgAggregation = (IntervalAverageAggregation) functions.getQualified(
            Signature.aggregate(
                    AverageAggregation.NAME,
                    DataTypes.INTERVAL.getTypeSignature(),
                    DataTypes.INTERVAL.getTypeSignature())
                .withFeature(Scalar.Feature.DETERMINISTIC),
            List.of(DataTypes.INTERVAL),
            DataTypes.INTERVAL
        );

        onHeapMemoryManager = new OnHeapMemoryManager(bytes -> {});
        onHeapCollectorSum = new AggregateCollector(
            Collections.singletonList(inExpr0),
            RamAccounting.NO_ACCOUNTING,
            onHeapMemoryManager,
            Version.CURRENT,
            AggregateMode.ITER_FINAL,
            new AggregationFunction[] { intervalSumAggregation },
            Version.CURRENT,
            new Input[][] { { inExpr0 } },
            new Input[] { Literal.BOOLEAN_TRUE }
        );
        onHeapCollectorAvg = new AggregateCollector(
            Collections.singletonList(inExpr0),
            RamAccounting.NO_ACCOUNTING,
            onHeapMemoryManager,
            Version.CURRENT,
            AggregateMode.ITER_FINAL,
            new AggregationFunction[] { intervalAvgAggregation },
            Version.CURRENT,
            new Input[][] { { inExpr0 } },
            new Input[] { Literal.BOOLEAN_TRUE }
        );
    }

    @TearDown
    public void tearDown() {
        onHeapMemoryManager.close();
    }

    @Benchmark
    public Iterable<Row> benchmarkIntervalSumAggregation() {
        Object[] state = onHeapCollectorSum.supplier().get();
        BiConsumer<Object[], Row> accumulator = onHeapCollectorSum.accumulator();
        Function<Object[], Iterable<Row>> finisher = onHeapCollectorSum.finisher();
        for (int i = 0; i < rows.size(); i++) {
            accumulator.accept(state, rows.get(i));
        }
        return finisher.apply(state);
    }

    @Benchmark
    public Iterable<Row> benchmarkIntervalAvgAggregation() {
        Object[] state = onHeapCollectorAvg.supplier().get();
        BiConsumer<Object[], Row> accumulator = onHeapCollectorAvg.accumulator();
        Function<Object[], Iterable<Row>> finisher = onHeapCollectorAvg.finisher();
        for (int i = 0; i < rows.size(); i++) {
            accumulator.accept(state, rows.get(i));
        }
        return finisher.apply(state);
    }
}
