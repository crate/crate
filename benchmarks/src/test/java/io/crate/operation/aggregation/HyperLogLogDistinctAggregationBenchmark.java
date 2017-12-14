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

package io.crate.operation.aggregation;

import io.crate.analyze.symbol.AggregateMode;
import io.crate.breaker.RamAccountingContext;
import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.metadata.Functions;
import io.crate.module.HyperLogLogModule;
import io.crate.operation.collect.InputCollectExpression;
import io.crate.operation.projectors.AggregateCollector;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.search.aggregations.metrics.cardinality.HyperLogLogPlusPlus;
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

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
public class HyperLogLogDistinctAggregationBenchmark {

    private final RamAccountingContext RAM_ACCOUNTING_CONTEXT =
        new RamAccountingContext("dummy", new NoopCircuitBreaker("dummy"));
    private final List<Row> rows = IntStream.range(0, 10_000).mapToObj(i -> new Row1(new BytesRef(i))).collect(Collectors.toList());
    private final HyperLogLogDistinctAggregation.Murmur3Hash murmur3Hash =
        HyperLogLogDistinctAggregation.Murmur3Hash.getForType(DataTypes.STRING, Version.CURRENT);

    private HyperLogLogPlusPlus hyperLogLogPlusPlus;
    private AggregateCollector collector;
    private AggregateCollector collectorNoDeepCopy;

    @Setup
    public void setUp() throws Exception {
        hyperLogLogPlusPlus = new HyperLogLogPlusPlus(HyperLogLogPlusPlus.DEFAULT_PRECISION, BigArrays.NON_RECYCLING_INSTANCE, 1);
        InputCollectExpression inExpr0 = new BytesRefDeepCopyCollectExpression(0);
        Functions functions = new ModulesBuilder()
            .add(new HyperLogLogModule())
            .createInjector().getInstance(Functions.class);
        HyperLogLogDistinctAggregation hllAggregation = ((HyperLogLogDistinctAggregation) functions.getBuiltin(
            HyperLogLogDistinctAggregation.NAME, Collections.singletonList(DataTypes.STRING)));
        collector = new AggregateCollector(
            Collections.singletonList(inExpr0),
            RAM_ACCOUNTING_CONTEXT,
            AggregateMode.ITER_FINAL,
            new AggregationFunction[] { hllAggregation },
            Version.CURRENT,
            BigArrays.NON_RECYCLING_INSTANCE,
            new Input[] { inExpr0 }
        );

        InputCollectExpression inExpr1 = new InputCollectExpression(0);
        collectorNoDeepCopy = new AggregateCollector(
            Collections.singletonList(inExpr1),
            RAM_ACCOUNTING_CONTEXT,
            AggregateMode.ITER_FINAL,
            new AggregationFunction[] { hllAggregation },
            Version.CURRENT,
            BigArrays.NON_RECYCLING_INSTANCE,
            new Input[] { inExpr1 }
        );
    }

    @Benchmark
    public long benchmarkHLLPlusPlus() throws Exception {
        for (int i = 0; i < rows.size(); i++) {
            hyperLogLogPlusPlus.collect(0, murmur3Hash.hash(rows.get(i).get(0)));
        }
        return hyperLogLogPlusPlus.cardinality(0);
    }

    @Benchmark
    public Object[] benchmarkHLLAggregation() throws Exception {
        Object[] state = collector.supplier().get();
        BiConsumer<Object[], Row> accumulator = collector.accumulator();
        Function<Object[], Object[]> finisher = collector.finisher();
        for (int i = 0; i < rows.size(); i++) {
            accumulator.accept(state, rows.get(i));
        }
        return finisher.apply(state);
    }

    @Benchmark
    public Object[] benchmarkHLLAggregationNoDeepCopy() throws Exception {
        Object[] state = collectorNoDeepCopy.supplier().get();
        BiConsumer<Object[], Row> accumulator = collectorNoDeepCopy.accumulator();
        Function<Object[], Object[]> finisher = collectorNoDeepCopy.finisher();
        for (int i = 0; i < rows.size(); i++) {
            accumulator.accept(state, rows.get(i));
        }
        return finisher.apply(state);
    }

    static class BytesRefDeepCopyCollectExpression extends InputCollectExpression {

        BytesRefDeepCopyCollectExpression(int position) {
            super(position);
        }

        @Override
        public Object value() {
            return BytesRef.deepCopyOf((BytesRef) super.value());
        }
    }
}
