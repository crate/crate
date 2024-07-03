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

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.IntStream;

import org.elasticsearch.Version;
import org.elasticsearch.common.hash.MurmurHash3;
import org.elasticsearch.common.settings.Settings;
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
import io.crate.execution.engine.aggregation.impl.HyperLogLogPlusPlus;
import io.crate.execution.engine.collect.RowCollectExpression;
import io.crate.expression.symbol.AggregateMode;
import io.crate.expression.symbol.Literal;
import io.crate.memory.OffHeapMemoryManager;
import io.crate.memory.OnHeapMemoryManager;
import io.crate.metadata.FunctionType;
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
public class HyperLogLogDistinctAggregationBenchmark {

    private final List<Row1> rows = IntStream.range(0, 10_000).mapToObj(i -> new Row1(String.valueOf(i))).toList();

    private HyperLogLogPlusPlus hyperLogLogPlusPlus;
    private AggregateCollector onHeapCollector;
    private OnHeapMemoryManager onHeapMemoryManager;
    private OffHeapMemoryManager offHeapMemoryManager;
    private AggregateCollector offHeapCollector;
    private MurmurHash3.Hash128 hash;

    @SuppressWarnings("unchecked")
    @Setup
    public void setUp() throws Exception {
        hash = new MurmurHash3.Hash128();
        final RowCollectExpression inExpr0 = new RowCollectExpression(0);
        Functions functions = Functions.load(Settings.EMPTY, new SessionSettingRegistry(Set.of()));
        final HyperLogLogDistinctAggregation hllAggregation = (HyperLogLogDistinctAggregation) functions.getQualified(
                Signature.builder(HyperLogLogDistinctAggregation.NAME, FunctionType.AGGREGATE)
                        .argumentTypes(DataTypes.STRING.getTypeSignature())
                        .returnType(DataTypes.LONG.getTypeSignature())
                        .features(Scalar.Feature.DETERMINISTIC)
                        .build(),
                List.of(DataTypes.STRING),
                DataTypes.STRING
        );
        onHeapMemoryManager = new OnHeapMemoryManager(bytes -> {});
        offHeapMemoryManager = new OffHeapMemoryManager();
        hyperLogLogPlusPlus = new HyperLogLogPlusPlus(HyperLogLogPlusPlus.DEFAULT_PRECISION, onHeapMemoryManager::allocate);
        onHeapCollector = new AggregateCollector(
            Collections.singletonList(inExpr0),
            RamAccounting.NO_ACCOUNTING,
            onHeapMemoryManager,
            Version.CURRENT,
            AggregateMode.ITER_FINAL,
            new AggregationFunction[] { hllAggregation },
            Version.CURRENT,
            new Input[][] { { inExpr0 } },
            new Input[] { Literal.BOOLEAN_TRUE }
        );
        offHeapCollector = new AggregateCollector(
            Collections.singletonList(inExpr0),
            RamAccounting.NO_ACCOUNTING,
            offHeapMemoryManager,
            Version.CURRENT,
            AggregateMode.ITER_FINAL,
            new AggregationFunction[] { hllAggregation },
            Version.CURRENT,
            new Input[][] { { inExpr0 } },
            new Input[] { Literal.BOOLEAN_TRUE }
        );
    }

    @TearDown
    public void tearDown() {
        onHeapMemoryManager.close();
        offHeapMemoryManager.close();
    }

    @Benchmark
    public long benchmarkHLLPlusPlusMurmur128() throws Exception {
        for (int i = 0; i < rows.size(); i++) {
            String value = DataTypes.STRING.sanitizeValue(rows.get(i).get(0));
            byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
            MurmurHash3.Hash128 hash128 = MurmurHash3.hash128(bytes, 0, bytes.length, 0, hash);
            hyperLogLogPlusPlus.collect(hash128.h1);
        }
        return hyperLogLogPlusPlus.cardinality();
    }

    @Benchmark
    public long benchmarkHLLPlusPlusMurmur64() throws Exception {
        for (int i = 0; i < rows.size(); i++) {
            String value = DataTypes.STRING.sanitizeValue(rows.get(i).get(0));
            byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
            hyperLogLogPlusPlus.collect(MurmurHash3.hash64(bytes, 0, bytes.length));
        }
        return hyperLogLogPlusPlus.cardinality();
    }

    @Benchmark
    public Iterable<Row> benchmarkHLLAggregationOnHeap() throws Exception {
        Object[] state = onHeapCollector.supplier().get();
        BiConsumer<Object[], Row> accumulator = onHeapCollector.accumulator();
        Function<Object[], Iterable<Row>> finisher = onHeapCollector.finisher();
        for (int i = 0; i < rows.size(); i++) {
            accumulator.accept(state, rows.get(i));
        }
        return finisher.apply(state);
    }

    @Benchmark
    public Iterable<Row> benchmarkHLLAggregationOffHeap() throws Exception {
        Object[] state = offHeapCollector.supplier().get();
        BiConsumer<Object[], Row> accumulator = offHeapCollector.accumulator();
        Function<Object[], Iterable<Row>> finisher = offHeapCollector.finisher();
        for (int i = 0; i < rows.size(); i++) {
            accumulator.accept(state, rows.get(i));
        }
        return finisher.apply(state);
    }
}
