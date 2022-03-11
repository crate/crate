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

import com.koloboke.collect.map.hash.HashObjObjMaps;
import io.crate.breaker.RamAccounting;
import io.crate.data.BatchIterator;
import io.crate.data.BatchIterators;
import io.crate.data.InMemoryBatchIterator;
import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.execution.engine.aggregation.impl.AggregationImplModule;
import io.crate.execution.engine.aggregation.impl.MinimumAggregation;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.execution.engine.collect.InputCollectExpression;
import io.crate.expression.symbol.AggregateMode;
import io.crate.expression.symbol.Literal;
import io.crate.memory.OnHeapMemoryManager;
import io.crate.metadata.Functions;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataTypes;
import it.unimi.dsi.fastutil.Hash;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenCustomHashMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import net.jpountz.xxhash.XXHash32;
import net.jpountz.xxhash.XXHashFactory;
import org.elasticsearch.Version;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static io.crate.data.SentinelRow.SENTINEL;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class GroupingStringCollectorBenchmark {

    private GroupingCollector currentNettyMapCollector;
    private GroupingCollector kolobokeCollector;
    private GroupingCollector fastutilCollector;
    private GroupingCollector fastutilCustomHashCollector;
    private GroupingCollector jdkCollector;
    private BatchIterator<Row> rowsIterator;
    private List<Row> rows;
    private OnHeapMemoryManager memoryManager;

    private static final XXHash32 xxHash32 = XXHashFactory.fastestInstance().hash32();


    private static final class StringHashStrategy implements Hash.Strategy<String> {

        private static final StringHashStrategy INSTANCE = new StringHashStrategy();

        @Override
        public int hashCode(String o) {
            byte[] bytes = o.getBytes(StandardCharsets.UTF_8);
            return xxHash32.hash(bytes, 0, bytes.length, 0);
        }

        @Override
        public boolean equals(String a, String b) {
            return a.equals(b);
        }
    }

    @Setup
    public void createGroupingCollector() {
        Functions functions = new ModulesBuilder().add(new AggregationImplModule())
            .createInjector().getInstance(Functions.class);

        currentNettyMapCollector = createGroupByMinBytesRefCollector(functions, () -> new HashMap()); // We use JDK map for Strings currently.
        jdkCollector = createGroupByMinBytesRefCollector(functions, () -> new HashMap());
        kolobokeCollector = createGroupByMinBytesRefCollector(functions, () -> new PrimitiveMapWithNulls(HashObjObjMaps.newMutableMap()));
        fastutilCollector = createGroupByMinBytesRefCollector(functions, () -> new PrimitiveMapWithNulls(new Object2ObjectOpenHashMap()));
        fastutilCustomHashCollector = createGroupByMinBytesRefCollector(functions,
            () -> new PrimitiveMapWithNulls(
                new Object2ObjectOpenCustomHashMap(StringHashStrategy.INSTANCE)
            )
        );
        memoryManager = new OnHeapMemoryManager(bytes -> {});

        List<String> keys = new ArrayList<>(Locale.getISOCountries().length);
        keys.addAll(Arrays.asList(Locale.getISOCountries()));

        rows = new ArrayList<>(20_000_000);
        for (int i = 0; i < 20_000_000; i++) {
            rows.add(new Row1(keys.get(i % keys.size())));
        }
    }

    private GroupingCollector createGroupByMinBytesRefCollector(Functions functions, Supplier<Map<Object, Object[]>> supplier) {
        InputCollectExpression keyInput = new InputCollectExpression(0);
        List<Input<?>> keyInputs = Collections.singletonList(keyInput);
        CollectExpression[] collectExpressions = new CollectExpression[]{keyInput};

        MinimumAggregation minAgg = (MinimumAggregation) functions.getQualified(
            Signature.aggregate(
                MinimumAggregation.NAME,
                DataTypes.STRING.getTypeSignature(),
                DataTypes.STRING.getTypeSignature()
            ),
            List.of(DataTypes.STRING),
            DataTypes.STRING
        );

        return GroupingCollector.singleKey(
            collectExpressions,
            AggregateMode.ITER_FINAL,
            new AggregationFunction[] { minAgg },
            new Input[][] { new Input[] { keyInput }},
            new Input[] { Literal.BOOLEAN_TRUE },
            RamAccounting.NO_ACCOUNTING,
            memoryManager,
            Version.CURRENT,
            keyInputs.get(0),
            DataTypes.STRING,
            Version.CURRENT,
            supplier
        );
    }

    @Benchmark
    public void measureGroupByMinString(Blackhole blackhole) throws Exception {
        rowsIterator = InMemoryBatchIterator.of(rows, SENTINEL, true);
        blackhole.consume(BatchIterators.collect(rowsIterator, currentNettyMapCollector).get());
    }

    @Benchmark
    public void measureGroupByMinStringJDK(Blackhole blackhole) throws Exception {
        rowsIterator = InMemoryBatchIterator.of(rows, SENTINEL, true);
        blackhole.consume(BatchIterators.collect(rowsIterator, jdkCollector).get());
    }

    @Benchmark
    public void measureGroupByMinStringFastutil(Blackhole blackhole) throws Exception {
        rowsIterator = InMemoryBatchIterator.of(rows, SENTINEL, true);
        blackhole.consume(BatchIterators.collect(rowsIterator, fastutilCollector).get());
    }

    @Benchmark
    public void measureGroupByMinStringFastutilCustomHash(Blackhole blackhole) throws Exception {
        rowsIterator = InMemoryBatchIterator.of(rows, SENTINEL, true);
        blackhole.consume(BatchIterators.collect(rowsIterator, fastutilCustomHashCollector).get());
    }

    @Benchmark
    public void measureGroupByMinStringKoloboke(Blackhole blackhole) throws Exception {
        rowsIterator = InMemoryBatchIterator.of(rows, SENTINEL, true);
        blackhole.consume(BatchIterators.collect(rowsIterator, kolobokeCollector).get());
    }
}
