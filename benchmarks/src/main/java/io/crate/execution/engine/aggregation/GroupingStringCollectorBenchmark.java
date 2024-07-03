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

import static io.crate.data.SentinelRow.SENTINEL;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

import io.crate.data.BatchIterator;
import io.crate.data.InMemoryBatchIterator;
import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.data.breaker.RamAccounting;
import io.crate.execution.engine.aggregation.impl.MinimumAggregation;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.execution.engine.collect.RowCollectExpression;
import io.crate.expression.symbol.AggregateMode;
import io.crate.expression.symbol.Literal;
import io.crate.memory.OnHeapMemoryManager;
import io.crate.metadata.FunctionType;
import io.crate.metadata.Functions;
import io.crate.metadata.Scalar;
import io.crate.metadata.functions.Signature;
import io.crate.metadata.settings.session.SessionSettingRegistry;
import io.crate.types.DataTypes;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class GroupingStringCollectorBenchmark {

    private GroupingCollector<?> groupByMinCollector;
    private BatchIterator<Row> rowsIterator;
    private List<Row> rows;
    private OnHeapMemoryManager memoryManager;

    @Setup
    public void createGroupingCollector() {
        Functions functions = Functions.load(Settings.EMPTY, new SessionSettingRegistry(Set.of()));

        groupByMinCollector = createGroupByMinBytesRefCollector(functions);
        memoryManager = new OnHeapMemoryManager(bytes -> {});

        List<String> keys = new ArrayList<>(Locale.getISOCountries().length);
        keys.addAll(Arrays.asList(Locale.getISOCountries()));

        rows = new ArrayList<>(20_000_000);
        for (int i = 0; i < 20_000_000; i++) {
            rows.add(new Row1(keys.get(i % keys.size())));
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private GroupingCollector<?> createGroupByMinBytesRefCollector(Functions functions) {
        RowCollectExpression keyInput = new RowCollectExpression(0);
        List<Input<?>> keyInputs = Collections.singletonList(keyInput);
        CollectExpression[] collectExpressions = new CollectExpression[]{keyInput};

        MinimumAggregation minAgg = (MinimumAggregation) functions.getQualified(
                Signature.builder(MinimumAggregation.NAME, FunctionType.AGGREGATE)
                        .argumentTypes(DataTypes.STRING.getTypeSignature())
                        .returnType(DataTypes.STRING.getTypeSignature())
                        .features(Scalar.Feature.DETERMINISTIC)
                        .build(),
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
            Version.CURRENT
        );
    }

    @Benchmark
    public void measureGroupByMinString(Blackhole blackhole) throws Exception {
        rowsIterator = InMemoryBatchIterator.of(rows, SENTINEL, true);
        blackhole.consume(rowsIterator.collect(groupByMinCollector).get());
    }
}
