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
import io.crate.data.*;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.Functions;
import io.crate.operation.aggregation.AggregationFunction;
import io.crate.operation.aggregation.impl.AggregationImplModule;
import io.crate.operation.aggregation.impl.MinimumAggregation;
import io.crate.operation.collect.CollectExpression;
import io.crate.operation.collect.InputCollectExpression;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class GroupingBytesRefCollectorBenchmark {

    private static final RamAccountingContext RAM_ACCOUNTING_CONTEXT =
        new RamAccountingContext("dummy", new NoopCircuitBreaker(CircuitBreaker.FIELDDATA));

    private GroupingCollector groupByMinCollector;
    private BatchIterator rowsIterator;
    private List<Row> rows;

    @Setup
    public void createGroupingCollector() {
        Functions functions = new ModulesBuilder().add(new AggregationImplModule())
            .createInjector().getInstance(Functions.class);

        groupByMinCollector = createGroupByMinBytesRefCollector(functions);

        List<BytesRef> keys = new ArrayList<>(Locale.getISOCountries().length);
        for (String s : Locale.getISOCountries()) {
            keys.add(new BytesRef(s));
        }

        rows = new ArrayList<>(20_000_000);
        for (int i = 0; i < 20_000_000; i++) {
            rows.add(new Row1(keys.get(i % keys.size())));
        }
    }

    private GroupingCollector createGroupByMinBytesRefCollector(Functions functions) {
        InputCollectExpression keyInput = new InputCollectExpression(0);
        List<Input<?>> keyInputs = Collections.singletonList(keyInput);
        CollectExpression[] collectExpressions = new CollectExpression[]{keyInput};

        FunctionIdent minBytesRefFuncIdent = new FunctionIdent(MinimumAggregation.NAME,
            Collections.singletonList(DataTypes.STRING));
        AggregationFunction minAgg = (AggregationFunction) functions.get(minBytesRefFuncIdent);

        return GroupingCollector.singleKey(
            collectExpressions,
            AggregateMode.ITER_FINAL,
            new AggregationFunction[] { minAgg },
            new Input[][] { new Input[] { keyInput }},
            RAM_ACCOUNTING_CONTEXT,
            keyInputs.get(0),
            DataTypes.STRING
        );
    }

    @Benchmark
    public void measureGroupByMinBytesRef(Blackhole blackhole) throws Exception {
        rowsIterator = RowsBatchIterator.newInstance(rows, 1);
        blackhole.consume(BatchRowVisitor.visitRows(rowsIterator, groupByMinCollector).get());
    }
}
