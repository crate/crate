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

import io.crate.analyze.symbol.Aggregation;
import io.crate.analyze.symbol.InputColumn;
import io.crate.breaker.RamAccountingContext;
import io.crate.data.*;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Functions;
import io.crate.operation.aggregation.AggregationFunction;
import io.crate.operation.aggregation.Aggregator;
import io.crate.operation.aggregation.impl.AggregationImplModule;
import io.crate.operation.aggregation.impl.SumAggregation;
import io.crate.operation.collect.CollectExpression;
import io.crate.operation.collect.InputCollectExpression;
import io.crate.types.DataTypes;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class GroupingIntegerCollectorBenchmark {

    private static final RamAccountingContext RAM_ACCOUNTING_CONTEXT =
        new RamAccountingContext("dummy", new NoopCircuitBreaker(CircuitBreaker.FIELDDATA));

    private GroupingCollector groupBySumCollector;
    private BatchIterator rowsIterator;

    @Setup
    public void createGroupingCollector() {
        Functions functions = new ModulesBuilder().add(new AggregationImplModule())
            .createInjector().getInstance(Functions.class);
        groupBySumCollector = createGroupBySumCollector(functions);

        List<Row> rows = new ArrayList<>(20_000_000);
        for (int i = 0; i < 20_000_000; i++) {
            rows.add(new Row1(i % 200));
        }
        rowsIterator = RowsBatchIterator.newInstance(rows, 1);
    }

    private GroupingCollector createGroupBySumCollector(Functions functions) {
        InputCollectExpression keyInput = new InputCollectExpression(0);
        List<Input<?>> keyInputs = Arrays.<Input<?>>asList(keyInput);
        CollectExpression[] collectExpressions = new CollectExpression[]{keyInput};

        FunctionIdent functionIdent = new FunctionIdent(SumAggregation.NAME,
            Arrays.asList(DataTypes.INTEGER));
        FunctionInfo functionInfo = new FunctionInfo(functionIdent, DataTypes.INTEGER, FunctionInfo.Type.AGGREGATE);
        AggregationFunction sumAgg = (AggregationFunction) functions.get(functionIdent);
        Aggregation aggregation = Aggregation.finalAggregation(functionInfo,
            Arrays.asList(new InputColumn(0)), Aggregation.Step.ITER);

        Aggregator[] aggregators = new Aggregator[]{
            new Aggregator(
                RAM_ACCOUNTING_CONTEXT,
                aggregation,
                sumAgg,
                new Input[]{keyInput}
            )
        };

        return GroupingCollector.singleKey(
            collectExpressions,
            aggregators,
            RAM_ACCOUNTING_CONTEXT,
            keyInputs.get(0),
            DataTypes.INTEGER
        );
    }

    @Benchmark
    public void measureGroupBySumInteger(Blackhole blackhole) throws Exception {
        blackhole.consume(BatchRowVisitor.visitRows(rowsIterator, groupBySumCollector).get());
        rowsIterator.moveToStart();
    }
}
