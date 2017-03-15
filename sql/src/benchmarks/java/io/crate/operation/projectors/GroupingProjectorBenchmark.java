/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.operation.projectors;

import com.carrotsearch.junitbenchmarks.BenchmarkRule;
import io.crate.analyze.symbol.Aggregation;
import io.crate.analyze.symbol.InputColumn;
import io.crate.analyze.symbol.Symbol;
import io.crate.breaker.RamAccountingContext;
import io.crate.data.Row;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Functions;
import io.crate.operation.AggregationContext;
import io.crate.data.Input;
import io.crate.operation.aggregation.AggregationFunction;
import io.crate.operation.aggregation.impl.AggregationImplModule;
import io.crate.operation.aggregation.impl.MinimumAggregation;
import io.crate.operation.aggregation.impl.SumAggregation;
import io.crate.operation.collect.CollectExpression;
import io.crate.operation.collect.InputCollectExpression;
import io.crate.testing.RowCountRowReceiver;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

public class GroupingProjectorBenchmark {

    private static final RamAccountingContext RAM_ACCOUNTING_CONTEXT =
        new RamAccountingContext("dummy", new NoopCircuitBreaker(CircuitBreaker.FIELDDATA));

    @Rule
    public BenchmarkRule benchmarkRule = new BenchmarkRule();

    class SpareRow implements Row {

        Object value;

        @Override
        public int numColumns() {
            return 1;
        }

        @Override
        public Object get(int index) {
            return value;
        }

        @Override
        public Object[] materialize() {
            return new Object[]{value};
        }
    }

    @Test
    public void testGroupByMinBytesRef() throws Exception {
        Functions functions = new ModulesBuilder().add(new AggregationImplModule())
            .createInjector().getInstance(Functions.class);

        InputCollectExpression keyInput = new InputCollectExpression(0);
        List<Input<?>> keyInputs = Arrays.<Input<?>>asList(keyInput);
        CollectExpression[] collectExpressions = new CollectExpression[]{keyInput};

        FunctionIdent minStringFuncIdent = new FunctionIdent(MinimumAggregation.NAME,
            Arrays.<DataType>asList(DataTypes.STRING));
        FunctionInfo minStringFuncInfo = new FunctionInfo(minStringFuncIdent, DataTypes.STRING, FunctionInfo.Type.AGGREGATE);
        AggregationFunction minAgg = (AggregationFunction) functions
            .get(minStringFuncIdent.schema(), minStringFuncIdent.name(), minStringFuncIdent.argumentTypes());
        Aggregation aggregation = Aggregation.finalAggregation(minStringFuncInfo,
            Arrays.<Symbol>asList(new InputColumn(0)), Aggregation.Step.ITER);
        AggregationContext aggregationContext = new AggregationContext(minAgg, aggregation);
        aggregationContext.addInput(keyInput);
        AggregationContext[] aggregations = new AggregationContext[]{aggregationContext};
        GroupingProjector groupingProjector = new GroupingProjector(
            Arrays.<DataType>asList(DataTypes.STRING), keyInputs, collectExpressions, aggregations, RAM_ACCOUNTING_CONTEXT);
        RowReceiver finalReceiver = new RowCountRowReceiver();
        groupingProjector.downstream(finalReceiver);


        List<BytesRef> keys = new ArrayList<>(Locale.getISOCountries().length);
        for (String s : Locale.getISOCountries()) {
            keys.add(new BytesRef(s));
        }

        SpareRow row = new SpareRow();
        for (int i = 0; i < 20_000_000; i++) {
            row.value = keys.get(i % keys.size());
            groupingProjector.setNextRow(row);
        }

        groupingProjector.finish(RepeatHandle.UNSUPPORTED);
    }

    @Test
    public void testGroupBySumInteger() throws Exception {
        Functions functions = new ModulesBuilder().add(new AggregationImplModule())
            .createInjector().getInstance(Functions.class);

        InputCollectExpression keyInput = new InputCollectExpression(0);
        List<Input<?>> keyInputs = Arrays.<Input<?>>asList(keyInput);
        CollectExpression[] collectExpressions = new CollectExpression[]{keyInput};

        FunctionIdent functionIdent = new FunctionIdent(SumAggregation.NAME,
            Arrays.<DataType>asList(DataTypes.INTEGER));
        FunctionInfo functionInfo = new FunctionInfo(functionIdent, DataTypes.INTEGER, FunctionInfo.Type.AGGREGATE);
        AggregationFunction minAgg = (AggregationFunction) functions
            .get(functionIdent.schema(), functionIdent.name(), functionIdent.argumentTypes());
        Aggregation aggregation = Aggregation.finalAggregation(functionInfo,
            Arrays.<Symbol>asList(new InputColumn(0)), Aggregation.Step.ITER);
        AggregationContext aggregationContext = new AggregationContext(minAgg, aggregation);
        aggregationContext.addInput(keyInput);
        AggregationContext[] aggregations = new AggregationContext[]{aggregationContext};
        GroupingProjector groupingProjector = new GroupingProjector(
            Arrays.<DataType>asList(DataTypes.INTEGER), keyInputs, collectExpressions, aggregations, RAM_ACCOUNTING_CONTEXT);
        RowReceiver finalReceiver = new RowCountRowReceiver();
        groupingProjector.downstream(finalReceiver);

        SpareRow row = new SpareRow();
        for (int i = 0; i < 20_000_000; i++) {
            row.value = i % 200;
            groupingProjector.setNextRow(row);
        }

        groupingProjector.finish(RepeatHandle.UNSUPPORTED);
    }
}
