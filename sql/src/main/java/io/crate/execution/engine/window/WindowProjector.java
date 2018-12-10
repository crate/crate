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

package io.crate.execution.engine.window;

import io.crate.analyze.WindowDefinition;
import io.crate.breaker.RamAccountingContext;
import io.crate.data.BatchIterator;
import io.crate.data.Input;
import io.crate.data.Projector;
import io.crate.data.Row;
import io.crate.execution.engine.aggregation.AggregateCollector;
import io.crate.execution.engine.aggregation.AggregationFunction;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.expression.symbol.AggregateMode;
import org.elasticsearch.Version;
import org.elasticsearch.common.util.BigArrays;

import java.util.ArrayList;
import java.util.List;

public class WindowProjector implements Projector {

    private final WindowDefinition windowDefinition;
    private final AggregateCollector collector;
    private final List<Input<?>> standaloneInputs;
    private final int windowFunctionsCount;
    private final List<CollectExpression<Row, ?>> standaloneExpressions;

    public WindowProjector(WindowDefinition windowDefinition,
                           List<WindowFunctionContext> windowFunctions,
                           List<Input<?>> standaloneInputs,
                           List<CollectExpression<Row, ?>> standaloneExpressions,
                           RamAccountingContext ramAccountingContext,
                           Version indexVersionCreated,
                           BigArrays bigArrays) {
        this.windowDefinition = windowDefinition;
        this.standaloneInputs = standaloneInputs;
        this.standaloneExpressions = standaloneExpressions;
        this.windowFunctionsCount = windowFunctions.size();

        List<CollectExpression<Row, ?>> expressions = new ArrayList<>();

        AggregationFunction[] functions = new AggregationFunction[windowFunctions.size()];
        Input[][] inputs = new Input[windowFunctions.size()][];

        for (int i = 0; i < windowFunctions.size(); i++) {
            WindowFunctionContext functionContext = windowFunctions.get(i);
            functions[i] = functionContext.function();
            inputs[i] = functionContext.inputs().toArray(new Input[0]);
            expressions.addAll(functionContext.expressions());
        }

        collector = new AggregateCollector(
            expressions,
            ramAccountingContext,
            AggregateMode.ITER_FINAL,
            functions,
            indexVersionCreated,
            bigArrays,
            inputs
        );
    }

    @Override
    public BatchIterator<Row> apply(BatchIterator<Row> batchIterator) {
        return new WindowBatchIterator(
            windowDefinition,
            standaloneInputs,
            standaloneExpressions,
            windowFunctionsCount,
            batchIterator,
            collector);
    }
}
