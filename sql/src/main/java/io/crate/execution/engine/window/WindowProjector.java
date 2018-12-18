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
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.types.DataType;

import javax.annotation.Nullable;
import java.util.List;

public class WindowProjector implements Projector {

    private final WindowDefinition windowDefinition;
    private final List<Input<?>> standaloneInputs;
    private final List<CollectExpression<Row, ?>> standaloneExpressions;
    private final List<WindowFunction> windowFunctions;
    @Nullable
    private final int[] orderByIndexes;
    private final List<DataType> outputTypes;
    private final RamAccountingContext ramAccountingContext;

    public WindowProjector(WindowDefinition windowDefinition,
                           List<WindowFunction> windowFunctions,
                           List<Input<?>> standaloneInputs,
                           List<CollectExpression<Row, ?>> standaloneExpressions,
                           List<DataType> outputTypes,
                           RamAccountingContext ramAccountingContext,
                           @Nullable int[] orderByIndexes) {
        this.windowDefinition = windowDefinition;
        this.standaloneInputs = standaloneInputs;
        this.standaloneExpressions = standaloneExpressions;
        this.windowFunctions = windowFunctions;
        this.orderByIndexes = orderByIndexes;
        this.outputTypes = outputTypes;
        this.ramAccountingContext = ramAccountingContext;
    }

    @Override
    public BatchIterator<Row> apply(BatchIterator<Row> batchIterator) {
        return new WindowBatchIterator(
            windowDefinition,
            standaloneInputs,
            standaloneExpressions,
            batchIterator,
            windowFunctions,
            outputTypes,
            ramAccountingContext,
            orderByIndexes);
    }
}
