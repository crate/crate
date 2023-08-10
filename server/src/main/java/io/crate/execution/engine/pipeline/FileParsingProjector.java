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

package io.crate.execution.engine.pipeline;

import io.crate.data.BatchIterator;
import io.crate.data.BatchIterators;
import io.crate.data.Input;
import io.crate.data.Projector;
import io.crate.data.Row;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.expression.InputFactory;
import io.crate.expression.reference.file.FileParsingExpression;
import io.crate.metadata.Reference;

import java.util.List;

/**
 * Similar to InputRowProjector, transforms a Row but can adjust targetReferences, inputs and expressions.
 * Also, doesn't create a MapRowUsingInputs instance on each apply.
 */
public class FileParsingProjector implements Projector {

    private final List<Input<?>> inputs;
    private final List<CollectExpression<Row, ?>> collectExpressions;
    private final MapRowUsingInputs mapRowUsingInputs;
    private final InputFactory.Context<CollectExpression<Row, ?>> ctx;

    public FileParsingProjector(InputFactory.Context<CollectExpression<Row, ?>> ctx) {
        this.ctx = ctx;
        this.inputs = ctx.topLevelInputs();
        List<CollectExpression<Row, ?>> expressions = ctx.expressions();
        for (CollectExpression<Row, ?> expression: expressions) {
            ((FileParsingExpression) expression).projector(this);
        }
        this.collectExpressions = expressions;
        this.mapRowUsingInputs = new MapRowUsingInputs(inputs, collectExpressions);
    }

    public void addColumn(Reference newColumn) {
        Input<?> input = ctx.add(newColumn);
        inputs.add(input);
        collectExpressions.add(ctx.expressions().get(ctx.expressions().size() - 1));
    }

    @Override
    public BatchIterator<Row> apply(BatchIterator<Row> batchIterator) {
        return BatchIterators.map(batchIterator, mapRowUsingInputs);
    }
}
