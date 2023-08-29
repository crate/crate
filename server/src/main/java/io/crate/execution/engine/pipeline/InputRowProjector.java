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

import java.util.List;

import io.crate.data.BatchIterator;
import io.crate.data.Input;
import io.crate.data.Projector;
import io.crate.data.Row;
import io.crate.execution.engine.collect.CollectExpression;

/**
 * Projector which evaluates scalars or extends/cuts columns, see {@link MapRowUsingInputs}.
 */
public class InputRowProjector implements Projector {

    protected final List<Input<?>> inputs;
    protected final List<? extends CollectExpression<Row, ?>> collectExpressions;

    public InputRowProjector(List<Input<?>> inputs,
                             List<? extends CollectExpression<Row, ?>> collectExpressions) {
        this.inputs = inputs;
        this.collectExpressions = collectExpressions;
    }

    @Override
    public BatchIterator<Row> apply(BatchIterator<Row> batchIterator) {
        return batchIterator.map(new MapRowUsingInputs(inputs, collectExpressions));
    }

    @Override
    public boolean providesIndependentScroll() {
        return false;
    }
}
