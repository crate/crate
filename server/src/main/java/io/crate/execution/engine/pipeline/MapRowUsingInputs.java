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
import java.util.RandomAccess;
import java.util.function.UnaryOperator;

import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.expression.InputRow;

/**
 * Function that transforms a Row using the supplied {@link Input}s and {@link CollectExpression}s.
 *
 * Example:
 * <pre>
 *     Input: add(inputColumn(0), 2)
 *     Expressions: InputColumnExpression
 *
 *     input:
 *      Row[1]
 *     output:
 *      Row[3]
 * </pre>
 *
 * Note that the returned row is a shared object and re-used between calls.
 */
public final class MapRowUsingInputs implements UnaryOperator<Row> {

    private final List<? extends CollectExpression<Row, ?>> expressions;
    private final Row resultRow;

    public MapRowUsingInputs(List<? extends Input<?>> inputs,
                             List<? extends CollectExpression<Row, ?>> expressions) {
        assert expressions instanceof RandomAccess
            : "Must be able to use fast indexed for loop to avoid iterator allocations";
        this.expressions = expressions;
        this.resultRow = new InputRow(inputs);
    }

    @Override
    public Row apply(Row row) {
        for (int i = 0; i < expressions.size(); i++) {
            expressions.get(i).setNextRow(row);
        }
        return resultRow;
    }
}
