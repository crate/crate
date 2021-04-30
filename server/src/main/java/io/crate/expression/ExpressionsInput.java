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

package io.crate.expression;

import io.crate.data.Input;
import io.crate.execution.engine.collect.CollectExpression;

import java.util.List;
import java.util.RandomAccess;

/**
 * An {@link Input} that is linked and coupled with some {@link CollectExpression}
 */
public final class ExpressionsInput<TRow, TResult> {

    private final Input<TResult> input;
    private final List<? extends CollectExpression<TRow, ?>> expressions;

    public ExpressionsInput(Input<TResult> input, List<? extends CollectExpression<TRow, ?>> expressions) {
        assert expressions instanceof RandomAccess : "expressions must be a RandomAccess list to avoid iterator allocations";
        this.input = input;
        this.expressions = expressions;
    }

    public TResult value(TRow row) {
        for (int i = 0; i < expressions.size(); i++) {
            expressions.get(i).setNextRow(row);
        }
        return input.value();
    }

    @Override
    public String toString() {
        return "ExpressionsInput{input=" + input + '}';
    }
}
