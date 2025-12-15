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

package io.crate.execution.engine.collect;

import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.expression.InputRow;

import org.jspecify.annotations.Nullable;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * A Row implementation that is backed by some TValue
 * <p>
 * This is used for system tables to transform an Iterable of TValue into Row using the supplied inputs and collectExpression.
 * <p>
 * It will keep a reference to the original TValue to be used by {@link io.crate.execution.dml.SysUpdateProjector}
 */
public class ValueAndInputRow<TValue> extends InputRow implements Function<TValue, Row>, Supplier<TValue> {

    private final Iterable<? extends CollectExpression<TValue, ?>> expressions;
    private TValue value;

    public ValueAndInputRow(List<? extends Input<?>> inputs, Iterable<? extends CollectExpression<TValue, ?>> expressions) {
        super(inputs);
        this.expressions = expressions;
    }

    @Override
    public TValue get() {
        return value;
    }

    @Nullable
    @Override
    public Row apply(@Nullable TValue input) {
        value = input;
        for (CollectExpression<TValue, ?> expression : expressions) {
            expression.setNextRow(input);
        }
        return this;
    }
}
