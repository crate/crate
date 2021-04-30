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

import io.crate.expression.NestableInput;

import java.util.function.Function;

/**
 * Base interface for row based expressions.
 *
 * @param <T> The type of the input to the expression
 * @param <R> The type of the result to the expression
 */
public interface NestableCollectExpression<T, R> extends CollectExpression<T, R>, NestableInput<R> {

    static <T, R> NestableCollectExpression<T, R> constant(R val) {
        return new ConstantNestableCollectExpression<>(val);
    }

    static <T, R> NestableCollectExpression<T, R> forFunction(Function<T, R> fun) {
        return new FuncExpression<>(fun);
    }

    class FuncExpression<T, R> implements NestableCollectExpression<T, R> {

        private final Function<T, R> f;
        private R value;

        FuncExpression(Function<T, R> f) {
            this.f = f;
        }

        @Override
        public void setNextRow(T tRow) {
            value = f.apply(tRow);
        }

        @Override
        public R value() {
            return value;
        }

        @Override
        public String toString() {
            return "FuncExpression{func=" + f + '}';
        }
    }

    class ConstantNestableCollectExpression<T, R> implements NestableCollectExpression<T, R> {
        private final R val;

        ConstantNestableCollectExpression(R val) {
            this.val = val;
        }

        @Override
        public void setNextRow(T tRow) {
        }

        @Override
        public R value() {
            return val;
        }
    }
}
