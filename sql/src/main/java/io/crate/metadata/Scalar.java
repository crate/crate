/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.metadata;

import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.format.OperatorFormatSpec;
import io.crate.data.Input;

import java.util.Collection;
import java.util.List;

/**
 * Base class for Scalar functions in crate.
 * A Scalar function is a function which has zero or more arguments and returns a value. (not rows).
 * <p>
 * Argument types and return types are restricted to the types supported by Crate (see {@link io.crate.types.DataType})
 * </p>
 *
 * <p>
 *     Usually functions are registered as deterministic (See {@link io.crate.metadata.FunctionInfo.Feature}.
 *     If this is the case the function must be a pure function. Meaning that given the same input it must always produce
 *     the same output.
 *
 *     Functions also MUST NOT have any internal state that influences the result of future calls.
 *     Functions are used as singletons.
 *     An exception is if {@link #compile(List)} returns a NEW instance.
 * </p>
 *
 * @param <ReturnType> the class of the returned value
 */
public abstract class Scalar<ReturnType, InputType> implements FunctionImplementation {

    public static <R, I> Scalar<R, I> withOperator(Scalar<R, I> func, String operator) {
        return new OperatorScalar<>(func, operator);
    }


    /**
     * Evaluate the function using the provided arguments
     */
    public abstract ReturnType evaluate(Input<InputType>... args);

    /**
     * Called to return a "optimized" version of a scalar implementation.
     *
     * The returned instance will only be used in the context of a single query
     * (or rather, a subset of a single query if executed distributed).
     *
     * @param arguments arguments in symbol form. If any symbols are literals, any arguments passed to
     *                  {@link #evaluate(Input[])} will have the same value as those literals.
     *                  (Within the scope of a single operation)
     */
    public Scalar<ReturnType, InputType> compile(List<Symbol> arguments) {
        return this;
    }

    @Override
    public Symbol normalizeSymbol(Function symbol, TransactionContext transactionContext) {
        try {
            return evaluateIfLiterals(this, symbol);
        } catch (Throwable t) {
            return symbol;
        }
    }

    protected static boolean anyNonLiterals(Collection<? extends Symbol> arguments) {
        for (Symbol symbol : arguments) {
            if (!symbol.symbolType().isValueSymbol()) {
                return true;
            }
        }
        return false;
    }

    /**
     * This method will evaluate the function using the given scalar if all arguments are literals.
     * Otherwise it will return the function as is or NULL in case it contains a null literal
     */
    protected static <ReturnType, InputType> Symbol evaluateIfLiterals(Scalar<ReturnType, InputType> scalar, Function function) {
        List<Symbol> arguments = function.arguments();
        for (Symbol argument : arguments) {
            if (!(argument instanceof Input)) {
                return function;
            }
        }
        Input[] inputs = new Input[arguments.size()];
        int idx = 0;
        for (Symbol arg : arguments) {
            inputs[idx] = (Input) arg;
            idx++;
        }
        //noinspection unchecked
        return Literal.of(function.info().returnType(), scalar.evaluate(inputs));
    }

    private static class OperatorScalar<R, I> extends Scalar<R, I> implements OperatorFormatSpec {
        private final Scalar<R, I> func;
        private final String operator;

        OperatorScalar(Scalar<R, I> func, String operator) {
            this.func = func;
            this.operator = operator;
        }

        @Override
        public FunctionInfo info() {
            return func.info();
        }

        @Override
        public R evaluate(Input<I>... args) {
            return func.evaluate(args);
        }

        @Override
        public String operator(Function function) {
            return operator;
        }
    }
}
