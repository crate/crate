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
import io.crate.operation.Input;

import java.util.Collection;
import java.util.List;

/**
 * evaluable function implementation
 *
 * @param <ReturnType> the class of the returned value
 */
public abstract class Scalar<ReturnType, InputType> implements FunctionImplementation {

    public abstract ReturnType evaluate(Input<InputType>... args);

    /**
     * Returns a optional compiled version of the scalar implementation.
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
    private static <ReturnType, InputType> Symbol evaluateIfLiterals(Scalar<ReturnType, InputType> scalar, Function function) {
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
}
