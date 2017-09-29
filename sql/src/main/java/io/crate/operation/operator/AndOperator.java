/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.operation.operator;

import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import io.crate.data.Input;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.TransactionContext;
import io.crate.types.DataTypes;

import java.util.Arrays;
import java.util.Iterator;

public class AndOperator extends Operator<Boolean> {

    public static final String NAME = "op_and";
    public static final FunctionInfo INFO = generateInfo(NAME, DataTypes.BOOLEAN);

    @Override
    public FunctionInfo info() {
        return INFO;
    }

    public static void register(OperatorModule module) {
        module.registerOperatorFunction(new AndOperator());
    }

    @Override
    public Symbol normalizeSymbol(Function function, TransactionContext transactionContext) {
        assert function != null : "function must not be null";
        assert function.arguments().size() == 2 : "number of args must be 2";

        Symbol left = function.arguments().get(0);
        Symbol right = function.arguments().get(1);

        if (left instanceof Input && right instanceof Input) {
            return Literal.of(evaluate((Input) left, (Input) right));
        }

        /**
         * true  and x  -> x
         * false and x  -> false
         * null  and x  -> false or null -> function as is
         */
        if (left instanceof Input) {
            Object value = ((Input) left).value();
            if (value == null) {
                return function;
            }
            if ((Boolean) value) {
                return right;
            } else {
                return Literal.of(false);
            }
        }
        if (right instanceof Input) {
            Object value = ((Input) right).value();
            if (value == null) {
                return function;
            }
            if ((Boolean) value) {
                return left;
            } else {
                return Literal.of(false);
            }
        }
        return function;
    }

    @Override
    public Boolean evaluate(Input<Boolean>... args) {
        assert args != null : "args must not be null";
        assert args.length == 2 : "number of args must be 2";
        assert args[0] != null && args[1] != null : "1st and 2nd arguments must not be null";

        // implement three valued logic.
        // don't touch anything unless you have a good reason for it! :)
        // http://en.wikipedia.org/wiki/Three-valued_logic
        Boolean left = args[0].value();
        Boolean right = args[1].value();

        if (left == null && right == null) {
            return null;
        }

        if (left == null) {
            return (!right) ? false : null;
        }

        if (right == null) {
            return (!left) ? false : null;
        }

        return left && right;
    }

    public static Function of(Symbol first, Symbol second) {
        assert first.valueType().equals(DataTypes.BOOLEAN) || first.valueType().equals(DataTypes.UNDEFINED) :
            "first symbol must have BOOLEAN return type to create AND function";
        assert second.valueType().equals(DataTypes.BOOLEAN) || second.valueType().equals(DataTypes.UNDEFINED) :
            "second symbol must have BOOLEAN return type to create AND function";

        return new Function(INFO, Arrays.asList(first, second));
    }

    public static Symbol join(Iterable<? extends Symbol> symbols) {
        return join(symbols.iterator());
    }

    public static Symbol join(Iterator<? extends Symbol> symbols) {
        if (!symbols.hasNext()) {
            return Literal.BOOLEAN_TRUE;
        }
        Symbol first = symbols.next();
        while (symbols.hasNext()) {
            first = new Function(INFO, Arrays.asList(first, symbols.next()));
        }
        return first;
    }
}
