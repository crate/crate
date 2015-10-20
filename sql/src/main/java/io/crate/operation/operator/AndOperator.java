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
import io.crate.metadata.FunctionInfo;
import io.crate.operation.Input;
import io.crate.types.DataTypes;

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
    public Symbol normalizeSymbol(Function function) {
        assert (function != null);
        assert function.arguments().size() == 2;

        Symbol left = function.arguments().get(0);
        Symbol right = function.arguments().get(1);

        if (left instanceof Input && right instanceof Input) {
            return Literal.newLiteral(evaluate((Input) left, (Input) right));
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
                return Literal.newLiteral(false);
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
                return Literal.newLiteral(false);
            }
        }
        return function;
    }

    @Override
    public Boolean evaluate(Input<Boolean>... args) {
        assert (args != null);
        assert (args.length == 2);
        assert (args[0] != null && args[1] != null);

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

}
