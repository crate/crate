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

package io.crate.operator.operator;

import io.crate.metadata.FunctionInfo;
import io.crate.operator.Input;
import io.crate.planner.symbol.BooleanLiteral;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Symbol;
import io.crate.planner.symbol.SymbolType;
import org.cratedb.DataType;

public class AndOperator extends Operator {

    public static final String NAME = "op_and";
    public static final FunctionInfo INFO = generateInfo(NAME, DataType.BOOLEAN);

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

        for (Symbol symbol : function.arguments()) {
            if ((symbol.symbolType() != SymbolType.BOOLEAN_LITERAL)) {
                return function; // can't optimize -> return unmodified symbol
            }
        }

        BooleanLiteral[] literals = new BooleanLiteral[function.arguments().size()];
        function.arguments().toArray(literals);
        return new BooleanLiteral(evaluate(literals));
    }

    @Override
    public Boolean evaluate(Input<?>... args) {
        if (args == null) {
            return false;
        }
        for (Input<?> input : args) {
            if (!(Boolean)input.value()) {
                return false;
            }
        }

        return true;
    }
}
