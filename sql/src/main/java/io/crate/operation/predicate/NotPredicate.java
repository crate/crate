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

package io.crate.operation.predicate;

import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.format.OperatorFormatSpec;
import io.crate.data.Input;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

import java.util.Arrays;

public class NotPredicate extends Scalar<Boolean, Boolean> implements OperatorFormatSpec {

    public static final String NAME = "op_not";
    public static final String OPERATOR_ALIAS = "NOT";
    public static final FunctionInfo INFO = new FunctionInfo(
        new FunctionIdent(NAME, Arrays.<DataType>asList(DataTypes.BOOLEAN)), DataTypes.BOOLEAN);

    public static void register(PredicateModule module) {
        module.register(new NotPredicate());
    }

    @Override
    public FunctionInfo info() {
        return INFO;
    }

    @Override
    public Symbol normalizeSymbol(Function symbol, TransactionContext transactionContext) {
        assert symbol != null : "function must not be null";
        assert symbol.arguments().size() == 1 : "function's number of arguments must be 1";

        Symbol arg = symbol.arguments().get(0);
        if (arg instanceof Input) {
            Object value = ((Input) arg).value();
            if (value == null) {
                /**
                 * WHERE NOT NULL -> WHERE NULL
                 */
                return Literal.NULL;
            }
            if (value instanceof Boolean) {
                return Literal.of(!((Boolean) value));
            }
        }
        return symbol;
    }

    @Override
    public Boolean evaluate(Input<Boolean>... args) {
        assert args.length == 1 : "number of args must be 1";
        Boolean value = args[0].value();
        return value != null ? !value : null;
    }

    @Override
    public String operator(Function function) {
        return OPERATOR_ALIAS;
    }
}
