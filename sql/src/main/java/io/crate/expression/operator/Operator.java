/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.expression.operator;

import com.google.common.collect.ImmutableList;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.format.OperatorFormatSpec;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

import java.util.Locale;

public abstract class Operator<I> extends Scalar<Boolean, I> implements OperatorFormatSpec {

    public static final io.crate.types.DataType RETURN_TYPE = DataTypes.BOOLEAN;
    public static final String PREFIX = "op_";

    @Override
    public String operator(Function function) {
        // strip "op_" from function name
        return info().ident().name().substring(3).toUpperCase(Locale.ENGLISH);
    }

    @Override
    public Symbol normalizeSymbol(Function function, TransactionContext transactionContext) {
        // all operators evaluates to NULL if one argument is NULL
        // let's handle this here to prevent unnecessary collect operations
        for (Symbol symbol : function.arguments()) {
            if (isNull(symbol)) {
                return Literal.NULL;
            }
        }
        return super.normalizeSymbol(function, transactionContext);
    }

    private static boolean isNull(Symbol symbol) {
        return symbol.symbolType().isValueSymbol() && ((Literal) symbol).value() == null;
    }

    protected static FunctionInfo generateInfo(String name, DataType type) {
        return new FunctionInfo(new FunctionIdent(name, ImmutableList.of(type, type)), RETURN_TYPE);
    }
}
