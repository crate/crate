/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.operation.scalar.cast;

import com.google.common.base.Preconditions;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.operation.Input;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Symbol;
import io.crate.planner.symbol.SymbolFormatter;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

import java.util.List;
import java.util.Locale;

public abstract class ToPrimitiveFunction<T> extends Scalar<T, Object> {

    protected final DataType returnType;
    protected final FunctionInfo info;


    protected ToPrimitiveFunction(FunctionInfo functionInfo){
        this.returnType = functionInfo.returnType();
        this.info = functionInfo;
    }

    protected static void checkPreconditions(List<DataType> dataTypes) throws IllegalArgumentException {
        Preconditions.checkArgument(dataTypes.size() == 1,
                "invalid size of arguments, 1 expected");
        Preconditions.checkArgument(DataTypes.PRIMITIVE_TYPES.contains(dataTypes.get(0)),
                "type '%s' not supported for conversion", dataTypes.get(0));
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    @Override
    public Symbol normalizeSymbol(Function symbol) {
        assert symbol.arguments().size() == 1;
        Symbol argument = symbol.arguments().get(0);
        if (argument.symbolType().isValueSymbol()) {
            Object value = ((Input) argument).value();
            try {
                return Literal.newLiteral(returnType, returnType.value(value));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(
                        String.format(Locale.ENGLISH, "cannot cast %s to %s",
                                SymbolFormatter.format(argument), returnType), e);
            }
        }
        return symbol;
    }

    @Override
    public T evaluate(Input[] args) {
        assert args.length == 1;
        return (T)returnType.value(args[0].value());
    }
}
