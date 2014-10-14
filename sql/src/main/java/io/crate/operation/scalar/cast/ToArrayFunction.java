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
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

import java.util.List;
import java.util.Locale;

public abstract class ToArrayFunction<T> extends Scalar<T[], Object> {

    protected final DataType returnType;
    private final FunctionInfo info;

    protected ToArrayFunction(FunctionInfo functionInfo) {
        this.returnType = functionInfo.returnType();
        this.info = functionInfo;
    }

    protected static void checkPreconditions(List<DataType> dataTypes) throws IllegalArgumentException {
        Preconditions.checkArgument(dataTypes.size() == 1, "Invalid number of arguments");
        Preconditions.checkArgument(dataTypes.get(0) instanceof ArrayType, "Argument must be an array type");
        ArrayType arrayType = (ArrayType) dataTypes.get(0);
        Preconditions.checkArgument(DataTypes.PRIMITIVE_TYPES.contains(arrayType.innerType()),
                String.format(Locale.ENGLISH, "Array inner type '%s' not supported for conversion",
                        arrayType.innerType().getName()));
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    @Override
    public Symbol normalizeSymbol(Function symbol) {
        final int size = symbol.arguments().size();
        assert size == 1 : "Invalid number of arguments";

        if (anyNonLiterals(symbol.arguments())) {
            return symbol;
        }
        final Object inputValue = ((Input) symbol.arguments().get(0)).value();
        return Literal.newLiteral(returnType, returnType.value(inputValue));
    }

    @Override
    @SuppressWarnings("unchecked")
    public T[] evaluate(Input[] args) {
        assert args.length == 1 : "Number of arguments must be 1";
        return (T[]) returnType.value(args[0].value());
    }
}
