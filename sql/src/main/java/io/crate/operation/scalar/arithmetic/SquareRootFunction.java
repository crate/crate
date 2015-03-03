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

package io.crate.operation.scalar.arithmetic;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import io.crate.metadata.*;
import io.crate.operation.Input;
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Symbol;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

import java.util.List;
import java.util.Set;

public abstract class SquareRootFunction extends Scalar<Number, Number> {

    public static final String NAME = "sqrt";
    private static final Set<DataType> ALLOWED_TYPES = ImmutableSet.<DataType>builder()
            .addAll(DataTypes.NUMERIC_PRIMITIVE_TYPES)
            .add(DataTypes.UNDEFINED)
            .build();

    private final FunctionInfo info;


    public static void register(ScalarFunctionModule module){
        module.register(NAME, new Resolver());
    }

    public SquareRootFunction(FunctionInfo info){
        this.info = info;
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    @Override
    public Symbol normalizeSymbol(Function symbol) {
        Symbol argument = symbol.arguments().get(0);
        if (argument.symbolType().isLiteral()) {
            return Literal.newLiteral(info().returnType(), evaluate((Input) argument));
        }
        return symbol;
    }

    static class DoubleSquareRootFunction extends SquareRootFunction {

        public DoubleSquareRootFunction(FunctionInfo info){
            super(info);
        }

        @Override
        public Double evaluate(Input[] args) {
            Number value = (Number)args[0].value();
            if(value==null){
                return null;
            }
            Preconditions.checkArgument(value.doubleValue() >= 0, "cannot take square root of a negative number");
            return Math.sqrt(value.doubleValue());
        }

    }

    private static class Resolver implements DynamicFunctionResolver {

        @Override
        public FunctionImplementation<Function> getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
            Preconditions.checkArgument(dataTypes.size() == 1,
                    "invalid size of arguments, 1 expected");
            Preconditions.checkArgument(ALLOWED_TYPES.contains(dataTypes.get(0)),
                    "invalid datatype for %s function", NAME);
            return new DoubleSquareRootFunction(new FunctionInfo(new FunctionIdent(NAME, dataTypes), DataTypes.DOUBLE));
        }
    }


}
