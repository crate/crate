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
import io.crate.metadata.*;
import io.crate.operation.Input;
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Symbol;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

import java.util.List;

public abstract class ToIntFunction extends Scalar<Integer, Object> {

    public static final String NAME = "toInt";

    public static void register(ScalarFunctionModule module) {
        module.register(NAME, new Resolver());
    }

    protected final FunctionInfo info;

    public ToIntFunction(FunctionInfo info) {
        this.info = info;
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
            return Literal.toLiteral(argument, DataTypes.INTEGER);
        }
        return symbol;
    }

    private static class NumericToIntFunction extends ToIntFunction {

        public NumericToIntFunction(FunctionInfo info){
            super(info);
        }

        @Override
        public Integer evaluate(Input[] args) {
            assert args.length == 1;
            Object value = args[0].value();
            if (value == null) {
                return null;
            }
            return ((Number) value).intValue();
        }
    }

    private static class StringToIntFunction extends ToIntFunction {

        public StringToIntFunction(FunctionInfo info){
            super(info);
        }

        @Override
        public Integer evaluate(Input[] args) {
            assert args.length == 1;
            Object value = args[0].value();
            if (value == null) {
                return null;
            }
            return Integer.parseInt(value.toString());
        }
    }

    private static class Resolver implements DynamicFunctionResolver {

        @Override
        public FunctionImplementation<Function> getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
            Preconditions.checkArgument(dataTypes.size() == 1,
                    "invalid size of arguments, 1 expected");
            Preconditions.checkArgument(DataTypes.PRIMITIVE_TYPES.contains(dataTypes.get(0)),
                    "invalid datatype %s for integer conversion", dataTypes.get(0));
            if(dataTypes.get(0).id() == DataTypes.STRING.id()){
                return new StringToIntFunction(new FunctionInfo(new FunctionIdent(NAME, dataTypes), dataTypes.get(0)));
            }
            return new NumericToIntFunction(new FunctionInfo(new FunctionIdent(NAME, dataTypes), dataTypes.get(0)));
        }
    }
}
