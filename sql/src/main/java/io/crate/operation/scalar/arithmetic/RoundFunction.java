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

import com.google.common.collect.ImmutableList;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.operation.Input;
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Symbol;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

public abstract class RoundFunction extends Scalar<Number, Number> {

    public static final String NAME = "round";

    public static void register(ScalarFunctionModule module) {
        module.register(new FloatRoundFunction());
        module.register(new DoubleRoundFunction());
        module.register(new NoopRoundFunction(DataTypes.LONG));
        module.register(new NoopRoundFunction(DataTypes.INTEGER));
        module.register(new NoopRoundFunction(DataTypes.SHORT));
        module.register(new NoopRoundFunction(DataTypes.BYTE));
        module.register(new NoopRoundFunction(DataTypes.UNDEFINED));
    }

    @Override
    public Symbol normalizeSymbol(Function symbol) {
        Symbol argument = symbol.arguments().get(0);

        if (argument.symbolType().isLiteral()) {
            return Literal.newLiteral(info().returnType(), evaluate((Input) argument));
        }

        return symbol;
    }

    static class FloatRoundFunction extends RoundFunction {

        private final static FunctionInfo INFO = new FunctionInfo(
                new FunctionIdent(NAME, ImmutableList.<DataType>of(DataTypes.FLOAT)), DataTypes.INTEGER);

        @Override
        public FunctionInfo info() {
            return INFO;
        }

        @Override
        public Number evaluate(Input[] args) {
            Object value = args[0].value();
            if (value == null) {
                return null;
            }
            return Math.round(((Number) value).floatValue());
        }
    }

    static class DoubleRoundFunction extends RoundFunction {

        private final static FunctionInfo INFO = new FunctionInfo(
                new FunctionIdent(NAME, ImmutableList.<DataType>of(DataTypes.DOUBLE)), DataTypes.LONG);

        @Override
        public FunctionInfo info() {
            return INFO;
        }

        @Override
        public Number evaluate(Input[] args) {
            Object value = args[0].value();
            if (value == null) {
                return null;
            }
            return Math.round(((Number) value).doubleValue());
        }
    }

    private static class NoopRoundFunction extends RoundFunction {
        private final FunctionInfo info;

        public NoopRoundFunction(DataType type) {
            info = new FunctionInfo(new FunctionIdent(NAME, ImmutableList.of(type)), type);
        }

        @Override
        public FunctionInfo info() {
            return info;
        }

        @Override
        public Number evaluate(Input[] args) {
            return (Number) args[0].value();
        }
    }
}
