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

import com.google.common.collect.ImmutableSet;
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

import java.util.Arrays;
import java.util.Set;

public abstract class LogFunction extends Scalar<Number,Number> {

    public static final String NAME = "log";
    private static final Set<DataType> ALLOWED_TYPES = ImmutableSet.<DataType>builder()
            .addAll(DataTypes.NUMERIC_PRIMITIVE_TYPES)
            .add(DataTypes.UNDEFINED)
            .build();

    protected final FunctionInfo info;

    public static void register(ScalarFunctionModule module) {
        LogBaseFunction.registerLogBaseFunctions(module);
        Log10Function.registerLog10Functions(module);
        LnFunction.registerLnFunctions(module);
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    /**
     * @param result the value to validate
     * @param caller used in the error message for clarification purposes.
     * @return the validated result
     */
    protected Double validateResult(Double result, String caller) {
        if (result == null) {
            return null;
        }
        if (Double.isNaN(result) || Double.isInfinite(result)) {
            throw new IllegalArgumentException(caller + ": given arguments would result in: '" + result + "'");
        }
        return result;
    }

    public LogFunction(FunctionInfo info) {
        this.info = info;
    }

    static class LogBaseFunction extends LogFunction {

        protected static void registerLogBaseFunctions(ScalarFunctionModule module) {
            // log(valueType, baseType) : double
            for (DataType baseType : ALLOWED_TYPES) {
                for (DataType valueType : ALLOWED_TYPES) {
                    FunctionInfo info = new FunctionInfo(
                            new FunctionIdent(
                                    NAME,
                                    Arrays.asList(valueType, baseType)
                            ),
                            DataTypes.DOUBLE
                    );
                    module.register(new LogBaseFunction(info));
                }
            }
        }

        public LogBaseFunction(FunctionInfo info) {
            super(info);
        }

        @Override
        public Symbol normalizeSymbol(Function symbol) {
            assert (symbol.arguments().size() == 2);
            Symbol base = symbol.arguments().get(0);
            Symbol value = symbol.arguments().get(1);
            if (value.symbolType().isValueSymbol() && base.symbolType().isValueSymbol()) {
                return Literal.newLiteral(info.returnType(), evaluate((Input) base, (Input) value));
            }
            return symbol;
        }

        @Override
        public Number evaluate(Input<Number>... args) {
            assert args.length == 2;
            if (args[0].value() == null || args[1].value() == null) {
                return null;
            }
            double value = args[0].value().doubleValue();
            double base = args[1].value().doubleValue();
            double baseResult = Math.log(base);
            if (baseResult == 0) {
                throw new IllegalArgumentException("log(x, b): given 'base' would result in a division by zero.");
            }
            return validateResult(Math.log(value) / baseResult, "log(x, b)");
        }

    }

    static class Log10Function extends LogFunction {

        protected static void registerLog10Functions(ScalarFunctionModule module) {
            // log(dataType) : double
            for (DataType dt : ALLOWED_TYPES) {
                FunctionInfo info = new FunctionInfo(new FunctionIdent(NAME, Arrays.asList(dt)), DataTypes.DOUBLE);
                module.register(new Log10Function(info));
            }
        }

        public Log10Function(FunctionInfo info) {
            super(info);
        }

        @Override
        public Symbol normalizeSymbol(Function symbol) {
            assert (symbol.arguments().size() == 1);
            Symbol value = symbol.arguments().get(0);
            if (value.symbolType().isValueSymbol()) {
                return Literal.newLiteral(info.returnType(), evaluate((Input)value));
            }
            return symbol;
        }

        @Override
        public Number evaluate(Input<Number>... args) {
            assert args.length == 1;
            if (args[0].value() == null) {
                return null;
            }
            double value = args[0].value().doubleValue();
            return evaluate(value);
        }

        protected Double evaluate(double value) {
            return validateResult(Math.log10(value), "log(x)");
        }

    }

    public static class LnFunction extends Log10Function {

        protected static void registerLnFunctions(ScalarFunctionModule module) {
            // ln(dataType) : double
            for (DataType dt : ALLOWED_TYPES) {
                FunctionInfo info = new FunctionInfo(new FunctionIdent(LnFunction.NAME, Arrays.asList(dt)), DataTypes.DOUBLE);
                module.register(new LnFunction(info));
            }
        }

        public static final String NAME = "ln";

        public LnFunction(FunctionInfo info) {
            super(info);
        }

        @Override
        protected Double evaluate(double value) {
            return validateResult(Math.log(value), "ln(x)");
        }
    }

}
