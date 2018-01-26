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

package io.crate.expression.scalar.arithmetic;

import com.google.common.collect.ImmutableSet;
import io.crate.data.Input;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.expression.scalar.ScalarFunctionModule;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

import java.util.Arrays;
import java.util.Set;

public abstract class LogFunction extends Scalar<Number, Number> {

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
    Double validateResult(Double result, String caller) {
        if (result == null) {
            return null;
        }
        if (Double.isNaN(result) || Double.isInfinite(result)) {
            throw new IllegalArgumentException(caller + ": given arguments would result in: '" + result + "'");
        }
        return result;
    }

    LogFunction(FunctionInfo info) {
        this.info = info;
    }

    static class LogBaseFunction extends LogFunction {

        static void registerLogBaseFunctions(ScalarFunctionModule module) {
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

        LogBaseFunction(FunctionInfo info) {
            super(info);
        }

        @Override
        public Number evaluate(Input<Number>... args) {
            assert args.length == 2 : "number of args must be 2";
            Number value1 = args[0].value();
            Number value2 = args[1].value();

            if (value1 == null || value2 == null) {
                return null;
            }
            double value = value1.doubleValue();
            double base = value2.doubleValue();
            double baseResult = Math.log(base);
            if (baseResult == 0) {
                throw new IllegalArgumentException("log(x, b): given 'base' would result in a division by zero.");
            }
            return validateResult(Math.log(value) / baseResult, "log(x, b)");
        }

    }

    static class Log10Function extends LogFunction {

        static void registerLog10Functions(ScalarFunctionModule module) {
            // log(dataType) : double
            for (DataType dt : ALLOWED_TYPES) {
                FunctionInfo info = new FunctionInfo(new FunctionIdent(NAME, Arrays.asList(dt)), DataTypes.DOUBLE);
                module.register(new Log10Function(info));
            }
        }

        Log10Function(FunctionInfo info) {
            super(info);
        }

        @Override
        public Number evaluate(Input<Number>... args) {
            assert args.length == 1  : "number of args must be 1";
            Number value = args[0].value();
            if (value == null) {
                return null;
            }
            return evaluate(value.doubleValue());
        }

        protected Double evaluate(double value) {
            return validateResult(Math.log10(value), "log(x)");
        }

    }

    public static class LnFunction extends Log10Function {

        static void registerLnFunctions(ScalarFunctionModule module) {
            // ln(dataType) : double
            for (DataType dt : ALLOWED_TYPES) {
                FunctionInfo info = new FunctionInfo(new FunctionIdent(LnFunction.NAME, Arrays.asList(dt)), DataTypes.DOUBLE);
                module.register(new LnFunction(info));
            }
        }

        public static final String NAME = "ln";

        LnFunction(FunctionInfo info) {
            super(info);
        }

        @Override
        protected Double evaluate(double value) {
            return validateResult(Math.log(value), "ln(x)");
        }
    }

}
