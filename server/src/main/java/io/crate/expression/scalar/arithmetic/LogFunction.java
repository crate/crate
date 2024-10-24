/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.expression.scalar.arithmetic;

import java.math.BigDecimal;
import java.math.MathContext;

import ch.obermuhlner.math.big.BigDecimalMath;
import io.crate.data.Input;
import io.crate.expression.scalar.UnaryScalar;
import io.crate.metadata.FunctionType;
import io.crate.metadata.Functions;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataTypes;
import io.crate.types.TypeSignature;

public abstract class LogFunction extends Scalar<Number, Number> {

    public static final String NAME = "log";

    public static void register(Functions.Builder module) {
        LogBaseFunction.registerLogBaseFunctions(module);
        Log10Function.registerLog10Functions(module);
        LnFunction.registerLnFunctions(module);
    }

    LogFunction(Signature signature, BoundSignature boundSignature) {
        super(signature, boundSignature);
    }

    /**
     * @param argument the value to validate
     * @param caller used in the error message for clarification purposes.
     * @return the validated argument
     */
    Double validateArgument(Double argument, String caller) {
        if (argument == null) {
            return null;
        }
        if (argument == 0.0) {
            throw new IllegalArgumentException(caller + ": given arguments would result in: '-Infinity'");
        }
        if (argument < 0.0) {
            throw new IllegalArgumentException(caller + ": given arguments would result in: 'NaN'");
        }
        return argument;
    }

    /**
     * @param argument the value to validate
     * @param caller used in the error message for clarification purposes.
     * @return the validated argument
     */
    static BigDecimal validateArgument(BigDecimal argument, String caller) {
        if (argument == null) {
            return null;
        }
        if (argument.compareTo(BigDecimal.ZERO) == 0) {
            throw new IllegalArgumentException(caller + ": given arguments would result in: '-Infinity'");
        }
        if (argument.compareTo(BigDecimal.ZERO) < 0) {
            throw new IllegalArgumentException(caller + ": given arguments would result in: 'NaN'");
        }
        return argument;
    }

    static class LogBaseFunction extends LogFunction {

        static void registerLogBaseFunctions(Functions.Builder builder) {
            // log(valueType, baseType) : double
            builder.add(
                Signature.builder(NAME, FunctionType.SCALAR)
                    .argumentTypes(DataTypes.DOUBLE.getTypeSignature(),
                        DataTypes.DOUBLE.getTypeSignature())
                    .returnType(TypeSignature.parse("double precision"))
                    .features(Feature.DETERMINISTIC, Feature.STRICTNULL)
                    .build(),
                LogBaseFunction::new
            );
        }

        LogBaseFunction(Signature signature, BoundSignature boundSignature) {
            super(signature, boundSignature);
        }

        @Override
        public Number evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input<Number>... args) {
            assert args.length == 2 : "number of args must be 2";
            Number value1 = args[0].value();
            Number value2 = args[1].value();

            if (value1 == null || value2 == null) {
                return null;
            }
            double value = value1.doubleValue();
            validateArgument(value, "log(x, b)");
            double base = value2.doubleValue();
            validateArgument(base, "log(x, b)");
            double baseResult = Math.log(base);
            if (baseResult == 0) {
                throw new IllegalArgumentException("log(x, b): given 'base' would result in a division by zero.");
            }
            return Math.log(value) / baseResult;
        }

    }

    static class Log10Function extends LogFunction {

        static void registerLog10Functions(Functions.Builder builder) {
            // log(double) : double
            builder.add(
                Signature.builder(NAME, FunctionType.SCALAR)
                    .argumentTypes(DataTypes.DOUBLE.getTypeSignature())
                    .returnType(DataTypes.DOUBLE.getTypeSignature())
                    .features(Feature.DETERMINISTIC, Feature.STRICTNULL)
                    .build(),
                Log10Function::new
            );
            // log(numeric) : numeric
            builder.add(
                Signature.builder(NAME, FunctionType.SCALAR)
                    .argumentTypes(DataTypes.NUMERIC.getTypeSignature())
                    .returnType(DataTypes.NUMERIC.getTypeSignature())
                    .features(Feature.DETERMINISTIC, Feature.STRICTNULL)
                    .build(),
                (signature, ignoredBoundSignature) -> new UnaryScalar<>(
                    signature,
                    BoundSignature.sameAsUnbound(signature),
                    DataTypes.NUMERIC,
                    x -> BigDecimalMath.log10(validateArgument(x, "log(x)"), MathContext.DECIMAL128)
                )
            );
        }

        Log10Function(Signature signature, BoundSignature boundSignature) {
            super(signature, boundSignature);
        }

        @Override
        public Number evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input<Number>... args) {
            assert args.length == 1 : "number of args must be 1";
            Number value = args[0].value();
            if (value == null) {
                return null;
            }
            return evaluate(value.doubleValue());
        }

        protected Double evaluate(double value) {
            return Math.log10(validateArgument(value, "log(x)"));
        }
    }

    public static class LnFunction extends Log10Function {

        static void registerLnFunctions(Functions.Builder builder) {
            // ln(double) : double
            builder.add(
                Signature.builder(LnFunction.NAME, FunctionType.SCALAR)
                    .argumentTypes(DataTypes.DOUBLE.getTypeSignature())
                    .returnType(DataTypes.DOUBLE.getTypeSignature())
                    .features(Feature.DETERMINISTIC, Feature.STRICTNULL)
                    .build(),
                LnFunction::new
            );
            // ln(numeric) : numeric
            builder.add(
                Signature.builder(LnFunction.NAME, FunctionType.SCALAR)
                    .argumentTypes(DataTypes.NUMERIC.getTypeSignature())
                    .returnType(DataTypes.NUMERIC.getTypeSignature())
                    .features(Feature.DETERMINISTIC, Feature.STRICTNULL)
                    .build(),
                (signature, ignoredBoundSignature) -> new UnaryScalar<>(
                    signature,
                    BoundSignature.sameAsUnbound(signature),
                    DataTypes.NUMERIC,
                    x -> BigDecimalMath.log(validateArgument(x, "ln(x)"), MathContext.DECIMAL128)
                )
            );
        }

        public static final String NAME = "ln";

        LnFunction(Signature signature, BoundSignature boundSignature) {
            super(signature, boundSignature);
        }

        @Override
        protected Double evaluate(double value) {
            return Math.log(validateArgument(value, "ln(x)"));
        }
    }
}
