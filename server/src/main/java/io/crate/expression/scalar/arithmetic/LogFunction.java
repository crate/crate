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

import static io.crate.metadata.functions.Signature.scalar;

import io.crate.data.Input;
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

    static class LogBaseFunction extends LogFunction {

        static void registerLogBaseFunctions(Functions.Builder builder) {
            // log(valueType, baseType) : double
            builder.add(
                scalar(
                    NAME,
                    Feature.NULLABLE,
                    DataTypes.DOUBLE.getTypeSignature(),
                    DataTypes.DOUBLE.getTypeSignature(),
                    TypeSignature.parse("double precision"))
                    .withFeature(Feature.DETERMINISTIC),
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
            double base = value2.doubleValue();
            double baseResult = Math.log(base);
            if (baseResult == 0) {
                throw new IllegalArgumentException("log(x, b): given 'base' would result in a division by zero.");
            }
            return validateResult(Math.log(value) / baseResult, "log(x, b)");
        }

    }

    static class Log10Function extends LogFunction {

        static void registerLog10Functions(Functions.Builder builder) {
            // log(double) : double
            builder.add(
                scalar(
                    NAME,
                    Feature.NULLABLE,
                    DataTypes.DOUBLE.getTypeSignature(),
                    DataTypes.DOUBLE.getTypeSignature())
                    .withFeature(Feature.DETERMINISTIC),
                Log10Function::new
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
            return validateResult(Math.log10(value), "log(x)");
        }

    }

    public static class LnFunction extends Log10Function {

        static void registerLnFunctions(Functions.Builder builder) {
            // ln(double) : double
            builder.add(
                scalar(
                    LnFunction.NAME,
                    Feature.NULLABLE,
                    DataTypes.DOUBLE.getTypeSignature(),
                    DataTypes.DOUBLE.getTypeSignature())
                    .withFeature(Feature.DETERMINISTIC),
                LnFunction::new
            );
        }

        public static final String NAME = "ln";

        LnFunction(Signature signature, BoundSignature boundSignature) {
            super(signature, boundSignature);
        }

        @Override
        protected Double evaluate(double value) {
            return validateResult(Math.log(value), "ln(x)");
        }
    }
}
