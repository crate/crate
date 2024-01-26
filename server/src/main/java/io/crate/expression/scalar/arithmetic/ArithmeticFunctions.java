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

import static io.crate.expression.scalar.Scalar.DETERMINISTIC_ONLY;
import static io.crate.expression.scalar.Scalar.Feature.NULLABLE;

import io.crate.expression.scalar.BinaryScalar;
import io.crate.expression.scalar.ScalarFunctionModule;
import io.crate.expression.scalar.Scalar;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataTypes;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.BinaryOperator;

public class ArithmeticFunctions {

    public static class Names {
        public static final String ADD = "add";
        public static final String SUBTRACT = "subtract";
        public static final String MULTIPLY = "multiply";
        public static final String DIVIDE = "divide";
        public static final String POWER = "power";
        public static final String MODULUS = "modulus";
        public static final String MOD = "mod";
    }

    private enum Operations {
        ADD(
            Scalar.DETERMINISTIC_AND_COMPARISON_REPLACEMENT,
            Math::addExact,
            Double::sum,
            Math::addExact,
            Float::sum,
            BigDecimal::add
        ),
        SUBTRACT(
            DETERMINISTIC_ONLY,
            Math::subtractExact,
                (arg0, arg1) -> arg0 - arg1,
            Math::subtractExact,
                (arg0, arg1) -> arg0 - arg1,
            BigDecimal::subtract
        ),
        MULTIPLY(
            DETERMINISTIC_ONLY,
            Math::multiplyExact,
                (arg0, arg1) -> arg0 * arg1,
            Math::multiplyExact,
                (arg0, arg1) -> arg0 * arg1,
            BigDecimal::multiply
        ),
        DIVIDE(
            DETERMINISTIC_ONLY,
                (arg0, arg1) -> arg0 / arg1,
                (arg0, arg1) -> arg0 / arg1,
                (arg0, arg1) -> arg0 / arg1,
                (arg0, arg1) -> arg0 / arg1,
                (arg0, arg1) -> arg0.divide(arg1, MathContext.DECIMAL64)
        ),
        MODULUS(
            DETERMINISTIC_ONLY,
                (arg0, arg1) -> arg0 % arg1,
                (arg0, arg1) -> arg0 % arg1,
                (arg0, arg1) -> arg0 % arg1,
                (arg0, arg1) -> arg0 % arg1,
            BigDecimal::remainder
        ),
        MOD(
            DETERMINISTIC_ONLY,
                (arg0, arg1) -> arg0 % arg1,
                (arg0, arg1) -> arg0 % arg1,
                (arg0, arg1) -> arg0 % arg1,
                (arg0, arg1) -> arg0 % arg1,
            BigDecimal::remainder
        );

        private final Set<Scalar.Feature> features;

        private final BinaryOperator<Integer> integerFunction;
        private final BinaryOperator<Double> doubleFunction;
        private final BinaryOperator<Long> longFunction;
        private final BinaryOperator<Float> floatFunction;
        private final BinaryOperator<BigDecimal> bdFunction;

        Operations(Set<Scalar.Feature> features,
                   BinaryOperator<Integer> integerFunction,
                   BinaryOperator<Double> doubleFunction,
                   BinaryOperator<Long> longFunction,
                   BinaryOperator<Float> floatFunction,
                   BinaryOperator<BigDecimal> bdFunction) {
            this.features = features;
            this.doubleFunction = doubleFunction;
            this.integerFunction = integerFunction;
            this.longFunction = longFunction;
            this.floatFunction = floatFunction;
            this.bdFunction = bdFunction;
        }

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ENGLISH);
        }
    }

    public static void register(ScalarFunctionModule module) {
        for (var op : Operations.values()) {
            module.register(
                Signature.scalar(
                        op.toString(),
                        DataTypes.INTEGER.getTypeSignature(),
                        DataTypes.INTEGER.getTypeSignature(),
                        DataTypes.INTEGER.getTypeSignature()
                    )
                    .withFeatures(op.features)
                    .withFeature(NULLABLE),
                (signature, boundSignature) ->
                    new BinaryScalar<>(op.integerFunction, signature, boundSignature, DataTypes.INTEGER)
            );
            module.register(
                Signature.scalar(
                        op.toString(),
                        DataTypes.LONG.getTypeSignature(),
                        DataTypes.LONG.getTypeSignature(),
                        DataTypes.LONG.getTypeSignature()
                    )
                    .withFeatures(op.features)
                    .withFeature(NULLABLE),
                (signature, boundSignature) ->
                    new BinaryScalar<>(op.longFunction, signature, boundSignature, DataTypes.LONG)
            );
            if (op != Operations.SUBTRACT) {
                for (var type : List.of(DataTypes.TIMESTAMP, DataTypes.TIMESTAMPZ)) {
                    module.register(
                        Signature.scalar(
                                op.toString(),
                                type.getTypeSignature(),
                                type.getTypeSignature(),
                                type.getTypeSignature()
                            )
                            .withFeatures(op.features)
                            .withFeature(NULLABLE),
                        (signature, boundSignature) ->
                            new BinaryScalar<>(op.longFunction, signature, boundSignature, type)
                    );
                }
            }
            module.register(
                Signature.scalar(
                        op.toString(),
                        DataTypes.FLOAT.getTypeSignature(),
                        DataTypes.FLOAT.getTypeSignature(),
                        DataTypes.FLOAT.getTypeSignature()
                    )
                    .withFeatures(op.features)
                    .withFeature(NULLABLE),
                (signature, boundSignature) ->
                    new BinaryScalar<>(op.floatFunction, signature, boundSignature, DataTypes.FLOAT)
            );
            module.register(
                Signature.scalar(
                        op.toString(),
                        DataTypes.DOUBLE.getTypeSignature(),
                        DataTypes.DOUBLE.getTypeSignature(),
                        DataTypes.DOUBLE.getTypeSignature()
                    )
                    .withFeatures(op.features)
                    .withFeature(NULLABLE),
                (signature, boundSignature) ->
                    new BinaryScalar<>(op.doubleFunction, signature, boundSignature, DataTypes.DOUBLE)
            );
            module.register(
                Signature.scalar(
                        op.toString(),
                        DataTypes.NUMERIC.getTypeSignature(),
                        DataTypes.NUMERIC.getTypeSignature(),
                        DataTypes.NUMERIC.getTypeSignature()
                    )
                    .withFeatures(op.features)
                    .withFeature(NULLABLE),
                (signature, boundSignature) ->
                    new BinaryScalar<>(op.bdFunction, signature, boundSignature, DataTypes.NUMERIC)
            );
        }

        module.register(
            Signature.scalar(
                    Names.POWER,
                    DataTypes.DOUBLE.getTypeSignature(),
                    DataTypes.DOUBLE.getTypeSignature(),
                    DataTypes.DOUBLE.getTypeSignature()
                )
                .withFeatures(DETERMINISTIC_ONLY)
                .withFeature(NULLABLE),
            (signature, boundSignature) ->
                new BinaryScalar<>(Math::pow, signature, boundSignature, DataTypes.DOUBLE)
        );
    }
}
