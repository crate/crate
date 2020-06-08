/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.expression.scalar.arithmetic;

import io.crate.expression.scalar.ScalarFunctionModule;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataTypes;

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
            FunctionInfo.DETERMINISTIC_AND_COMPARISON_REPLACEMENT,
            Math::addExact,
            Double::sum,
            Math::addExact,
            Float::sum
        ),
        SUBTRACT(
            FunctionInfo.DETERMINISTIC_ONLY,
            Math::subtractExact,
                (arg0, arg1) -> arg0 - arg1,
            Math::subtractExact,
                (arg0, arg1) -> arg0 - arg1
        ),
        MULTIPLY(
            FunctionInfo.DETERMINISTIC_ONLY,
            Math::multiplyExact,
                (arg0, arg1) -> arg0 * arg1,
            Math::multiplyExact,
                (arg0, arg1) -> arg0 * arg1
        ),
        DIVIDE(
            FunctionInfo.DETERMINISTIC_ONLY,
                (arg0, arg1) -> arg0 / arg1,
                (arg0, arg1) -> arg0 / arg1,
                (arg0, arg1) -> arg0 / arg1,
                (arg0, arg1) -> arg0 / arg1
        ),
        MODULUS(
            FunctionInfo.DETERMINISTIC_ONLY,
                (arg0, arg1) -> arg0 % arg1,
                (arg0, arg1) -> arg0 % arg1,
                (arg0, arg1) -> arg0 % arg1,
                (arg0, arg1) -> arg0 % arg1
        ),
        MOD(
            FunctionInfo.DETERMINISTIC_ONLY,
                (arg0, arg1) -> arg0 % arg1,
                (arg0, arg1) -> arg0 % arg1,
                (arg0, arg1) -> arg0 % arg1,
                (arg0, arg1) -> arg0 % arg1
        );

        private final Set<FunctionInfo.Feature> features;

        private final BinaryOperator<Integer> integerFunction;
        private final BinaryOperator<Double> doubleFunction;
        private final BinaryOperator<Long> longFunction;
        private final BinaryOperator<Float> floatFunction;

        Operations(Set<FunctionInfo.Feature> features,
                   BinaryOperator<Integer> integerFunction,
                   BinaryOperator<Double> doubleFunction,
                   BinaryOperator<Long> longFunction,
                   BinaryOperator<Float> floatFunction) {
            this.features = features;
            this.doubleFunction = doubleFunction;
            this.integerFunction = integerFunction;
            this.longFunction = longFunction;
            this.floatFunction = floatFunction;
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
                ),
                (signature, args) ->
                    new BinaryScalar<>(op.integerFunction, op.toString(), signature, DataTypes.INTEGER, op.features)
            );
            for (var type : List.of(DataTypes.LONG, DataTypes.TIMESTAMP, DataTypes.TIMESTAMPZ)) {
                module.register(
                    Signature.scalar(
                        op.toString(),
                        type.getTypeSignature(),
                        type.getTypeSignature(),
                        type.getTypeSignature()
                    ),
                    (signature, args) ->
                        new BinaryScalar<>(op.longFunction, op.toString(), signature, type, op.features)
                );
            }
            module.register(
                Signature.scalar(
                    op.toString(),
                    DataTypes.FLOAT.getTypeSignature(),
                    DataTypes.FLOAT.getTypeSignature(),
                    DataTypes.FLOAT.getTypeSignature()
                ),
                (signature, args) ->
                    new BinaryScalar<>(op.floatFunction, op.toString(), signature, DataTypes.FLOAT, op.features)
            );
            module.register(
                Signature.scalar(
                    op.toString(),
                    DataTypes.DOUBLE.getTypeSignature(),
                    DataTypes.DOUBLE.getTypeSignature(),
                    DataTypes.DOUBLE.getTypeSignature()
                ),
                (signature, args) ->
                    new BinaryScalar<>(op.doubleFunction, op.toString(), signature, DataTypes.DOUBLE, op.features)
            );
        }

        module.register(
            Signature.scalar(
                Names.POWER,
                DataTypes.DOUBLE.getTypeSignature(),
                DataTypes.DOUBLE.getTypeSignature(),
                DataTypes.DOUBLE.getTypeSignature()
            ),
            (signature, dataTypes) ->
                new BinaryScalar<>(Math::pow, Names.POWER, signature, DataTypes.DOUBLE, FunctionInfo.DETERMINISTIC_ONLY)
        );
    }
}
