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

    public static void register(ScalarFunctionModule module) {
        for (var operatorName : List.of(Names.ADD,
                                        Names.SUBTRACT,
                                        Names.MULTIPLY,
                                        Names.DIVIDE,
                                        Names.MOD, Names.MODULUS)) {
            for (var supportType : List.of(DataTypes.BYTE, DataTypes.SHORT, DataTypes.INTEGER)) {
                final BinaryOperator<Integer> operator;
                switch (operatorName) {
                    case Names.ADD:
                        operator = Math::addExact;
                        break;
                    case Names.SUBTRACT:
                        operator = Math::subtractExact;
                        break;
                    case Names.MULTIPLY:
                        operator = Math::multiplyExact;
                        break;
                    case Names.DIVIDE:
                        operator = (x, y) -> x / y;
                        break;
                    case Names.MOD:
                    case Names.MODULUS:
                        operator = (x, y) -> x % y;
                        break;
                    default:
                        throw new IllegalStateException("Unexpected value: " + "");
                }
                module.register(
                    Signature.scalar(
                        operatorName,
                        supportType.getTypeSignature(),
                        supportType.getTypeSignature(),
                        DataTypes.INTEGER.getTypeSignature()),
                    (signature, dataTypes) -> new BinaryScalar<>(
                        operator,
                        operatorName,
                        signature,
                        DataTypes.INTEGER,
                        FunctionInfo.DETERMINISTIC_ONLY)
                );
            }
            for (var supportType : List.of(DataTypes.LONG, DataTypes.TIMESTAMPZ, DataTypes.TIMESTAMP)) {
                final BinaryOperator<Long> operator;
                switch (operatorName) {
                    case Names.ADD:
                        operator = Math::addExact;
                        break;
                    case Names.SUBTRACT:
                        operator = Math::subtractExact;
                        break;
                    case Names.MULTIPLY:
                        operator = Math::multiplyExact;
                        break;
                    case Names.DIVIDE:
                        operator = (x, y) -> x / y;
                        break;
                    case Names.MOD:
                    case Names.MODULUS:
                        operator = (x, y) -> x % y;
                        break;
                    default:
                        throw new IllegalStateException("Unexpected value: " + "");
                }
                module.register(
                    Signature.scalar(
                        operatorName,
                        supportType.getTypeSignature(),
                        supportType.getTypeSignature(),
                        DataTypes.LONG.getTypeSignature()),
                    (signature, dataTypes) -> new BinaryScalar<>(
                        operator,
                        operatorName,
                        signature,
                        DataTypes.LONG,
                        FunctionInfo.DETERMINISTIC_ONLY)
                );
            }
            BinaryOperator<Double> doubleOperator;
            switch (operatorName) {
                case Names.ADD:
                    doubleOperator = Double::sum;
                    break;
                case Names.SUBTRACT:
                    doubleOperator = (x, y) -> x - y;
                    break;
                case Names.MULTIPLY:
                    doubleOperator = (x, y) -> x * y;
                    break;
                case Names.DIVIDE:
                    doubleOperator = (x, y) -> x / y;
                    break;
                case Names.MOD:
                case Names.MODULUS:
                    doubleOperator = (x, y) -> x % y;
                    break;

                default:
                    throw new IllegalStateException("Unexpected value: " + "");
            }
            module.register(
                Signature.scalar(
                    operatorName,
                    DataTypes.DOUBLE.getTypeSignature(),
                    DataTypes.DOUBLE.getTypeSignature(),
                    DataTypes.DOUBLE.getTypeSignature()
                ),
                (signature, dataTypes) -> new BinaryScalar<>(
                    doubleOperator,
                    operatorName,
                    signature,
                    DataTypes.DOUBLE,
                    FunctionInfo.DETERMINISTIC_ONLY)
            );
            BinaryOperator<Float> floatOperator;
            switch (operatorName) {
                case Names.ADD:
                    floatOperator = Float::sum;
                    break;
                case Names.SUBTRACT:
                    floatOperator = (x, y) -> x - y;
                    break;
                case Names.MULTIPLY:
                    floatOperator = (x, y) -> x * y;
                    break;
                case Names.DIVIDE:
                    floatOperator = (x, y) -> x / y;
                    break;
                case Names.MOD:
                case Names.MODULUS:
                    floatOperator = (x, y) -> x % y;
                    break;
                default:
                    throw new IllegalStateException("Unexpected value: " + "");
            }
            module.register(
                Signature.scalar(
                    operatorName,
                    DataTypes.FLOAT.getTypeSignature(),
                    DataTypes.FLOAT.getTypeSignature(),
                    DataTypes.FLOAT.getTypeSignature()
                ),
                (signature, dataTypes) -> new BinaryScalar<>(
                    floatOperator,
                    operatorName,
                    signature,
                    DataTypes.FLOAT,
                    FunctionInfo.DETERMINISTIC_ONLY)
            );
        }

        module.register(
            Signature.scalar(
                Names.POWER,
                DataTypes.DOUBLE.getTypeSignature(),
                DataTypes.DOUBLE.getTypeSignature(),
                DataTypes.DOUBLE.getTypeSignature()),
            (signature, dataTypes) -> new BinaryScalar<>(
                Math::pow,
                Names.POWER,
                signature,
                DataTypes.DOUBLE,
                FunctionInfo.DETERMINISTIC_ONLY)
        );
    }
}
