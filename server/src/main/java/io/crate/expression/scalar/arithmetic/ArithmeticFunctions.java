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
import io.crate.expression.scalar.BinaryScalar;
import io.crate.metadata.FunctionType;
import io.crate.metadata.Functions;
import io.crate.metadata.functions.Signature;
import io.crate.metadata.functions.Signature.Feature;
import io.crate.types.DataTypes;
import io.crate.types.NumberType;
import io.crate.types.TypeSignature;

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

    public static void register(Functions.Builder builder) {
        for (NumberType<?> type : DataTypes.NUMERIC_PRIMITIVE_TYPES) {
            TypeSignature typeSignature = type.getTypeSignature();
            builder.add(
                Signature.builder(Names.ADD, FunctionType.SCALAR)
                    .argumentTypes(typeSignature, typeSignature)
                    .returnType(typeSignature)
                    .features(Feature.DETERMINISTIC, Feature.COMPARISON_REPLACEMENT, Feature.STRICTNULL)
                    .build(),
                (signature, boundSignature) -> new BinaryScalar<>(type::addExact, signature, boundSignature)
            );
            builder.add(
                Signature.builder(Names.SUBTRACT, FunctionType.SCALAR)
                    .argumentTypes(typeSignature, typeSignature)
                    .returnType(typeSignature)
                    .features(Feature.DETERMINISTIC, Feature.STRICTNULL)
                    .build(),
                (signature, boundSignature) -> new BinaryScalar<>(type::subtractExact, signature, boundSignature)
            );
            builder.add(
                Signature.builder(Names.MULTIPLY, FunctionType.SCALAR)
                    .argumentTypes(typeSignature, typeSignature)
                    .returnType(typeSignature)
                    .features(Feature.DETERMINISTIC, Feature.STRICTNULL)
                    .build(),
                (signature, boundSignature) -> new BinaryScalar<>(type::multiplyExact, signature, boundSignature)
            );
            builder.add(
                Signature.builder(Names.DIVIDE, FunctionType.SCALAR)
                    .argumentTypes(typeSignature, typeSignature)
                    .returnType(typeSignature)
                    .features(Feature.DETERMINISTIC, Feature.STRICTNULL)
                    .build(),
                (signature, boundSignature) -> new BinaryScalar<>(type::divideExact, signature, boundSignature)
            );
            builder.add(
                Signature.builder(Names.MOD, FunctionType.SCALAR)
                    .argumentTypes(typeSignature, typeSignature)
                    .returnType(typeSignature)
                    .features(Feature.DETERMINISTIC, Feature.STRICTNULL)
                    .build(),
                (signature, boundSignature) -> new BinaryScalar<>(type::modulo, signature, boundSignature)
            );
            builder.add(
                Signature.builder(Names.MODULUS, FunctionType.SCALAR)
                    .argumentTypes(typeSignature, typeSignature)
                    .returnType(typeSignature)
                    .features(Feature.DETERMINISTIC, Feature.STRICTNULL)
                    .build(),
                (signature, boundSignature) -> new BinaryScalar<>(type::modulo, signature, boundSignature)
            );
        }

        builder.add(
            Signature.builder(Names.POWER, FunctionType.SCALAR)
                .argumentTypes(DataTypes.DOUBLE.getTypeSignature(), DataTypes.DOUBLE.getTypeSignature())
                .returnType(DataTypes.DOUBLE.getTypeSignature())
                .features(Feature.DETERMINISTIC, Feature.STRICTNULL)
                .build(),
            (signature, boundSignature) ->
                new BinaryScalar<>(Math::pow, signature, boundSignature)
        );
        builder.add(
            Signature.builder(Names.POWER, FunctionType.SCALAR)
                .argumentTypes(DataTypes.NUMERIC.getTypeSignature(), DataTypes.NUMERIC.getTypeSignature())
                .returnType(DataTypes.NUMERIC.getTypeSignature())
                .features(Feature.DETERMINISTIC, Feature.STRICTNULL)
                .build(),
            (signature, boundSignature) ->
                new BinaryScalar<>(
                    (BigDecimal arg1 , BigDecimal arg2) -> BigDecimalMath.pow(arg1, arg2, MathContext.DECIMAL128),
                    signature,
                    boundSignature)
        );
    }
}
