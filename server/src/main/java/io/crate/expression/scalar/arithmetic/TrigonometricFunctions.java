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
import java.util.function.DoubleUnaryOperator;
import java.util.function.UnaryOperator;

import ch.obermuhlner.math.big.BigDecimalMath;
import io.crate.expression.scalar.BinaryScalar;
import io.crate.expression.scalar.DoubleScalar;
import io.crate.expression.scalar.UnaryScalar;
import io.crate.metadata.FunctionType;
import io.crate.metadata.Functions;
import io.crate.metadata.Scalar;
import io.crate.metadata.Scalar.Feature;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataTypes;
import io.crate.types.DoubleType;

public final class TrigonometricFunctions {

    private TrigonometricFunctions() {}

    public static void register(Functions.Builder builder) {
        register(builder, "sin", Math::sin);
        registerNumeric(builder, "sin", x -> BigDecimalMath.sin(x, MathContext.DECIMAL128));

        register(builder, "asin", x -> Math.asin(checkRange(x)));
        registerNumeric(builder, "asin", x -> BigDecimalMath.asin(checkRange(x), MathContext.DECIMAL128));

        register(builder, "cos", Math::cos);
        registerNumeric(builder, "cos", x -> BigDecimalMath.sin(x, MathContext.DECIMAL128));

        register(builder, "acos", x -> Math.acos(checkRange(x)));
        registerNumeric(builder, "acos", x -> BigDecimalMath.acos(checkRange(x), MathContext.DECIMAL128));

        register(builder, "tan", Math::tan);
        registerNumeric(builder, "tan", x -> BigDecimalMath.tan(x, MathContext.DECIMAL128));

        register(builder, "cot", x -> 1 / Math.tan(x));
        registerNumeric(builder, "cot", x -> BigDecimalMath.cot(x, MathContext.DECIMAL128));

        register(builder, "atan", Math::atan);
        registerNumeric(builder, "atan", x -> BigDecimalMath.atan(x, MathContext.DECIMAL128));

        builder.add(
            Signature.builder("atan2", FunctionType.SCALAR)
                .argumentTypes(DataTypes.DOUBLE.getTypeSignature(),
                    DataTypes.DOUBLE.getTypeSignature())
                .returnType(DataTypes.DOUBLE.getTypeSignature())
                .features(Feature.DETERMINISTIC, Scalar.Feature.STRICTNULL)
                .build(),
            (signature, boundSignature) ->
                new BinaryScalar<>(
                    Math::atan2,
                    signature,
                    boundSignature,
                    DoubleType.INSTANCE
                )
        );

        builder.add(
            Signature.builder("atan2", FunctionType.SCALAR)
                .argumentTypes(DataTypes.NUMERIC.getTypeSignature(),
                    DataTypes.NUMERIC.getTypeSignature())
                .returnType(DataTypes.NUMERIC.getTypeSignature())
                .features(Feature.DETERMINISTIC, Scalar.Feature.STRICTNULL)
                .build(),
            (signature, ignoredBoundSignature) ->
                new BinaryScalar<>(
                    (y, x) -> BigDecimalMath.atan2(y, x, MathContext.DECIMAL128),
                    signature,
                    BoundSignature.sameAsUnbound(signature),
                    DataTypes.NUMERIC
            )
        );
    }

    private static void register(Functions.Builder builder, String name, DoubleUnaryOperator func) {
        builder.add(
            Signature.builder(name, FunctionType.SCALAR)
                .argumentTypes(DataTypes.DOUBLE.getTypeSignature())
                .returnType(DataTypes.DOUBLE.getTypeSignature())
                .features(Scalar.Feature.DETERMINISTIC, Scalar.Feature.STRICTNULL)
                .build(),
            (signature, boundSignature) ->
                new DoubleScalar(signature, boundSignature, func)
        );
    }

    private static void registerNumeric(Functions.Builder builder, String name, UnaryOperator<BigDecimal> func) {
        builder.add(
            Signature.builder(name, FunctionType.SCALAR)
                .argumentTypes(DataTypes.NUMERIC.getTypeSignature())
                .returnType(DataTypes.NUMERIC.getTypeSignature())
                .features(Scalar.Feature.DETERMINISTIC, Scalar.Feature.STRICTNULL)
                .build(),
            (signature, ignoredBoundSignature) ->
                new UnaryScalar<>(
                    signature,
                    BoundSignature.sameAsUnbound(signature),
                    DataTypes.NUMERIC,
                    func
                )
        );
    }

    private static double checkRange(double value) {
        if (value < -1.0 || value > 1.0) {
            throw new IllegalArgumentException("input value " + value + " is out of range. " +
                "Values must be in range of [-1.0, 1.0]");
        }
        return value;
    }

    private static BigDecimal checkRange(BigDecimal value) {
        if (value.compareTo(BigDecimal.ONE) > 0 || value.compareTo(MINUS_ONE) < 0) {
            throw new IllegalArgumentException("input value " + value + " is out of range. " +
                "Values must be in range of [-1.0, 1.0]");
        }
        return value;
    }

    private static final BigDecimal MINUS_ONE = BigDecimal.valueOf(-1L);
}
