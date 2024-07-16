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

import java.util.function.DoubleUnaryOperator;

import io.crate.expression.scalar.BinaryScalar;
import io.crate.expression.scalar.DoubleScalar;
import io.crate.metadata.FunctionType;
import io.crate.metadata.Functions;
import io.crate.metadata.Scalar;
import io.crate.metadata.Scalar.Feature;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataTypes;
import io.crate.types.DoubleType;

public final class TrigonometricFunctions {

    public static void register(Functions.Builder builder) {
        register(builder, "sin", Math::sin);
        register(builder, "asin", x -> Math.asin(checkRange(x)));
        register(builder, "cos", Math::cos);
        register(builder, "acos", x -> Math.acos(checkRange(x)));
        register(builder, "tan", Math::tan);
        register(builder, "cot", x -> 1 / Math.tan(x));
        register(builder, "atan", x -> Math.atan(x));

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

    private static double checkRange(double value) {
        if (value < -1.0 || value > 1.0) {
            throw new IllegalArgumentException("input value " + value + " is out of range. " +
                "Values must be in range of [-1.0, 1.0]");
        }
        return value;
    }
}
