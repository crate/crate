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

import java.util.function.DoubleUnaryOperator;

import io.crate.expression.scalar.DoubleScalar;
import io.crate.expression.scalar.ScalarFunctionModule;
import io.crate.metadata.Scalar;
import io.crate.types.DataTypes;
import io.crate.types.DoubleType;

public final class TrigonometricFunctions {

    public static void register(ScalarFunctionModule module) {
        register(module, "sin", Math::sin);
        register(module, "asin", x -> Math.asin(checkRange(x)));
        register(module, "cos", Math::cos);
        register(module, "acos", x -> Math.acos(checkRange(x)));
        register(module, "tan", Math::tan);
        register(module, "cot", x -> 1 / Math.tan(x));
        register(module, "atan", x -> Math.atan(x));

        module.register(
            scalar(
                "atan2",
                DataTypes.DOUBLE.getTypeSignature(),
                DataTypes.DOUBLE.getTypeSignature(),
                DataTypes.DOUBLE.getTypeSignature())
                .withFeatures(Scalar.DETERMINISTIC_ONLY)
                .withFeature(Scalar.Feature.NULLABLE),
            (signature, boundSignature) ->
                new BinaryScalar<>(
                    Math::atan2,
                    signature,
                    boundSignature,
                    DoubleType.INSTANCE
                )
        );
    }

    private static void register(ScalarFunctionModule module, String name, DoubleUnaryOperator func) {
        module.register(
            scalar(
                name,
                DataTypes.DOUBLE.getTypeSignature(),
                DataTypes.DOUBLE.getTypeSignature()
            ).withFeature(Scalar.Feature.NULLABLE),
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
