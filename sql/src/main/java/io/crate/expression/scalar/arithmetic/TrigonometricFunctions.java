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

import io.crate.expression.scalar.DoubleScalar;
import io.crate.expression.scalar.ScalarFunctionModule;
import io.crate.metadata.FunctionInfo;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.DoubleType;

import java.util.function.DoubleUnaryOperator;

import static io.crate.metadata.functions.Signature.scalar;
import static io.crate.types.DataTypes.NUMERIC_PRIMITIVE_TYPES;

public final class TrigonometricFunctions {

    public static void register(ScalarFunctionModule module) {
        register(module, "sin", Math::sin);
        register(module, "asin", x -> Math.asin(checkRange(x)));
        register(module, "cos", Math::cos);
        register(module, "acos", x -> Math.acos(checkRange(x)));
        register(module, "tan", Math::tan);
        register(module, "cot", x -> 1 / Math.tan(x));
        register(module, "atan", x -> Math.atan(checkRange(x)));

        for (DataType<?> inputType : NUMERIC_PRIMITIVE_TYPES) {
            module.register(
                scalar(
                    "atan2",
                    inputType.getTypeSignature(),
                    inputType.getTypeSignature(),
                    DataTypes.DOUBLE.getTypeSignature()
                ),
                (signature, argumentTypes) ->
                    new BinaryScalar<>(
                        Math::atan2,
                        "atan2",
                        signature,
                        DoubleType.INSTANCE,
                        FunctionInfo.DETERMINISTIC_ONLY
                    )
            );
        }
    }

    private static void register(ScalarFunctionModule module, String name, DoubleUnaryOperator func) {
        for (DataType<?> inputType : NUMERIC_PRIMITIVE_TYPES) {
            module.register(
                scalar(
                    name,
                    inputType.getTypeSignature(),
                    DataTypes.DOUBLE.getTypeSignature()
                ),
                (signature, argumentTypes) ->
                    new DoubleScalar(name, signature, inputType, func)
            );
        }
    }

    private static double checkRange(double value) {
        if (value < -1.0 || value > 1.0) {
            throw new IllegalArgumentException("input value " + value + " is out of range. " +
                                               "Values must be in range of [-1.0, 1.0]");
        }
        return value;
    }
}
