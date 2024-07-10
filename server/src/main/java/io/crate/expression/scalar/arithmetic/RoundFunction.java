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

import io.crate.expression.scalar.UnaryScalar;
import io.crate.metadata.FunctionType;
import io.crate.metadata.Functions;
import io.crate.metadata.Scalar;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

public final class RoundFunction {

    public static final String NAME = "round";

    public static void register(Functions.Builder module) {
        for (var type : DataTypes.NUMERIC_PRIMITIVE_TYPES) {
            var typeSignature = type.getTypeSignature();
            DataType<?> returnType = DataTypes.getIntegralReturnType(type);
            assert returnType != null : "Could not get integral type of " + type;
            module.add(
                Signature.builder(NAME, FunctionType.SCALAR)
                    .argumentTypes(typeSignature)
                    .returnType(returnType.getTypeSignature())
                    .features(Scalar.Feature.DETERMINISTIC, Scalar.Feature.STRICTNULL)
                    .build(),
                (signature, boundSignature) -> {
                    if (returnType.equals(DataTypes.INTEGER)) {
                        return new UnaryScalar<>(
                            signature,
                            boundSignature,
                            type,
                            x -> Math.round(((Number) x).floatValue())
                        );
                    } else {
                        return new UnaryScalar<>(
                            signature,
                            boundSignature,
                            type,
                            x -> Math.round(((Number) x).doubleValue())
                        );
                    }
                }
            );
        }
    }
}
