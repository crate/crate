/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import io.crate.expression.scalar.DoubleScalar;
import io.crate.expression.scalar.UnaryScalar;
import io.crate.metadata.FunctionType;
import io.crate.metadata.Functions;
import io.crate.metadata.Scalar;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

public class SignFunction {

    public static final String NAME = "sign";

    public static void register(Functions.Builder builder) {
        builder.add(
            Signature.builder(NAME, FunctionType.SCALAR)
                .argumentTypes(DataTypes.NUMERIC.getTypeSignature())
                .returnType(DataTypes.NUMERIC.getTypeSignature())
                .features(Scalar.Feature.DETERMINISTIC, Scalar.Feature.NULLABLE)
                .build(),
            (signature, boundSignature) -> {
                DataType<?> argType = boundSignature.argTypes().getFirst();
                return new UnaryScalar<>(
                    signature,
                    boundSignature,
                    argType,
                    x -> BigDecimal.valueOf(Math.signum(((Number) argType.sanitizeValue(x)).longValue()))
                );
            }
        );


        builder.add(
            Signature.builder(NAME, FunctionType.SCALAR)
                .argumentTypes(DataTypes.DOUBLE.getTypeSignature())
                .returnType(DataTypes.DOUBLE.getTypeSignature())
                .features(Scalar.Feature.DETERMINISTIC, Scalar.Feature.NULLABLE)
                .build(),
            (signature, boundSignature) ->
                new DoubleScalar(signature, boundSignature, Math::signum)
        );
    }
}
