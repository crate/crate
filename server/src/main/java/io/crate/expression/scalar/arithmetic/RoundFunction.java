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
import java.math.RoundingMode;

import io.crate.common.annotations.VisibleForTesting;
import io.crate.data.Input;
import io.crate.expression.scalar.UnaryScalar;
import io.crate.metadata.FunctionType;
import io.crate.metadata.Functions;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

public final class RoundFunction {

    public static final String NAME = "round";

    /**
     * The maximum supported `precision` argument of `round(x, precision)`. Matches the maximum
     * scale of PostgreSQL's `numeric` type. Without an upper bound, `setScale()` has to materialize
     * an unscaled value with `precision` digits, which runs out of memory or seems hang.
     */
    @VisibleForTesting
    static final int MAX_PRECISION = 16383;

    private RoundFunction() {}

    public static void register(Functions.Builder builder) {
        for (var type : DataTypes.NUMERIC_PRIMITIVE_TYPES) {
            var typeSignature = type.getTypeSignature();
            DataType<?> returnType = DataTypes.getIntegralReturnType(type);
            assert returnType != null : "Could not get integral type of " + type;
            builder.add(
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
                            x -> Math.round(x.floatValue())
                        );
                    } else {
                        return new UnaryScalar<>(
                            signature,
                            boundSignature,
                            type,
                            x -> Math.round(x.doubleValue())
                        );
                    }
                }
            );
        }

        builder.add(
            Signature.builder(NAME, FunctionType.SCALAR)
                .argumentTypes(DataTypes.NUMERIC.getTypeSignature())
                .returnType(DataTypes.NUMERIC.getTypeSignature())
                .features(Scalar.Feature.DETERMINISTIC, Scalar.Feature.STRICTNULL)
                .build(),
            (signature, boundSignature) -> new UnaryScalar<>(
                signature,
                boundSignature,
                DataTypes.NUMERIC,
                x -> x.setScale(0, RoundingMode.HALF_UP)
            )
        );

        builder.add(
            Signature.builder(NAME, FunctionType.SCALAR)
                .argumentTypes(DataTypes.NUMERIC.getTypeSignature(),
                    DataTypes.INTEGER.getTypeSignature())
                .returnType(DataTypes.NUMERIC.getTypeSignature())
                .features(Scalar.Feature.DETERMINISTIC, Scalar.Feature.STRICTNULL)
                .build(),
            RoundFunction::roundWithPrecision
        );
    }

    private static Scalar<Number, Number> roundWithPrecision(Signature signature, BoundSignature boundSignature) {
        return new Scalar<>(signature, boundSignature) {

            @Override
            public BigDecimal evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input<Number>... args) {
                Number n = args[0].value();
                Number nd = args[1].value();
                if (n == null || nd == null) {
                    return null;
                }

                int numDecimals = nd.intValue();
                BigDecimal val;

                if (n instanceof BigDecimal) {
                    val = (BigDecimal) n;
                } else if (n instanceof Long) {
                    val = BigDecimal.valueOf(n.longValue());
                } else {
                    val = BigDecimal.valueOf(n.doubleValue());
                }

                if (numDecimals < 0) {
                    // Rounding to a power of ten which is larger than the value itself always
                    // results in 0. Short-circuit those cases, otherwise `movePointRight()` creates
                    // a BigDecimal with a scale of `numDecimals`, and rounding it requires to
                    // materialize an unscaled value with that many digits, which increases execution
                    // time as `numDecimals` grows.
                    if (-(long) numDecimals > val.precision() - val.scale()) {
                        return BigDecimal.ZERO;
                    }
                    return val.movePointRight(numDecimals).setScale(0, RoundingMode.HALF_UP).movePointLeft(numDecimals);
                }
                if (numDecimals > MAX_PRECISION) {
                    return val.setScale(MAX_PRECISION, RoundingMode.HALF_UP);
                }
                return val.setScale(numDecimals, RoundingMode.HALF_UP);
            }
        };
    }
}
