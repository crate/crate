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

import io.crate.data.Input;
import io.crate.expression.scalar.ScalarFunctionModule;
import io.crate.expression.scalar.UnaryScalar;
import io.crate.metadata.Scalar;
import io.crate.metadata.NodeContext;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

import java.math.BigDecimal;
import java.math.RoundingMode;

import static io.crate.metadata.functions.Signature.scalar;

public final class RoundFunction {

    public static final String NAME = "round";

    public static void register(ScalarFunctionModule module) {
        for (var type : DataTypes.NUMERIC_PRIMITIVE_TYPES) {
            var typeSignature = type.getTypeSignature();
            DataType<?> returnType = DataTypes.getIntegralReturnType(type);
            assert returnType != null : "Could not get integral type of " + type;
            module.register(
                scalar(NAME, typeSignature, returnType.getTypeSignature())
                    .withFeature(Scalar.Feature.NULLABLE),
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
        module.register(
            scalar(
                NAME,
                DataTypes.DOUBLE.getTypeSignature(),
                DataTypes.INTEGER.getTypeSignature(),
                DataTypes.DOUBLE.getTypeSignature()
            ),
            RoundFunction::roundWithPrecision
        );
    }

    private static Scalar<Number, Number> roundWithPrecision(Signature signature, BoundSignature boundSignature) {
        return new Scalar<>(signature,boundSignature) {
            
            @Override
            public Number evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input<Number>... args) {
                Number n = args[0].value();
                Number nd = args[1].value();
                if (n == null || nd == null) {
                    return null;
                }
                double val = n.doubleValue();
                int numDecimals = nd.intValue();
                
                return BigDecimal.valueOf(val).setScale(numDecimals, RoundingMode.HALF_UP).doubleValue();
            }
        };
    }
}
