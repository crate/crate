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

package io.crate.expression.scalar;

import io.crate.expression.scalar.array.ArraySummationFunctions;
import io.crate.metadata.Scalar;
import io.crate.metadata.functions.Signature;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

public class ArrayAvgFunction {

    public static final String NAME = "array_avg";

    private enum Operations {
        FLOAT(
            (Function<List<Float>, Float>) list -> {
                if (list == null || list.isEmpty()) {
                    return null;
                }
                long size = list.stream().filter(Objects::nonNull).count();
                Float sum = (Float) ArraySummationFunctions.FLOAT.getFunction().apply(list);
                return sum == null ? null : sum / size;
            }
        ),
        DOUBLE(
            (Function<List<Double>, Double>) list -> {
                if (list == null || list.isEmpty()) {
                    return null;
                }
                long size = list.stream().filter(Objects::nonNull).count();
                Double sum = (Double) ArraySummationFunctions.DOUBLE.getFunction().apply(list);
                return sum == null ? null : sum / size;
            }
        ),
        NUMERIC(
            (Function<List<BigDecimal>, BigDecimal>) list -> {
                if (list == null || list.isEmpty()) {
                    return null;
                }
                long size = list.stream().filter(Objects::nonNull).count();
                BigDecimal sum = (BigDecimal) ArraySummationFunctions.NUMERIC.getFunction().apply(list);
                return sum == null ? null : sum.divide(BigDecimal.valueOf(size), MathContext.DECIMAL128);
            }
        ),
        INTEGRAL_PRIMITIVE(
            (Function<List<Number>, Number>) list -> {
                if (list == null || list.isEmpty()) {
                    return null;
                }
                long size = list.stream().filter(Objects::nonNull).count();
                BigDecimal sum = (BigDecimal) ArraySummationFunctions.PRIMITIVE_NON_FLOAT_NOT_OVERFLOWING.getFunction().apply(list);
                return sum == null ? null : sum.divide(BigDecimal.valueOf(size), MathContext.DECIMAL128);
            }
        );

        private final Function function;

        Operations(Function function) {
            this.function = function;
        }

        public Function getFunction() {
            return function;
        }
    }

    public static void register(ScalarFunctionModule module) {

        // All types except float and double have numeric average
        // https://www.postgresql.org/docs/13/functions-aggregate.html

        module.register(
            Signature.scalar(
                NAME,
                new ArrayType(DataTypes.NUMERIC).getTypeSignature(),
                DataTypes.NUMERIC.getTypeSignature()
            ).withFeature(Scalar.Feature.NULLABLE),
            (signature, boundSignature) -> new UnaryScalar<>(
                signature,
                boundSignature,
                new ArrayType(DataTypes.NUMERIC),
                Operations.NUMERIC.getFunction())
        );

        module.register(
            Signature.scalar(
                NAME,
                new ArrayType(DataTypes.FLOAT).getTypeSignature(),
                DataTypes.FLOAT.getTypeSignature()
            ).withFeature(Scalar.Feature.NULLABLE),
            (signature, boundSignature) -> new UnaryScalar<>(
                signature,
                boundSignature,
                new ArrayType(DataTypes.FLOAT),
                Operations.FLOAT.getFunction())
        );

        module.register(
            Signature.scalar(
                NAME,
                new ArrayType(DataTypes.DOUBLE).getTypeSignature(),
                DataTypes.DOUBLE.getTypeSignature()
            ).withFeature(Scalar.Feature.NULLABLE),
            (signature, boundSignature) -> new UnaryScalar<>(
                signature,
                boundSignature,
                new ArrayType(DataTypes.DOUBLE),
                Operations.DOUBLE.getFunction())
        );


        for (var supportedType : DataTypes.NUMERIC_PRIMITIVE_TYPES) {
            if (supportedType != DataTypes.FLOAT && supportedType != DataTypes.DOUBLE) {
                module.register(
                    Signature.scalar(
                        NAME,
                        new ArrayType(supportedType).getTypeSignature(),
                        DataTypes.NUMERIC.getTypeSignature()
                    ).withFeature(Scalar.Feature.NULLABLE),
                    (signature, boundSignature) -> new UnaryScalar<>(
                        signature,
                        boundSignature,
                        new ArrayType(supportedType),
                        Operations.INTEGRAL_PRIMITIVE.getFunction())
                );
            }
        }
    }
}
