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

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.List;
import java.util.Objects;

import org.jetbrains.annotations.Nullable;

import io.crate.data.Input;
import io.crate.expression.scalar.array.ArraySummationFunctions;
import io.crate.metadata.Functions;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.Signature;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;

public class ArrayAvgFunction {

    public static final String NAME = "array_avg";

    @Nullable
    private static Float avgFloat(List<Float> values) {
        if (values == null || values.isEmpty()) {
            return null;
        }
        long size = values.stream().filter(Objects::nonNull).count();
        Float sum = ArraySummationFunctions.sumFloat(values);
        return sum == null ? null : sum / size;
    }

    @Nullable
    private static Double avgDouble(List<Double> values) {
        if (values == null || values.isEmpty()) {
            return null;
        }
        long size = values.stream().filter(Objects::nonNull).count();
        Double sum = ArraySummationFunctions.sumDouble(values);
        return sum == null ? null : sum / size;
    }


    @Nullable
    private static BigDecimal avgBigDecimal(List<BigDecimal> values) {
        if (values == null || values.isEmpty()) {
            return null;
        }
        long size = values.stream().filter(Objects::nonNull).count();
        BigDecimal sum = ArraySummationFunctions.sumBigDecimal(values);
        return sum == null ? null : sum.divide(BigDecimal.valueOf(size), MathContext.DECIMAL128);
    }

    @Nullable
    private static Number avgNumber(List<? extends Number> values) {
        if (values == null || values.isEmpty()) {
            return null;
        }
        long size = values.stream().filter(Objects::nonNull).count();
        BigDecimal sum = ArraySummationFunctions.sumNumberWithOverflow(values);
        return sum == null ? null : sum.divide(BigDecimal.valueOf(size), MathContext.DECIMAL128);
    }

    public static void register(Functions.Builder builder) {

        // All types except float and double have numeric average
        // https://www.postgresql.org/docs/13/functions-aggregate.html

        builder.add(
            Signature.scalar(
                    NAME,
                    new ArrayType<>(DataTypes.NUMERIC).getTypeSignature(),
                    DataTypes.NUMERIC.getTypeSignature()
                ).withFeature(Scalar.Feature.DETERMINISTIC),
            (signature, boundSignature) -> new Scalar<>(signature, boundSignature) {
                @Override
                public Object evaluate(TransactionContext txnCtx, NodeContext nodeContext, Input<Object>... args) {
                    return avgBigDecimal(new ArrayType<>(DataTypes.NUMERIC).sanitizeValue(args[0].value()));
                }
            }
        );

        builder.add(
            Signature.scalar(
                    NAME,
                    new ArrayType<>(DataTypes.FLOAT).getTypeSignature(),
                    DataTypes.FLOAT.getTypeSignature()
                ).withFeature(Scalar.Feature.DETERMINISTIC),
            (signature, boundSignature) -> new Scalar<>(signature, boundSignature) {
                @SafeVarargs
                @Override
                public final Object evaluate(TransactionContext txnCtx, NodeContext nodeContext, Input<Object>... args) {
                    return avgFloat(new ArrayType<>(DataTypes.FLOAT).sanitizeValue(args[0].value()));
                }
            }
        );

        builder.add(
            Signature.scalar(
                    NAME,
                    new ArrayType<>(DataTypes.DOUBLE).getTypeSignature(),
                    DataTypes.DOUBLE.getTypeSignature()
                ).withFeature(Scalar.Feature.DETERMINISTIC),
            (signature, boundSignature) -> new Scalar<>(signature, boundSignature) {
                @SafeVarargs
                @Override
                public final Object evaluate(TransactionContext txnCtx, NodeContext nodeContext, Input<Object>... args) {
                    return avgDouble(new ArrayType<>(DataTypes.DOUBLE).sanitizeValue(args[0].value()));
                }
            }
        );


        for (var supportedType : DataTypes.NUMERIC_PRIMITIVE_TYPES) {
            if (supportedType != DataTypes.FLOAT && supportedType != DataTypes.DOUBLE) {
                builder.add(
                    Signature.scalar(
                            NAME,
                            new ArrayType<>(supportedType).getTypeSignature(),
                            DataTypes.NUMERIC.getTypeSignature()
                        ).withFeature(Scalar.Feature.DETERMINISTIC),
                    (signature, boundSignature) -> new Scalar<>(signature, boundSignature) {
                        @SafeVarargs
                        @Override
                        public final Object evaluate(TransactionContext txnCtx, NodeContext nodeContext, Input<Object>... args) {
                            return avgNumber(new ArrayType<>(supportedType).sanitizeValue(args[0].value()));
                        }
                    }
                );
            }
        }
    }
}
