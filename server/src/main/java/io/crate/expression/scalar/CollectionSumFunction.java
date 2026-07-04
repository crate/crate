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

package io.crate.expression.scalar;

import java.util.List;

import io.crate.data.Input;
import io.crate.execution.engine.aggregation.impl.util.KahanSummationForDouble;
import io.crate.execution.engine.aggregation.impl.util.KahanSummationForFloat;
import io.crate.metadata.FunctionType;
import io.crate.metadata.Functions;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

public class CollectionSumFunction extends Scalar<Number, List<Object>> {

    public static final String NAME = "collection_sum";

    public static void register(Functions.Builder builder) {
        for (DataType<?> supportedType : List.of(DataTypes.BYTE, DataTypes.SHORT, DataTypes.INTEGER, DataTypes.LONG)) {
            builder.add(
                Signature.builder(NAME, FunctionType.SCALAR)
                    .argumentTypes(new ArrayType<>(supportedType).getTypeSignature())
                    .returnType(DataTypes.LONG.getTypeSignature())
                    .features(Feature.DETERMINISTIC, Feature.STRICTNULL)
                    .build(),
                CollectionSumFunction::new
            );
        }
        builder.add(
            Signature.builder(NAME, FunctionType.SCALAR)
                .argumentTypes(new ArrayType<>(DataTypes.FLOAT).getTypeSignature())
                .returnType(DataTypes.FLOAT.getTypeSignature())
                .features(Feature.DETERMINISTIC, Feature.STRICTNULL)
                .build(),
            CollectionSumFunction::new
        );
        builder.add(
            Signature.builder(NAME, FunctionType.SCALAR)
                .argumentTypes(new ArrayType<>(DataTypes.DOUBLE).getTypeSignature())
                .returnType(DataTypes.DOUBLE.getTypeSignature())
                .features(Feature.DETERMINISTIC, Feature.STRICTNULL)
                .build(),
            CollectionSumFunction::new
        );
    }

    private CollectionSumFunction(Signature signature, BoundSignature boundSignature) {
        super(signature, boundSignature);
    }

    @Override
    @SafeVarargs
    public final Number evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input<List<Object>>... args) {
        List<Object> argArray = args[0].value();
        if (argArray == null) {
            return null;
        }

        DataType<?> returnType = boundSignature().returnType();

        if (returnType == DataTypes.LONG) {
            long sum = 0L;
            boolean hasValues = false;
            for (Object value : argArray) {
                if (value != null) {
                    sum = Math.addExact(sum, ((Number) value).longValue());
                    hasValues = true;
                }
            }
            return hasValues ? sum : null;
        } else if (returnType == DataTypes.FLOAT) {
            float sum = 0f;
            KahanSummationForFloat kahanSummation = new KahanSummationForFloat();
            boolean hasValues = false;
            for (Object value : argArray) {
                if (value != null) {
                    sum = kahanSummation.sum(sum, ((Number) value).floatValue());
                    hasValues = true;
                }
            }
            return hasValues ? sum : null;
        } else if (returnType == DataTypes.DOUBLE) {
            double sum = 0d;
            KahanSummationForDouble kahanSummation = new KahanSummationForDouble();
            boolean hasValues = false;
            for (Object value : argArray) {
                if (value != null) {
                    sum = kahanSummation.sum(sum, ((Number) value).doubleValue());
                    hasValues = true;
                }
            }
            return hasValues ? sum : null;
        }
        return null;
    }
}
