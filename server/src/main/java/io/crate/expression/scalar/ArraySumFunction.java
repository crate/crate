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

import io.crate.data.Input;
import io.crate.execution.engine.aggregation.impl.KahanSummationForDouble;
import io.crate.execution.engine.aggregation.impl.KahanSummationForFloat;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.Signature;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

import java.math.BigDecimal;
import java.util.List;
import java.util.function.Function;

import static io.crate.expression.scalar.array.ArrayArgumentValidators.ensureInnerTypeIsNotUndefined;

public class ArraySumFunction<T extends Number, R extends Number> extends Scalar<R, List<T>> {

    public static final String NAME = "array_sum";

    private final DataType<R> returnType;
    private final Function summationFunction;

    private final Function<List<BigDecimal>, BigDecimal> numericFunction = list -> {
        BigDecimal sum = BigDecimal.ZERO;
        boolean hasNotNull = false;
        for (int i = 0; i < list.size(); i++) {
            var item = list.get(i);
            if (item != null) {
                hasNotNull = true;
                sum = sum.add(item);
            }
        }
        return hasNotNull ? sum : null;
    };

    private final Function<List<Float>, Float> floatFunction = list -> {
        var kahanSummationForFloat = new KahanSummationForFloat();
        float sum = 0;
        boolean hasNotNull = false;
        for (int i = 0; i < list.size(); i++) {
            var item = list.get(i);
            if (item != null) {
                hasNotNull = true;
                sum = kahanSummationForFloat.sum(sum, item);
            }
        }
        return hasNotNull ? sum : null;
    };

    private final Function<List<Double>, Double> doubleFunction = list -> {
        var kahanSummationForDouble = new KahanSummationForDouble();
        double sum = 0;
        boolean hasNotNull = false;
        for (int i = 0; i < list.size(); i++) {
            var item = list.get(i);
            if (item != null) {
                hasNotNull = true;
                sum = kahanSummationForDouble.sum(sum, item);
            }
        }
        return hasNotNull ? sum : null;
    };


    private final Function<List<Number>, Long> longFunction = list -> {
        Long sum = 0L;
        boolean hasNotNull = false;
        for (int i = 0; i < list.size(); i++) {
            var item = list.get(i);
            if (item != null) {
                hasNotNull = true;
                sum = Math.addExact(sum, item.longValue());
            }
        }
        return hasNotNull ? sum : null;
    };

    public static void register(ScalarFunctionModule module) {

        module.register(
            Signature.scalar(
                NAME,
                new ArrayType(DataTypes.NUMERIC).getTypeSignature(),
                DataTypes.NUMERIC.getTypeSignature()
            ),
            ArraySumFunction::new
        );

        for (var supportedType : DataTypes.NUMERIC_PRIMITIVE_TYPES) {
            DataType inputDependantOutputType = DataTypes.LONG;
            if (supportedType == DataTypes.FLOAT || supportedType == DataTypes.DOUBLE) {
                inputDependantOutputType = supportedType;
            }

            module.register(
                Signature.scalar(
                    NAME,
                    new ArrayType(supportedType).getTypeSignature(),
                    inputDependantOutputType.getTypeSignature()
                ),
                ArraySumFunction::new
            );
        }
    }

    private final Signature signature;
    private final Signature boundSignature;

    private ArraySumFunction(Signature signature, Signature boundSignature) {
        this.signature = signature;
        this.boundSignature = boundSignature;
        returnType = (DataType<R>) signature.getReturnType().createType();

        if (returnType == DataTypes.FLOAT) {
            summationFunction = floatFunction;
        } else if (returnType == DataTypes.DOUBLE) {
            summationFunction = doubleFunction;
        } else if (returnType == DataTypes.NUMERIC) {
            summationFunction = numericFunction;
        } else {
            summationFunction = longFunction;
        }

        ensureInnerTypeIsNotUndefined(boundSignature.getArgumentDataTypes(), signature.getName().name());
    }

    @Override
    public Signature signature() {
        return signature;
    }

    @Override
    public Signature boundSignature() {
        return boundSignature;
    }

    @Override
    public R evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input[] args) {
        List<T> values = (List) args[0].value();
        if (values == null || values.isEmpty()) {
            return null;
        }
        return returnType.implicitCast(summationFunction.apply(values));
    }
}
