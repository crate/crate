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

import static io.crate.expression.scalar.array.ArrayArgumentValidators.ensureInnerTypeIsNotUndefined;

import java.util.List;
import java.util.function.Function;

import io.crate.data.Input;
import io.crate.expression.scalar.array.ArraySummationFunctions;
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

public class ArraySumFunction<T extends Number, R extends Number> extends Scalar<R, List<T>> {

    public static final String NAME = "array_sum";

    private final Function<List<T>, R> sum;

    private static <T, U> Signature signature(DataType<T> elementType, DataType<U> returnType) {
        return Signature.builder(NAME, FunctionType.SCALAR)
            .argumentTypes(new ArrayType<>(elementType).getTypeSignature())
            .returnType(returnType.getTypeSignature())
            .features(Feature.DETERMINISTIC, Feature.NULLABLE)
            .build();
    }

    public static void register(Functions.Builder builder) {
        builder.add(
            signature(DataTypes.NUMERIC, DataTypes.NUMERIC),
            (sig, boundSig) -> new ArraySumFunction<>(sig, boundSig, ArraySummationFunctions::sumBigDecimal)
        );
        builder.add(
            signature(DataTypes.DOUBLE, DataTypes.DOUBLE),
            (sig, boundSig) -> new ArraySumFunction<>(sig, boundSig, ArraySummationFunctions::sumDouble)
        );
        builder.add(
            signature(DataTypes.FLOAT, DataTypes.FLOAT),
            (sig, boundSig) -> new ArraySumFunction<>(sig, boundSig, ArraySummationFunctions::sumFloat)
        );

        List<DataType<? extends Number>> integralTypes = List.of(
            DataTypes.BYTE,
            DataTypes.SHORT,
            DataTypes.INTEGER,
            DataTypes.LONG
        );
        for (DataType<? extends Number> integralType : integralTypes) {
            builder.add(
                signature(integralType, DataTypes.LONG),
                (sig, boundSig) -> new ArraySumFunction<>(sig, boundSig, ArraySummationFunctions::sumNumber)
            );
        }
    }

    private ArraySumFunction(Signature signature,
                             BoundSignature boundSignature,
                             Function<List<T>, R> sum) {
        super(signature, boundSignature);
        this.sum = sum;
        ensureInnerTypeIsNotUndefined(boundSignature.argTypes(), signature.getName().name());
    }

    @Override
    @SafeVarargs
    public final R evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input<List<T>> ... args) {
        List<T> values = args[0].value();
        if (values == null || values.isEmpty()) {
            return null;
        }
        return sum.apply(values);
    }
}
