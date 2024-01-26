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
import io.crate.metadata.NodeContext;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

public class ArraySumFunction<T extends Number, R extends Number> extends Scalar<R, List<T>> {

    public static final String NAME = "array_sum";

    private final DataType<R> returnType;
    private final Function<List<T>, R> summationFunction;

    public static void register(ScalarFunctionModule module) {

        module.register(
            Signature.scalar(
                    NAME,
                    new ArrayType(DataTypes.NUMERIC).getTypeSignature(),
                    DataTypes.NUMERIC.getTypeSignature()
                ).withFeature(Feature.NULLABLE),
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
                    ).withFeature(Feature.NULLABLE),
                ArraySumFunction::new
            );
        }
    }

    private ArraySumFunction(Signature signature, BoundSignature boundSignature) {
        super(signature, boundSignature);
        returnType = (DataType<R>) signature.getReturnType().createType();

        if (returnType == DataTypes.FLOAT) {
            summationFunction = ArraySummationFunctions.FLOAT.getFunction();
        } else if (returnType == DataTypes.DOUBLE) {
            summationFunction = ArraySummationFunctions.DOUBLE.getFunction();
        } else if (returnType == DataTypes.NUMERIC) {
            summationFunction = ArraySummationFunctions.NUMERIC.getFunction();
        } else {
            summationFunction = ArraySummationFunctions.PRIMITIVE_NON_FLOAT_OVERFLOWING.getFunction();
        }

        ensureInnerTypeIsNotUndefined(boundSignature.argTypes(), signature.getName().name());
    }

    @Override
    public R evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input[] args) {
        List<T> values = (List) args[0].value();
        if (values == null || values.isEmpty()) {
            return null;
        }
        return summationFunction.apply(values);
    }
}
