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
import io.crate.metadata.NodeContext;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.Signature;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

import java.util.List;

import static io.crate.expression.scalar.array.ArrayArgumentValidators.ensureInnerTypeIsNotUndefined;

public class ArrayMinFunction<T> extends Scalar<T, List<T>> {

    public static final String NAME = "array_min";

    private final DataType dataType;

    public static void register(ScalarFunctionModule module) {

        module.register(
            Signature.scalar(
                NAME,
                new ArrayType(DataTypes.NUMERIC).getTypeSignature(),
                DataTypes.NUMERIC.getTypeSignature()
            ),
            ArrayMinFunction::new
        );

        for (var supportedType : DataTypes.PRIMITIVE_TYPES) {
            module.register(
                Signature.scalar(
                    NAME,
                    new ArrayType(supportedType).getTypeSignature(),
                    supportedType.getTypeSignature()
                ),
                ArrayMinFunction::new
            );
        }
    }

    private final Signature signature;
    private final Signature boundSignature;

    private ArrayMinFunction(Signature signature, Signature boundSignature) {
        this.signature = signature;
        this.boundSignature = boundSignature;
        this.dataType = signature.getReturnType().createType();
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
    public T evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input[] args) {
        List<T> values = (List) args[0].value();
        if (values == null || values.isEmpty()) {
            return null;
        }

        // Taking first element in order not to initialize min
        // with type dependant TYPE.MAX_VALUE.
        T min = values.get(0);

        for (int i = 1; i < values.size(); i++) {
            T item = values.get(i);
            if (item != null) {
                //min can be null on the first iteration.
                if (min == null) {
                    min = item;
                } else if (dataType.compare(item, min) < 0) {
                    min = item;
                }
            }
        }
        return min;
    }
}
