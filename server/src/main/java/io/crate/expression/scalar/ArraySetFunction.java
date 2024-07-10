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

import static io.crate.metadata.functions.TypeVariableConstraint.typeVariable;

import java.util.ArrayList;
import java.util.List;

import io.crate.data.Input;
import io.crate.metadata.FunctionType;
import io.crate.metadata.Functions;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;
import io.crate.types.TypeSignature;

public class ArraySetFunction extends Scalar<List<Object>, Object> {

    public static final String NAME = "array_set";

    public static void register(Functions.Builder module) {
        TypeSignature arrayESignature = TypeSignature.parse("array(E)");
        module.add(
                Signature.builder(NAME, FunctionType.SCALAR)
                        .argumentTypes(arrayESignature,
                                new ArrayType<>(DataTypes.INTEGER).getTypeSignature(),
                                arrayESignature)
                        .returnType(arrayESignature)
                        .typeVariableConstraints(typeVariable("E"))
                        .features(Feature.DETERMINISTIC, Feature.STRICTNULL)
                        .build(),
                ArraySetFunction::new
        );
        module.add(
                Signature.builder(NAME, FunctionType.SCALAR)
                        .argumentTypes(arrayESignature,
                                DataTypes.INTEGER.getTypeSignature(),
                                TypeSignature.parse("E"))
                        .returnType(arrayESignature)
                        .typeVariableConstraints(typeVariable("E"))
                        .features(Feature.DETERMINISTIC, Feature.STRICTNULL)
                        .build(),
                SingleArraySetFunction::new
        );
    }

    public ArraySetFunction(Signature signature, BoundSignature boundSignature) {
        super(signature, boundSignature);
    }

    @Override
    @SafeVarargs
    @SuppressWarnings("unchecked")
    public final List<Object> evaluate(TransactionContext txnCtx, NodeContext nodeContext, Input<Object>... args) {
        List<Object> inputArray = (List<Object>) args[0].value();
        if (inputArray == null) {
            return null;
        }
        List<Integer> indexesToUpdate = (List<Integer>) args[1].value();
        List<Object> targetValues = (List<Object>) args[2].value();
        if (indexesToUpdate == null && targetValues == null) {
            return inputArray;
        }

        List<Object> updated = new ArrayList<>(inputArray);

        if (indexesToUpdate == null || targetValues == null || indexesToUpdate.size() != targetValues.size()) {
            throw new IllegalArgumentException(
                "`array_set(array, indexes, values)`: the size of indexes and values must match or both be nulls");
        }

        for (int i = 0; i < indexesToUpdate.size(); i++) {
            setElement(updated, indexesToUpdate.get(i), targetValues.get(i));
        }
        return updated;
    }

    private static void setElement(List<Object> source, int index, Object value) {
        if (index <= 0) {
            throw new UnsupportedOperationException("Updating arrays with indexes <= 0 is not supported");
        }
        index--; // Since array indexes in CrateDB start from 1
        if (index < source.size()) {
            source.set(index, value);
        } else {
            for (int i = source.size(); i < index; i++) {
                source.add(null);
            }
            source.add(value);
        }
    }

    static class SingleArraySetFunction extends Scalar<List<Object>, Object> {

        SingleArraySetFunction(Signature signature, BoundSignature boundSignature) {
            super(signature, boundSignature);
        }

        @SafeVarargs
        @Override
        @SuppressWarnings("unchecked")
        public final List<Object> evaluate(TransactionContext txnCtx, NodeContext nodeContext, Input<Object>... args) {
            List<Object> array = (List<Object>) args[0].value();
            if (array == null) {
                return null;
            }
            Integer index = (Integer) args[1].value();
            if (index == null) {
                return null;
            }
            Object newValue = args[2].value();
            setElement(array, index, newValue);
            return array;
        }
    }
}
