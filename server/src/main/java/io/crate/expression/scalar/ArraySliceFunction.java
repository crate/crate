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

package io.crate.expression.scalar;

import static io.crate.expression.scalar.array.ArrayArgumentValidators.ensureInnerTypeIsNotUndefined;
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
import io.crate.types.DataTypes;
import io.crate.types.TypeSignature;

public class ArraySliceFunction extends Scalar<List<Object>, Object> {

    public static final String NAME = "array_slice";

    public static void register(Functions.Builder module) {
        module.add(
                Signature.builder(NAME, FunctionType.SCALAR)
                        .argumentTypes(TypeSignature.parse("array(E)"),
                                DataTypes.INTEGER.getTypeSignature(),
                                DataTypes.INTEGER.getTypeSignature())
                        .returnType(TypeSignature.parse("array(E)"))
                        .typeVariableConstraints(typeVariable("E"))
                        .features(Feature.DETERMINISTIC, Feature.NOTNULL)
                        .build(),
                ArraySliceFunction::new
        );
    }

    private ArraySliceFunction(Signature signature, BoundSignature boundSignature) {
        super(signature, boundSignature);
        ensureInnerTypeIsNotUndefined(boundSignature.argTypes(), signature.getName().name());
    }

    @Override
    public List<Object> evaluate(TransactionContext txnCtx, NodeContext nodeContext, Input<Object>[] args) {
        List<Object> listInput = (List<Object>) args[0].value();
        if (listInput == null || listInput.isEmpty()) {
            return List.of();
        }

        int from = 1;
        if (args.length >= 2) {
            from = intValueOrDefault(args[1], 1);
            ensureGtZero(from);
        }

        int to = listInput.size();
        if (args.length == 3) {
            to = Math.min(
                to,
                intValueOrDefault(args[2], listInput.size())
            );
        }
        ensureGtZero(to);

        return to >= from ? new ArrayList<>(listInput.subList(from - 1, to)) : List.of();
    }

    private static int intValueOrDefault(Input<Object> arg, int defaultValue) {
        Object value = arg.value();
        try {
            if (value != null) {
                return ((Number) value).intValue();
            } else {
                return defaultValue;
            }
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                "Array index must be in range 1 to " + Integer.MAX_VALUE, e);
        }
    }

    private static void ensureGtZero(int value) {
        if (value < 1) {
            throw new IllegalArgumentException("Array index must be in range 1 to " + Integer.MAX_VALUE);
        }
    }
}
