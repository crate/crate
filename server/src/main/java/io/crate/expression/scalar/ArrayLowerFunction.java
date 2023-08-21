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

import java.util.List;

import org.jetbrains.annotations.Nullable;

import io.crate.data.Input;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataTypes;
import io.crate.types.TypeSignature;

class ArrayLowerFunction extends Scalar<Integer, Object> {

    public static final String NAME = "array_lower";

    public static void register(ScalarFunctionModule module) {
        module.register(
            Signature.scalar(
                NAME,
                TypeSignature.parse("array(E)"),
                DataTypes.INTEGER.getTypeSignature(),
                DataTypes.INTEGER.getTypeSignature()
            ).withTypeVariableConstraints(typeVariable("E")),
            ArrayLowerFunction::new
        );
    }

    public ArrayLowerFunction(Signature signature, BoundSignature boundSignature) {
        super(signature, boundSignature);
        ensureInnerTypeIsNotUndefined(boundSignature.argTypes(), NAME);
    }

    @Override
    public Integer evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input[] args) {
        @SuppressWarnings("unchecked")
        List<Object> values = (List<Object>) args[0].value();
        Object dimensionArg = args[1].value();
        if (values == null || values.isEmpty() || dimensionArg == null) {
            return null;
        }
        int dimension = (int) dimensionArg;
        if (dimension <= 0) {
            return null;
        }
        return lowerBound(values, dimension, 1);
    }

    /**
     * Recursively traverses all sub-arrays up to requestedDimension.
     * If arrayOrItem is not a List we reached the last possible dimension.
     * @param requestedDimension original dimension provided in array_upper. Guaranteed to be > 0 before the first call.
     * @param currentDimension <= requestedDimension on initial and further calls.
     */
    static final Integer lowerBound(@Nullable Object arrayOrItem, int requestedDimension, int currentDimension) {
        if (arrayOrItem instanceof List dimensionArray) {
            // instanceof is null safe
            if (currentDimension == requestedDimension) {
                return dimensionArray.isEmpty() ? null : 1;
            } else {
                for (Object object: dimensionArray) {
                    Integer lower = lowerBound(object, requestedDimension, currentDimension + 1);
                    if (lower == null) {
                        // Either dimension doesn't exist on some further levels or it has empty arrays, so answer is null, let's propagate it upstairs.
                        return null;
                    }
                }
                // Requested dimension exists and has no empty arrays or nulls so lower bound is 1.
                return 1;
            }
        } else {
            // We are on the last dimension (array with regular non-array items)
            // but requested dimension size is not yet resolved and thus doesn't exist.
            return null;
        }
    }
}
