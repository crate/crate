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

import io.crate.data.Input;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.Signature;
import io.crate.types.ArrayType;
import io.crate.types.DataType;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static io.crate.expression.scalar.array.ArrayArgumentValidators.ensureBothInnerTypesAreNotUndefined;
import static io.crate.expression.scalar.array.ArrayArgumentValidators.ensureSingleArgumentArrayInnerTypeIsNotUndefined;
import static io.crate.metadata.functions.TypeVariableConstraint.typeVariable;
import static io.crate.types.TypeSignature.parseTypeSignature;

class ArrayUniqueFunction extends Scalar<List<Object>, List<Object>> {

    public static final String NAME = "array_unique";

    public static void register(ScalarFunctionModule module) {
        module.register(
            Signature.scalar(
                NAME,
                parseTypeSignature("array(E)"),
                parseTypeSignature("array(E)")
            ).withTypeVariableConstraints(typeVariable("E")),
            ArrayUniqueFunction::new
        );
        module.register(
            Signature.scalar(
                NAME,
                parseTypeSignature("array(E)"),
                parseTypeSignature("array(E)"),
                parseTypeSignature("array(E)")
            ).withTypeVariableConstraints(typeVariable("E")),
            ArrayUniqueFunction::new
        );
    }

    private final Signature signature;
    private final Signature boundSignature;
    private final DataType<?> elementType;

    private ArrayUniqueFunction(Signature signature, Signature boundSignature) {
        this.signature = signature;
        this.boundSignature = boundSignature;
        this.elementType = ((ArrayType<?>) boundSignature.getReturnType().createType()).innerType();
        var argumentTypes = boundSignature.getArgumentDataTypes();
        if (argumentTypes.size() == 1) {
            ensureSingleArgumentArrayInnerTypeIsNotUndefined(argumentTypes);
        } else {
            ensureBothInnerTypesAreNotUndefined(boundSignature.getArgumentDataTypes(), NAME);
        }
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
    public List<Object> evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input[] args) {
        Set<Object> uniqueSet = new HashSet<>();
        ArrayList<Object> uniqueItems = new ArrayList<>();
        for (Input array : args) {
            assert array != null : "inputs must never be null";
            List<Object> values = (List<Object>) array.value();
            if (values == null) {
                continue;
            }
            for (Object element : values) {
                Object value = elementType.sanitizeValue(element);
                if (uniqueSet.add(value)) {
                    uniqueItems.add(value);
                }
            }
        }
        return uniqueItems;
    }
}
