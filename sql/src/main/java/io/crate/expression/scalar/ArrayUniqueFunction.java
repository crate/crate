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
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.Signature;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

import javax.annotation.Nullable;
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
            (signature, argumentTypes) -> {
                ensureSingleArgumentArrayInnerTypeIsNotUndefined(argumentTypes);
                return new ArrayUniqueFunction(
                    createInfo(argumentTypes),
                    signature
                );
            }
        );
        module.register(
            Signature.scalar(
                NAME,
                parseTypeSignature("array(E)"),
                parseTypeSignature("array(E)"),
                parseTypeSignature("array(E)")
            ).withTypeVariableConstraints(typeVariable("E")),
            (signature, argumentTypes) -> {
                ensureBothInnerTypesAreNotUndefined(argumentTypes, NAME);
                return new ArrayUniqueFunction(
                    createInfo(argumentTypes),
                    signature
                );
            }
        );
    }

    private static FunctionInfo createInfo(List<DataType> types) {
        ArrayType arrayType = (ArrayType) types.get(0);
        if (arrayType.innerType().equals(DataTypes.UNDEFINED) && types.size() == 2) {
            arrayType = (ArrayType) types.get(1);
        }
        return new FunctionInfo(new FunctionIdent(NAME, types), arrayType);
    }

    private final FunctionInfo functionInfo;
    private final Signature signature;
    private final DataType<?> elementType;

    private ArrayUniqueFunction(FunctionInfo functionInfo, Signature signature) {
        this.functionInfo = functionInfo;
        this.signature = signature;
        this.elementType = ((ArrayType) functionInfo.returnType()).innerType();
    }

    @Override
    public FunctionInfo info() {
        return functionInfo;
    }

    @Nullable
    @Override
    public Signature signature() {
        return signature;
    }

    @Override
    public List<Object> evaluate(TransactionContext txnCtx, Input[] args) {
        Set<Object> uniqueSet = new HashSet<>();
        ArrayList<Object> uniqueItems = new ArrayList<>();
        for (Input array : args) {
            assert array != null : "inputs must never be null";
            List<Object> values = (List<Object>) array.value();
            if (values == null) {
                continue;
            }
            for (Object element : values) {
                Object value = elementType.value(element);
                if (uniqueSet.add(value)) {
                    uniqueItems.add(value);
                }
            }
        }
        return uniqueItems;
    }
}
