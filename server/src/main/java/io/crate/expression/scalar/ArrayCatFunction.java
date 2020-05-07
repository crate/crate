/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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
import java.util.List;

import static io.crate.expression.scalar.array.ArrayArgumentValidators.ensureBothInnerTypesAreNotUndefined;
import static io.crate.metadata.functions.TypeVariableConstraint.typeVariable;
import static io.crate.types.TypeSignature.parseTypeSignature;

class ArrayCatFunction extends Scalar<List<Object>, List<Object>> {

    public static final String NAME = "array_cat";

    public static FunctionInfo createInfo(List<DataType> types, String name) {
        ensureBothInnerTypesAreNotUndefined(types, name);
        ArrayType<?> arrayType = (ArrayType<?>) types.get(0);
        if (arrayType.innerType().equals(DataTypes.UNDEFINED)) {
            arrayType = (ArrayType<?>) types.get(1);
        }
        return new FunctionInfo(new FunctionIdent(NAME, types), arrayType);
    }

    public static void register(ScalarFunctionModule module) {
        module.register(
            Signature.scalar(
                NAME,
                parseTypeSignature("array(E)"),
                parseTypeSignature("array(E)"),
                parseTypeSignature("array(E)")
            )
                .withTypeVariableConstraints(typeVariable("E")),
            (signature, args) ->
                new ArrayCatFunction(
                    ArrayCatFunction.createInfo(args, NAME),
                    signature
                )
        );
    }

    private final FunctionInfo functionInfo;
    private final Signature signature;

    ArrayCatFunction(FunctionInfo functionInfo, Signature signature) {
        this.functionInfo = functionInfo;
        this.signature = signature;
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

    @SafeVarargs
    @Override
    public final List<Object> evaluate(TransactionContext txnCtx, Input<List<Object>>... args) {
        DataType<?> innerType = ((ArrayType<?>) this.info().returnType()).innerType();
        ArrayList<Object> resultList = new ArrayList<>();
        for (Input<List<Object>> arg : args) {
            List<Object> values = arg.value();
            if (values == null) {
                continue;
            }
            for (Object value : values) {
                resultList.add(innerType.value(value));
            }
        }
        return resultList;
    }

}
