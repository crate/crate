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

import com.google.common.base.Preconditions;
import io.crate.data.Input;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.Signature;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static io.crate.metadata.functions.TypeVariableConstraint.typeVariable;
import static io.crate.types.TypeSignature.parseTypeSignature;

class ArrayCatFunction extends Scalar<List<Object>, List<Object>> {

    public static final String NAME = "array_cat";
    private final FunctionInfo functionInfo;

    public static FunctionInfo createInfo(List<DataType> types) {
        validateInnerTypes(types);
        ArrayType arrayType = (ArrayType) types.get(0);
        if (arrayType.innerType().equals(DataTypes.UNDEFINED)) {
            arrayType = (ArrayType) types.get(1);
        }
        return new FunctionInfo(new FunctionIdent(NAME, types), arrayType);
    }

    public static void register(ScalarFunctionModule module) {
        module.register(
            Signature.builder()
                .name(NAME)
                .kind(FunctionInfo.Type.SCALAR)
                .typeVariableConstraints(typeVariable("E"))
                .argumentTypes(parseTypeSignature("array(E)"), parseTypeSignature("array(E)"))
                .returnType(parseTypeSignature("array(E)"))
                .setVariableArity(false)
                .build(),
            args -> new ArrayCatFunction(ArrayCatFunction.createInfo(args))
        );
    }

    ArrayCatFunction(FunctionInfo functionInfo) {
        this.functionInfo = functionInfo;
    }

    @Override
    public FunctionInfo info() {
        return functionInfo;
    }

    @SafeVarargs
    @Override
    public final List<Object> evaluate(TransactionContext txnCtx, Input<List<Object>>... args) {
        DataType innerType = ((ArrayType) this.info().returnType()).innerType();
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

    static void validateInnerTypes(List<DataType> dataTypes) {
        DataType innerType0 = ((ArrayType) dataTypes.get(0)).innerType();
        DataType innerType1 = ((ArrayType) dataTypes.get(1)).innerType();

        Preconditions.checkArgument(
            !innerType0.equals(DataTypes.UNDEFINED) || !innerType1.equals(DataTypes.UNDEFINED),
            "When concatenating arrays, one of the two arguments can be of undefined inner type, but not both");

        if (!innerType0.equals(DataTypes.UNDEFINED)) {
            Preconditions.checkArgument(innerType1.isConvertableTo(innerType0),
                                        String.format(Locale.ENGLISH,
                                                      "Second argument's inner type (%s) of the array_cat function cannot be converted to the first argument's inner type (%s)",
                                                      innerType1, innerType0));
        }
    }
}
