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

package io.crate.operation.scalar;

import com.google.common.base.Preconditions;
import io.crate.data.Input;
import io.crate.metadata.BaseFunctionResolver;
import io.crate.metadata.functions.params.FuncParams;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.metadata.functions.params.Param;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

class ArrayCatFunction extends Scalar<Object[], Object> {

    public static final String NAME = "array_cat";
    private FunctionInfo functionInfo;

    public static FunctionInfo createInfo(List<DataType> types) {
        ArrayType arrayType = (ArrayType) types.get(0);
        if (arrayType.innerType().equals(DataTypes.UNDEFINED)) {
            arrayType = (ArrayType) types.get(1);
        }
        return new FunctionInfo(new FunctionIdent(NAME, types), arrayType);
    }

    public static void register(ScalarFunctionModule module) {
        module.register(NAME, new Resolver());
    }

    ArrayCatFunction(FunctionInfo functionInfo) {
        this.functionInfo = functionInfo;
    }

    @Override
    public FunctionInfo info() {
        return functionInfo;
    }

    @Override
    public Object[] evaluate(Input[] args) {
        DataType innerType = ((ArrayType) this.info().returnType()).innerType();
        List<Object> resultList = new ArrayList<>();

        for (Input array : args) {
            Object arrayValue = array.value();
            if (arrayValue == null) {
                continue;
            }

            Object[] arg = (Object[]) arrayValue;
            for (Object element : arg) {
                resultList.add(innerType.value(element));
            }
        }

        return resultList.toArray();
    }


    private static class Resolver extends BaseFunctionResolver {

        protected Resolver() {
            super(FuncParams.builder(Param.ANY_ARRAY, Param.ANY_ARRAY).build());
        }

        @Override
        public FunctionImplementation getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
            for (int i = 0; i < dataTypes.size(); i++) {
                Preconditions.checkArgument(dataTypes.get(i) instanceof ArrayType, String.format(Locale.ENGLISH,
                    "Argument %d of the array_cat function cannot be converted to array", i + 1));
            }

            DataType innerType0 = ((ArrayType) dataTypes.get(0)).innerType();
            DataType innerType1 = ((ArrayType) dataTypes.get(1)).innerType();

            Preconditions.checkArgument(
                !innerType0.equals(DataTypes.UNDEFINED) || !innerType1.equals(DataTypes.UNDEFINED),
                "One of the arguments of the array_cat function can be of undefined inner type, but not both");

            if (!innerType0.equals(DataTypes.UNDEFINED)) {
                Preconditions.checkArgument(innerType1.isConvertableTo(innerType0),
                    String.format(Locale.ENGLISH,
                        "Second argument's inner type (%s) of the array_cat function cannot be converted to the first argument's inner type (%s)",
                        innerType1, innerType0));
            }

            return new ArrayCatFunction(createInfo(dataTypes));
        }
    }
}
