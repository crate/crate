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

package io.crate.operation.scalar;

import com.google.common.base.Preconditions;
import io.crate.data.Input;
import io.crate.metadata.BaseFunctionResolver;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.metadata.Signature;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

class ArrayUniqueFunction extends Scalar<Object[], Object> {

    public static final String NAME = "array_unique";
    private final FunctionInfo functionInfo;
    private final DataType<?> elementType;

    private static FunctionInfo createInfo(List<DataType> types) {
        ArrayType arrayType = (ArrayType) types.get(0);
        if (arrayType.innerType().equals(DataTypes.UNDEFINED) && types.size() == 2) {
            arrayType = (ArrayType) types.get(1);
        }
        return new FunctionInfo(new FunctionIdent(NAME, types), arrayType);
    }

    public static void register(ScalarFunctionModule module) {
        module.register(NAME, new Resolver());
    }

    private ArrayUniqueFunction(FunctionInfo functionInfo) {
        this.functionInfo = functionInfo;
        this.elementType = ((ArrayType) functionInfo.returnType()).innerType();
    }

    @Override
    public FunctionInfo info() {
        return functionInfo;
    }

    @Override
    public Object[] evaluate(Input[] args) {
        Set<Object> uniqueSet = new LinkedHashSet<>();
        for (Input array : args) {
            assert array != null : "inputs must never be null";
            Object[] arrayValue = (Object[]) array.value();
            if (arrayValue == null) {
                continue;
            }
            for (Object element : arrayValue) {
                uniqueSet.add(elementType.value(element));
            }
        }
        return uniqueSet.toArray();
    }


    private static class Resolver extends BaseFunctionResolver {

        protected Resolver() {
            super(Signature.numArgs(1, 2).and(Signature.withLenientVarArgs(Signature.ArgMatcher.ANY_ARRAY)));
        }

        @Override
        public FunctionImplementation getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
            Preconditions.checkArgument(
                dataTypes.size() >= 1 && dataTypes.size() <= 2, "array_unique function requires one or two arguments");

            for (int i = 0; i < dataTypes.size(); i++) {
                Preconditions.checkArgument(dataTypes.get(i) instanceof ArrayType, String.format(Locale.ENGLISH,
                    "Argument %d of the array_unique function cannot be converted to array", i + 1));
            }

            if (dataTypes.size() == 2) {
                DataType innerType0 = ((ArrayType) dataTypes.get(0)).innerType();
                DataType innerType1 = ((ArrayType) dataTypes.get(1)).innerType();

                Preconditions.checkArgument(
                    !innerType0.equals(DataTypes.UNDEFINED) || !innerType1.equals(DataTypes.UNDEFINED),
                    "One of the arguments of the array_unique function can be of undefined inner type, but not both");

                if (!innerType0.equals(DataTypes.UNDEFINED)) {
                    Preconditions.checkArgument(innerType1.isConvertableTo(innerType0),
                        String.format(Locale.ENGLISH,
                            "Second argument's inner type (%s) of the array_unique function cannot be converted to the first argument's inner type (%s)",
                            innerType1, innerType0));
                }
            } else if (dataTypes.size() == 1) {
                DataType innerType = ((ArrayType) dataTypes.get(0)).innerType();
                Preconditions.checkArgument(!innerType.equals(DataTypes.UNDEFINED),
                    "When used with only one argument, the inner type of the array argument cannot be undefined");
            }
            return new ArrayUniqueFunction(createInfo(dataTypes));
        }
    }
}
