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

import com.google.common.base.Preconditions;
import io.crate.data.Input;
import io.crate.metadata.BaseFunctionResolver;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.metadata.functions.params.FuncParams;
import io.crate.metadata.functions.params.Param;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.IntegerType;

import java.util.List;
import java.util.Locale;

class ArrayUpperFunction extends Scalar<Integer, Object[]> {

    public static final String NAME = "array_upper";
    private FunctionInfo functionInfo;

    public static FunctionInfo createInfo(List<DataType> types) {
        return new FunctionInfo(new FunctionIdent(NAME, types), DataTypes.INTEGER);
    }

    public static void register(ScalarFunctionModule module) {
        module.register(NAME, new Resolver());
    }

    private ArrayUpperFunction(FunctionInfo functionInfo) {
        this.functionInfo = functionInfo;
    }

    @Override
    public FunctionInfo info() {
        return functionInfo;
    }

    @Override
    public Integer evaluate(Input[] args) {
        Object[] array = (Object[]) args[0].value();
        if (array == null || array.length == 0 || args[1].value() == null) {
            return null;
        }

        // sql dimensions are 1 indexed
        int dimension = (int) args[1].value() - 1;

        try {
            Object dimensionValue = array[dimension];
            if (dimensionValue.getClass().isArray()) {
                Object[] dimensionArray = (Object[]) dimensionValue;
                return dimensionArray.length;
            }
            // it's a one dimension array so return the argument length
            return array.length;
        } catch (ArrayIndexOutOfBoundsException e) {
            return null;
        }
    }


    private static class Resolver extends BaseFunctionResolver {

        protected Resolver() {
            super(FuncParams.builder(Param.ANY_ARRAY, Param.INTEGER).build());
        }

        @Override
        public FunctionImplementation getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
            DataType arrayArgument = dataTypes.get(0);
            Preconditions.checkArgument(arrayArgument instanceof ArrayType, String.format(Locale.ENGLISH,
                "The first argument of the %s function cannot be converted to array", NAME));

            Preconditions.checkArgument(!((ArrayType) arrayArgument).innerType().equals(DataTypes.UNDEFINED), String.format(Locale.ENGLISH,
                "The first argument of the %s function cannot be undefined", NAME));

            Preconditions.checkArgument(dataTypes.get(1) instanceof IntegerType, String.format(Locale.ENGLISH,
                "The second argument the %s function cannot be converted to integer", NAME));

            return new ArrayUpperFunction(createInfo(dataTypes));
        }
    }
}
