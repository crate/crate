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
import io.crate.metadata.BaseFunctionResolver;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.functions.params.FuncParams;
import io.crate.metadata.functions.params.Param;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.IntegerType;

import java.util.List;
import java.util.Locale;
import java.util.function.Function;

final class ArrayBoundFunctionResolver extends BaseFunctionResolver {

    private final String functionName;
    private final Function<FunctionInfo, FunctionImplementation> functionSupplier;

    ArrayBoundFunctionResolver(String functionName,
                               Function<FunctionInfo, FunctionImplementation> functionSupplier) {
        super(FuncParams.builder(Param.ANY_ARRAY, Param.INTEGER).build());
        this.functionName = functionName;
        this.functionSupplier = functionSupplier;
    }

    @Override
    public FunctionImplementation getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
        DataType arrayArgument = dataTypes.get(0);
        Preconditions.checkArgument(arrayArgument instanceof ArrayType, String.format(Locale.ENGLISH,
            "The first argument of the %s function cannot be converted to array", functionName));

        Preconditions.checkArgument(!((ArrayType) arrayArgument).innerType().equals(DataTypes.UNDEFINED), String.format(Locale.ENGLISH,
                                                                                                                        "The first argument of the %s function cannot be undefined", functionName));

        Preconditions.checkArgument(dataTypes.get(1) instanceof IntegerType, String.format(Locale.ENGLISH,
            "The second argument the %s function cannot be converted to integer", functionName));

        return functionSupplier.apply(createInfo(dataTypes));
    }

    private FunctionInfo createInfo(List<DataType> types) {
        return new FunctionInfo(new FunctionIdent(functionName, types), DataTypes.INTEGER);
    }
}
