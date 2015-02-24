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

package io.crate.operation.scalar.cast;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

import java.util.Locale;

public class CastFunctionResolver {

    // TODO: register all type conversion functions here
    private static final ImmutableMap<DataType, String> functionMap = new ImmutableMap.Builder<DataType, String>()
            .put(DataTypes.STRING, ToStringFunction.NAME)
            .put(DataTypes.INTEGER, ToIntFunction.NAME)
            .put(DataTypes.LONG, ToLongFunction.NAME)
            .put(DataTypes.TIMESTAMP, ToTimestampFunction.NAME)
            .put(DataTypes.BOOLEAN, ToBooleanFunction.NAME)
            .put(DataTypes.DOUBLE, ToDoubleFunction.NAME)
            .put(DataTypes.FLOAT, ToFloatFunction.NAME)
            .put(DataTypes.BYTE, ToByteFunction.NAME)
            .put(DataTypes.SHORT, ToShortFunction.NAME)
            .put(DataTypes.UNDEFINED, ToNullFunction.NAME)
            .put(ToStringArrayFunction.ARRAY_STRING_TYPE, ToStringArrayFunction.NAME)
            .put(ToLongArrayFunction.LONG_ARRAY_TYPE, ToLongArrayFunction.NAME)
            .put(ToIntArrayFunction.INT_ARRAY_TYPE, ToIntArrayFunction.NAME)
            .put(ToDoubleArrayFunction.DOUBLE_ARRAY_TYPE, ToDoubleArrayFunction.NAME)
            .put(ToBooleanArrayFunction.BOOLEAN_ARRAY_TYPE, ToBooleanArrayFunction.NAME)
            .put(ToByteArrayFunction.BYTE_ARRAY_TYPE, ToByteArrayFunction.NAME)
            .put(ToFloatArrayFunction.FLOAT_ARRAY_TYPE, ToFloatArrayFunction.NAME)
            .put(ToShortArrayFunction.SHORT_ARRAY_TYPE, ToShortArrayFunction.NAME)
            .build();

    /**
     * resolve the needed conversion function info based on the wanted return data type
     */
    public static FunctionInfo functionInfo(DataType dataType, DataType returnType) {
        String functionName = functionMap.get(returnType);
        if (functionName == null) {
            throw new IllegalArgumentException(
                    String.format(Locale.ENGLISH, "No cast function found for return type %s",
                            returnType.getName()));
        }
        return new FunctionInfo(new FunctionIdent(functionName, ImmutableList.of(dataType)), returnType);
    }

}
