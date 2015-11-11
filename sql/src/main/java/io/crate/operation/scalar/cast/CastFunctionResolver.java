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

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

import java.util.Locale;
import java.util.Map;

public class CastFunctionResolver {

    private static final String TRY_CAST_PREFIX = "try_";

    public static class FunctionNames {
        public static final String TO_STRING = "toString";
        public static final String TO_INTEGER = "toInt";
        public static final String TO_LONG = "toLong";
        public static final String TO_TIMESTAMP = "toTimestamp";
        public static final String TO_DOUBLE = "toDouble";
        public static final String TO_BOOLEAN = "toBoolean";
        public static final String TO_FLOAT = "toFloat";
        public static final String TO_BYTE = "toByte";
        public static final String TO_SHORT = "toShort";
        public static final String TO_IP = "toIp";

        public static final String TO_STRING_ARRAY = "toStringArray";
        public static final String TO_LONG_ARRAY = "toLongArray";
        public static final String TO_INTEGER_ARRAY = "toIntArray";
        public static final String TO_DOUBLE_ARRAY = "toDoubleArray";
        public static final String TO_BOOLEAN_ARRAY = "toBooleanArray";
        public static final String TO_BYTE_ARRAY = "toByteArray";
        public static final String TO_FLOAT_ARRAY = "toFloatArray";
        public static final String TO_SHORT_ARRAY = "toShortArray";
        public static final String TO_IP_ARRAY = "toIpArray";
        public static final String TO_GEO_POINT = "toGeoPoint";
        public static final String TO_GEO_SHAPE = "toGeoShape";
    }

    static final ImmutableMap<DataType, String> primitiveFunctionMap = new ImmutableMap.Builder<DataType, String>()
            .put(DataTypes.STRING, FunctionNames.TO_STRING)
            .put(DataTypes.INTEGER, FunctionNames.TO_INTEGER)
            .put(DataTypes.LONG, FunctionNames.TO_LONG)
            .put(DataTypes.TIMESTAMP, FunctionNames.TO_TIMESTAMP)
            .put(DataTypes.DOUBLE, FunctionNames.TO_DOUBLE)
            .put(DataTypes.BOOLEAN, FunctionNames.TO_BOOLEAN)
            .put(DataTypes.FLOAT, FunctionNames.TO_FLOAT)
            .put(DataTypes.BYTE, FunctionNames.TO_BYTE)
            .put(DataTypes.SHORT, FunctionNames.TO_SHORT)
            .put(DataTypes.IP, FunctionNames.TO_IP)
            .build();

    static final ImmutableMap<DataType, String> geoFunctionMap = new ImmutableMap.Builder<DataType, String>()
            .put(DataTypes.GEO_POINT, FunctionNames.TO_GEO_POINT)
            .put(DataTypes.GEO_SHAPE, FunctionNames.TO_GEO_SHAPE)
            .build();

    static final ImmutableMap<DataType, String> arrayFunctionMap = new ImmutableMap.Builder<DataType, String>()
            .put(new ArrayType(DataTypes.STRING), FunctionNames.TO_STRING_ARRAY)
            .put(new ArrayType(DataTypes.LONG), FunctionNames.TO_LONG_ARRAY)
            .put(new ArrayType(DataTypes.INTEGER), FunctionNames.TO_INTEGER_ARRAY)
            .put(new ArrayType(DataTypes.DOUBLE), FunctionNames.TO_DOUBLE_ARRAY)
            .put(new ArrayType(DataTypes.BOOLEAN), FunctionNames.TO_BOOLEAN_ARRAY)
            .put(new ArrayType(DataTypes.BYTE), FunctionNames.TO_BYTE_ARRAY)
            .put(new ArrayType(DataTypes.FLOAT), FunctionNames.TO_FLOAT_ARRAY)
            .put(new ArrayType(DataTypes.SHORT), FunctionNames.TO_SHORT_ARRAY)
            .put(new ArrayType(DataTypes.IP), FunctionNames.TO_IP_ARRAY)
            .build();

    // TODO: register all type conversion functions here
    private static final ImmutableMap<DataType, String> functionMap = new ImmutableMap.Builder<DataType, String>()
            .putAll(primitiveFunctionMap)
            .putAll(geoFunctionMap)
            .putAll(arrayFunctionMap)
            .build();

    /**
     * resolve the needed conversion function info based on the wanted return data type
     */
    public static FunctionInfo functionInfo(DataType dataType, DataType returnType, boolean tryCast) {
        String functionName = functionMap.get(returnType);
        if (functionName == null) {
            throw new IllegalArgumentException(
                    String.format(Locale.ENGLISH, "No cast function found for return type %s",
                            returnType.getName()));
        }
        functionName = tryCast ? TRY_CAST_PREFIX + functionName : functionName;
        return new FunctionInfo(new FunctionIdent(functionName, ImmutableList.of(dataType)), returnType);
    }

    public static boolean supportsExplicitConversion(DataType returnType) {
        return functionMap.get(returnType) != null;
    }

    private static Function<String, String> toTryCastMap = new Function<String, String>() {
        @Override
        public String apply(String functionName) {
            return TRY_CAST_PREFIX + functionName;
        }
    };

    public static Map<DataType, String> tryFunctionsMap() {
        return Maps.transformValues(functionMap, toTryCastMap);
    }

}
