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

    public static final String TRY_CAST_PREFIX = "try_";

    public static class FunctionNames {
        public static final String TO_STRING = "to_string";
        public static final String TO_INTEGER = "to_int";
        public static final String TO_LONG = "to_long";
        public static final String TO_TIMESTAMP = "to_timestamp";
        public static final String TO_DOUBLE = "to_double";
        public static final String TO_BOOLEAN = "to_boolean";
        public static final String TO_FLOAT = "to_float";
        public static final String TO_BYTE = "to_byte";
        public static final String TO_SHORT = "to_short";
        public static final String TO_IP = "to_ip";

        public static final String TO_STRING_ARRAY = "to_string_array";
        public static final String TO_LONG_ARRAY = "to_long_array";
        public static final String TO_INTEGER_ARRAY = "to_int_array";
        public static final String TO_DOUBLE_ARRAY = "to_double_array";
        public static final String TO_BOOLEAN_ARRAY = "to_boolean_array";
        public static final String TO_BYTE_ARRAY = "to_byte_array";
        public static final String TO_FLOAT_ARRAY = "to_float_array";
        public static final String TO_SHORT_ARRAY = "to_short_array";
        public static final String TO_IP_ARRAY = "to_ip_array";
        public static final String TO_GEO_POINT = "to_geo_point";
        public static final String TO_GEO_SHAPE = "to_geo_shape";
    }

    static final ImmutableMap<DataType, String> PRIMITIVE_FUNCTION_MAP = new ImmutableMap.Builder<DataType, String>()
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

    static final ImmutableMap<DataType, String> GEO_FUNCTION_MAP = new ImmutableMap.Builder<DataType, String>()
            .put(DataTypes.GEO_POINT, FunctionNames.TO_GEO_POINT)
            .put(DataTypes.GEO_SHAPE, FunctionNames.TO_GEO_SHAPE)
            .build();

    static final ImmutableMap<DataType, String> ARRAY_FUNCTION_MAP = new ImmutableMap.Builder<DataType, String>()
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
    private static final ImmutableMap<DataType, String> FUNCTION_MAP = new ImmutableMap.Builder<DataType, String>()
            .putAll(PRIMITIVE_FUNCTION_MAP)
            .putAll(GEO_FUNCTION_MAP)
            .putAll(ARRAY_FUNCTION_MAP)
            .build();

    /**
     * resolve the needed conversion function info based on the wanted return data type
     */
    public static FunctionInfo functionInfo(DataType dataType, DataType returnType, boolean tryCast) {
        String functionName = FUNCTION_MAP.get(returnType);
        if (functionName == null) {
            throw new IllegalArgumentException(
                    String.format(Locale.ENGLISH, "No cast function found for return type %s",
                            returnType.getName()));
        }
        functionName = tryCast ? TRY_CAST_PREFIX + functionName : functionName;
        return new FunctionInfo(new FunctionIdent(functionName, ImmutableList.of(dataType)), returnType);
    }

    public static boolean supportsExplicitConversion(DataType returnType) {
        return FUNCTION_MAP.get(returnType) != null;
    }

    private static Function<String, String> TO_TRY_CAST_MAP = new Function<String, String>() {
        @Override
        public String apply(String functionName) {
            return TRY_CAST_PREFIX + functionName;
        }
    };

    public static Map<DataType, String> tryFunctionsMap() {
        return Maps.transformValues(FUNCTION_MAP, TO_TRY_CAST_MAP);
    }
}
