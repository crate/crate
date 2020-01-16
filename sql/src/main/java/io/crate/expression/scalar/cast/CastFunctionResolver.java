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

package io.crate.expression.scalar.cast;

import io.crate.common.collections.Lists2;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.ObjectType;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static io.crate.types.DataTypes.GEO_POINT;
import static io.crate.types.DataTypes.GEO_SHAPE;
import static io.crate.types.DataTypes.PRIMITIVE_TYPES;

public class CastFunctionResolver {

    static final String TRY_CAST_PREFIX = "try_";
    private static final String TO_PREFIX = "to_";

    static final Map<String, DataType> CAST_SIGNATURES; // cast function name -> data type

    static {
        List<DataType> CAST_FUNC_TYPES = Lists2.concat(
            PRIMITIVE_TYPES,
            List.of(GEO_SHAPE, GEO_POINT, ObjectType.untyped()));

        CAST_SIGNATURES = new HashMap<>((CAST_FUNC_TYPES.size()) * 2);
        for (var type : CAST_FUNC_TYPES) {
            CAST_SIGNATURES.put(castFuncName(type), type);

            var arrayType = new ArrayType<>(type);
            CAST_SIGNATURES.put(castFuncName(arrayType), arrayType);
        }
    }

    private static String castFuncName(DataType type) {
        return TO_PREFIX + type.getName();
    }

    public static Symbol generateCastFunction(Symbol sourceSymbol, DataType targetType, boolean tryCast) {
        DataType sourceType = sourceSymbol.valueType();
        FunctionInfo functionInfo = functionInfo(sourceType, targetType, tryCast);
        return new Function(functionInfo, List.of(sourceSymbol));
    }

    /**
     * resolve the needed conversion function info based on the wanted return data type
     */
    private static FunctionInfo functionInfo(DataType dataType, DataType returnType, boolean tryCast) {
        var castFunctionName = castFuncName(returnType);
        if (CAST_SIGNATURES.get(castFunctionName) == null) {
            throw new IllegalArgumentException(
                String.format(Locale.ENGLISH, "No cast function found for return type %s",
                    returnType.getName()));
        }
        castFunctionName = tryCast ? TRY_CAST_PREFIX + castFunctionName : castFunctionName;
        return new FunctionInfo(new FunctionIdent(castFunctionName, List.of(dataType)), returnType);
    }

    public static boolean supportsExplicitConversion(DataType returnType) {
        return CAST_SIGNATURES.containsKey(castFuncName(returnType));
    }
}
