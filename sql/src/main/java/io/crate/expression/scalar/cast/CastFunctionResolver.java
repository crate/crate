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

import com.google.common.annotations.VisibleForTesting;
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

    static final Map<DataType, String> CAST_SIGNATURES; // data type -> name

    static {
        CAST_SIGNATURES = new HashMap<>((PRIMITIVE_TYPES.size()) * 3 + 3);
        for (var type : PRIMITIVE_TYPES) {
            CAST_SIGNATURES.put(type, castFuncName(type));

            var arrayType = new ArrayType(type);
            CAST_SIGNATURES.put(arrayType, castFuncName(arrayType));
        }
        CAST_SIGNATURES.put(ObjectType.untyped(), castFuncName(ObjectType.untyped()));
        CAST_SIGNATURES.put(GEO_POINT, castFuncName(GEO_POINT));
        CAST_SIGNATURES.put(GEO_SHAPE, castFuncName(GEO_SHAPE));
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
    @VisibleForTesting
    static FunctionInfo functionInfo(DataType dataType, DataType returnType, boolean tryCast) {
        String functionName = CAST_SIGNATURES.get(returnType);
        if (functionName == null) {
            throw new IllegalArgumentException(
                String.format(Locale.ENGLISH, "No cast function found for return type %s",
                    returnType.getName()));
        }
        functionName = tryCast ? TRY_CAST_PREFIX + functionName : functionName;
        return new FunctionInfo(new FunctionIdent(functionName, List.of(dataType)), returnType);
    }

    public static boolean supportsExplicitConversion(DataType returnType) {
        return CAST_SIGNATURES.containsKey(returnType);
    }
}
