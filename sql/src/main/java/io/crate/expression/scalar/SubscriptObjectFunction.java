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

import io.crate.data.Input;
import io.crate.metadata.BaseFunctionResolver;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.params.FuncParams;
import io.crate.metadata.functions.params.Param;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.ObjectType;
import io.crate.types.StringType;
import io.crate.types.UndefinedType;

import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Scalar function to resolve elements inside a map.
 * Because the function registration does not take the function return type into account, it is not possible
 * to register the same function with different return types under the same name.
 * As a workaround, the return type is encoded into the function name by using {@link #getNameForReturnType(DataType)}.
 */
public class SubscriptObjectFunction extends Scalar<Object, Map> {

    private static final String NAME = "subscript_obj";
    private static final FuncParams FUNCTION_PARAMS = FuncParams
        .builder(Param.of(ObjectType.untyped()), Param.of(StringType.INSTANCE))
        .withVarArgs(Param.of(StringType.INSTANCE))
        .build();

    private FunctionInfo info;

    public static void register(ScalarFunctionModule module) {
        for (DataType returnType : DataTypes.allRegisteredTypes()) {
            module.register(getNameForReturnType(returnType),
                new BaseFunctionResolver(FUNCTION_PARAMS) {
                    @Override
                    public FunctionImplementation getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
                        return ofReturnType(returnType, dataTypes);
                    }
                });
        }
    }

    public static SubscriptObjectFunction ofReturnType(DataType returnType, List<DataType> dataTypes) {
        return new SubscriptObjectFunction(
            new FunctionInfo(
                new FunctionIdent(getNameForReturnType(returnType), dataTypes),
                returnType));
    }

    public static String getNameForReturnType(DataType dataType) {
        String name = NAME;
        if (dataType.id() != UndefinedType.ID) {
            String returnTypeName;
            if (DataTypes.isArray(dataType)) {
                returnTypeName = ArrayType.NAME;
            } else {
                returnTypeName = dataType.getName();
            }
            name += "_" + returnTypeName;
        }
        return name;
    }

    private SubscriptObjectFunction(FunctionInfo info) {
        this.info = info;
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    @Override
    public Object evaluate(TransactionContext txnCtx, Input[] args) {
        assert args.length >= 2 : "invalid number of arguments";

        Object mapValue = args[0].value();
        for (var i = 1; i < args.length; i++) {
            mapValue = evaluate(mapValue, args[i].value());
        }
        return mapValue;
    }

    private static Object evaluate(Object element, Object key) {
        if (element == null || key == null) {
            return null;
        }
        assert element instanceof Map : "first argument must be of type Map";
        assert key instanceof String : "second argument must be of type String";

        Map m = (Map) element;
        String k = (String) key;
        if (!m.containsKey(k)) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH, "The object does not contain [%s] key", k));
        }
        return m.get(k);
    }
}
