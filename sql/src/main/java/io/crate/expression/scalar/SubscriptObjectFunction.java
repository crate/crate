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
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.BaseFunctionResolver;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.params.FuncParams;
import io.crate.metadata.functions.params.Param;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.ObjectType;
import io.crate.types.StringType;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Scalar function to resolve elements inside a map.
 */
public class SubscriptObjectFunction extends Scalar<Object, Map> {

    public static final String NAME = "subscript_obj";
    private static final FuncParams FUNCTION_PARAMS = FuncParams
        .builder(Param.of(ObjectType.untyped()), Param.of(StringType.INSTANCE))
        .withVarArgs(Param.of(StringType.INSTANCE))
        .build();

    private FunctionInfo info;

    public static void register(ScalarFunctionModule module) {
        module.register(NAME, new BaseFunctionResolver(FUNCTION_PARAMS) {

            @Override
            public FunctionImplementation getForTypes(List<DataType> types) throws IllegalArgumentException {
                return new SubscriptObjectFunction(new FunctionInfo(
                    new FunctionIdent(NAME, types),
                    DataTypes.UNDEFINED
                ));
            }
        });
    }

    private SubscriptObjectFunction(FunctionInfo info) {
        this.info = info;
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    @Override
    public Symbol normalizeSymbol(Function func, TransactionContext txnCtx) {
        Symbol result = evaluateIfLiterals(this, txnCtx, func);
        if (result instanceof Literal) {
            return result;
        }
        return tryToInferReturnTypeFromObjectTypeAndArguments(func);
    }

    private static Symbol tryToInferReturnTypeFromObjectTypeAndArguments(Function func) {
        var arguments = func.arguments();
        ObjectType objectType = (ObjectType) arguments.get(0).valueType();
        List<String> path = maybeCreatePath(arguments);
        if (path == null) {
            return func;
        } else {
            DataType<?> returnType = objectType.resolveInnerType(path);
            return returnType.equals(DataTypes.UNDEFINED)
                ? func
                : new Function(new FunctionInfo(func.info().ident(), returnType), func.arguments());
        }
    }

    @Nullable
    private static List<String> maybeCreatePath(List<Symbol> arguments) {
        List<String> path = null;
        for (int i = 1; i < arguments.size(); i++) {
            Symbol arg = arguments.get(i);
            if (arg instanceof Literal) {
                if (path == null) {
                    path = new ArrayList<>();
                }
                path.add(DataTypes.STRING.value(((Literal) arg).value()));
            } else {
                return null;
            }
        }
        return path;
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
