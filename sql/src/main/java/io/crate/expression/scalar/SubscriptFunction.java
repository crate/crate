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

package io.crate.expression.scalar;

import io.crate.data.Input;
import io.crate.expression.symbol.FuncArg;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.FunctionResolver;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.ObjectType;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

import static io.crate.expression.scalar.SubscriptObjectFunction.tryToInferReturnTypeFromObjectTypeAndArguments;

/** Supported subscript expressions:
 * <ul>
 *     <li>obj['x']</li>
 *     <li>arr[1]</li>
 *     <li>obj_array[1]</li>
 *     <li>obj_array['x']</li>
 * </ul>
 **/
public class SubscriptFunction extends Scalar<Object, Object[]> {

    public static final String NAME = "subscript";

    private final FunctionInfo info;
    private final BiFunction<Object, Object, Object> lookup;

    public static void register(ScalarFunctionModule module) {
        module.register(NAME, new Resolver());
    }

    private SubscriptFunction(FunctionInfo info, BiFunction<Object, Object, Object> lookup) {
        this.info = info;
        this.lookup = lookup;
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
        if (func.arguments().get(0).valueType().id() == ObjectType.ID) {
            return tryToInferReturnTypeFromObjectTypeAndArguments(func);
        }
        return func;
    }

    @Override
    public Object evaluate(TransactionContext txnCtx, Input[] args) {
        assert args.length == 2 : "invalid number of arguments";
        Object element = args[0].value();
        Object index = args[1].value();
        if (element == null || index == null) {
            return null;
        }
        return lookup.apply(element, index);
    }

    private static class Resolver implements FunctionResolver {

        private static final Set<DataType<?>> NUMERIC_ARRAY_INDEX_TYPES = Set.of(
            DataTypes.SHORT, DataTypes.INTEGER, DataTypes.LONG
        );

        @Nullable
        @Override
        public List<DataType> getSignature(List<? extends FuncArg> funcArgs) {
            // Only size check and normalizing numeric index to integer is done here
            // The rest of the validation happens in getForTypes.
            if (funcArgs.size() != 2) {
                return null;
            }
            DataType<?> baseType = funcArgs.get(0).valueType();
            DataType<?> indexType = funcArgs.get(1).valueType();
            if (baseType.id() == ArrayType.ID && NUMERIC_ARRAY_INDEX_TYPES.contains(indexType)) {
                return List.of(baseType, DataTypes.INTEGER);
            }
            return List.of(baseType, indexType);
        }

        @Override
        public FunctionImplementation getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
            assert dataTypes.size() == 2 : "Subscript function must have 2 arguments";
            DataType<?> baseType = dataTypes.get(0);
            DataType<?> returnType;
            BiFunction<Object, Object, Object> lookupElement;
            if (baseType instanceof ArrayType<?>) {
                if (dataTypes.get(1).equals(DataTypes.STRING)) {
                    if (ArrayType.unnest(baseType).id() == ObjectType.ID) {
                        returnType = new ArrayType<>(DataTypes.UNDEFINED);
                        lookupElement = SubscriptFunction::lookupIntoListObjectsByName;
                    } else {
                        throw new IllegalArgumentException(
                            "`index` in subscript expression (`base[index]`) must be a numeric type if the base expression is " + baseType);
                    }
                } else {
                    returnType = ((ArrayType<?>) baseType).innerType();
                    lookupElement = SubscriptFunction::lookupByNumericIndex;
                }
            } else {
                returnType = DataTypes.UNDEFINED;
                lookupElement = SubscriptFunction::lookupByName;
            }
            FunctionInfo info = FunctionInfo.of(NAME, dataTypes, returnType);
            return new SubscriptFunction(info, lookupElement);
        }
    }

    static Object lookupIntoListObjectsByName(Object base, Object name) {
        List<?> values = (List<?>) base;
        List<Object> result = new ArrayList<>(values.size());
        for (int i = 0; i < values.size(); i++) {
            Map<?, ?> map = (Map<?, ?>) values.get(i);
            result.add(map.get(name));
        }
        return result;
    }

    static Object lookupByNumericIndex(Object base, Object index) {
        List<?> values = (List<?>) base;
        int idx = (Integer) index;
        try {
            return values.get(idx - 1); // SQL array indices start with 1
        } catch (IndexOutOfBoundsException e) {
            return null;
        }
    }

    static Object lookupByName(Object base, Object name) {
        Map<?, ?> map = (Map<?, ?>) base;
        return map.get(name);
    }
}
