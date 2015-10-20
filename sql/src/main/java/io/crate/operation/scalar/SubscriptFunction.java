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

package io.crate.operation.scalar;

import com.google.common.base.Preconditions;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.*;
import io.crate.operation.Input;
import io.crate.types.CollectionType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

import java.util.List;

public class SubscriptFunction extends Scalar<Object, Object[]> implements DynamicFunctionResolver {

    public static final String NAME = "subscript";

    private static FunctionInfo createInfo(List<DataType> argumentTypes, DataType returnType) {
        return new FunctionInfo(new FunctionIdent(NAME, argumentTypes), returnType);
    }
    public static void register(ScalarFunctionModule module) {
        module.register(NAME, new SubscriptFunction());
    }

    private FunctionInfo info;

    private SubscriptFunction() {
    }

    public SubscriptFunction(FunctionInfo info) {
        this.info = info;
    }

    @Override
    public FunctionInfo info() {
        return info;
    }


    @Override
    public Symbol normalizeSymbol(Function symbol) {
        final int size = symbol.arguments().size();
        assert size == 2 : "invalid number of arguments";

        if (anyNonLiterals(symbol.arguments())) {
            return symbol;
        }

        final Symbol input = symbol.arguments().get(0);
        final Symbol index = symbol.arguments().get(1);
        final Object inputValue = ((Input) input).value();
        final Object indexValue = ((Input) index).value();
        if (inputValue == null || indexValue == null) {
            return Literal.NULL;
        }

        return Literal.newLiteral(info.returnType(), evaluate(inputValue, indexValue));
    }

    @Override
    public Object evaluate(Input[] args) {
        assert args.length == 2 : "invalid number of arguments";
        return evaluate(args[0].value(), args[1].value());
    }

    private Object evaluate(Object element, Object index) {
        if (element == null || index == null) {
            return null;
        }
        assert (element instanceof Object[] || element instanceof List)
                : "first argument must be of type array or list";
        assert index instanceof Integer : "second argument must be of type integer";

        // 1 based arrays as SQL standard says
        Integer idx = (Integer) index - 1;
        try {
            if (element instanceof List) {
                return ((List) element).get(idx);
            }
            return ((Object[]) element)[idx];
        } catch (IndexOutOfBoundsException e) {
            return null;
        }
    }

    @Override
    public FunctionImplementation<Function> getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
        Preconditions.checkArgument(dataTypes.size() == 2
                && DataTypes.isCollectionType(dataTypes.get(0))
                && dataTypes.get(1) == DataTypes.INTEGER);
        DataType returnType = ((CollectionType)dataTypes.get(0)).innerType();
        return new SubscriptFunction(createInfo(dataTypes, returnType));
    }


}
