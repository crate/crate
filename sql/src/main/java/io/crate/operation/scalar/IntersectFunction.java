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
import io.crate.metadata.*;
import io.crate.operation.Input;
import io.crate.planner.DataTypeVisitor;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Symbol;
import io.crate.types.ArrayType;
import io.crate.types.DataType;

import java.util.*;

public class IntersectFunction implements Scalar<Object[], Object> {

    public static final String NAME = "intersection";
    private final static Object[] EMPTY = new Object[0];
    private final static Comparator<Object> comparator =  new NullComparator();

    public static void register(ScalarFunctionModule module) {
        module.register(NAME, new Resolver());
    }

    private final FunctionInfo info;

    IntersectFunction(FunctionInfo info) {
        this.info = info;
    }

    @Override
    public Object[] evaluate(Input[] args) {
        Object arg1 = args[0].value();
        if (arg1 == null) {
            return null;
        }
        Object arg2 = args[1].value();
        if (arg2 == null) {
            return null;
        }

        Object[] values1;
        Object[] values2;
        if (arg1 instanceof List) {
            values1 = ((List) arg1).toArray();
        } else {
            values1 = (Object[]) arg1;
        }
        if (arg2 instanceof List) {
            values2 = ((List) arg2).toArray();
        } else {
            values2 = ((Object[]) arg2);
        }

        Arrays.sort(values1, comparator);
        Arrays.sort(values2, comparator);

        Object lastItem = null;
        Object[] result = new Object[Math.min(values1.length, values2.length)];
        int idx = 0;
        for (Object o : values2) {
            if (lastItem != null && lastItem.equals(o)) {
                lastItem = o;
                continue;
            }
            if (Arrays.binarySearch(values1, o, comparator) >= 0) {
                result[idx] = o;
                idx++;
            }
            lastItem = o;
        }

        if (idx == 0) {
            return EMPTY;
        } else if (result.length > idx) {
            return Arrays.copyOf(result, idx);
        } else {
            return result;
        }
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    @Override
    public Symbol normalizeSymbol(Function symbol) {
        Symbol arg1 = symbol.arguments().get(0);
        Symbol arg2 = symbol.arguments().get(1);

        if (arg1.symbolType().isValueSymbol() && arg2.symbolType().isValueSymbol()) {
            return Literal.newLiteral(
                    DataTypeVisitor.fromSymbol(arg1),
                    evaluate(new Input[] { (Input) arg1, (Input) arg2 })
            );
        }
        return symbol;
    }


    static class Resolver implements DynamicFunctionResolver {

        @Override
        public FunctionImplementation<Function> getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
            Preconditions.checkArgument(dataTypes.size() == 2, "%s requires 2 arguments", NAME);

            DataType innerType = null;
            for (int i = 0, dataTypesSize = dataTypes.size(); i < dataTypesSize; i++) {
                DataType type = dataTypes.get(i);
                if (!(type instanceof ArrayType)) {
                    throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                            "Argument %d of %s must be an array, not %s", i + 1, NAME, type.getName()));
                }
                if (innerType != null) {
                    if (! (((ArrayType) type).innerType().equals(innerType))) {
                        throw new IllegalArgumentException(String.format(
                                "Inner types of arrays in %s must match", NAME));
                    }
                }

                innerType = ((ArrayType) type).innerType();
            }

            return new IntersectFunction(new FunctionInfo(new FunctionIdent(NAME, dataTypes), dataTypes.get(0)));
        }
    }

    private static class NullComparator implements java.util.Comparator<Object> {

        @Override
        @SuppressWarnings("unchecked")
        public int compare(Object o1, Object o2) {
            if (o1 == null && o2 == null) {
                return 0;
            } else if (o1 == null) {
                return -1;
            } else if (o2 == null) {
                return 1;
            }
            return ((Comparable) o1).compareTo(o2);
        }
    }
}
