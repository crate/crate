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

package io.crate.operation.scalar.arithmetic;

import io.crate.metadata.*;
import io.crate.operation.Input;
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

import java.util.Iterator;
import java.util.List;

public class ArrayFunction extends Scalar<Object, Object> {

    private final static String NAME = "_array";
    private final FunctionInfo info;

    public static FunctionInfo createInfo(List<DataType> argumentTypes) {
        if (argumentTypes.isEmpty()) {
            throw new IllegalArgumentException("Cannot determine type of empty array");
        }
        Iterator<DataType> it = argumentTypes.iterator();
        DataType firstType = it.next();
        while (it.hasNext()) {
            DataType next = it.next();
            if (next.equals(DataTypes.UNDEFINED)) {
                continue;
            }
            if (firstType.equals(DataTypes.UNDEFINED)) {
                firstType = next;
            } else if (!firstType.equals(next)) {
                throw new IllegalArgumentException("All arguments to an array must have the same type. " +
                                                   "Found " + firstType.getName() + " and " + next.getName());
            }
        }
        return new FunctionInfo(new FunctionIdent(NAME, argumentTypes), new ArrayType(firstType));
    }

    private static final DynamicFunctionResolver RESOLVER = new DynamicFunctionResolver() {

        @Override
        public FunctionImplementation getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
            return new ArrayFunction(createInfo(dataTypes));
        }
    };

    private ArrayFunction(FunctionInfo info) {
        this.info = info;
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    @SafeVarargs
    @Override
    public final Object evaluate(Input<Object>... args) {
        Object[] values = new Object[args.length];
        for (int i = 0; i < values.length; i++) {
            values[i] = args[i].value();
        }
        return values;
    }

    public static void register(ScalarFunctionModule scalarFunctionModule) {
        scalarFunctionModule.register(NAME, RESOLVER);
    }
}
