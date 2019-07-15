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

package io.crate.expression.scalar.arithmetic;

import io.crate.data.Input;
import io.crate.expression.scalar.ScalarFunctionModule;
import io.crate.expression.symbol.FuncArg;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.FunctionResolver;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.params.FuncParams;
import io.crate.types.ArrayType;
import io.crate.types.DataType;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

public class ArrayFunction extends Scalar<Object, Object> {

    public static final String NAME = "_array";
    private final FunctionInfo info;

    public static FunctionInfo createInfo(List<DataType> argumentTypes) {
        DataType<?> innerType = argumentTypes.get(0);
        return new FunctionInfo(new FunctionIdent(NAME, argumentTypes), new ArrayType<>(innerType));
    }

    private static final FunctionResolver RESOLVER = new FunctionResolver() {

        @Override
        public FunctionImplementation getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
            return new ArrayFunction(createInfo(dataTypes));
        }

        @Nullable
        @Override
        public List<DataType> getSignature(List<? extends FuncArg> symbols) {
            return FuncParams.ANY_VAR_ARGS_SAME_TYPE.match(symbols);
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
    public final Object evaluate(TransactionContext txnCtx, Input<Object>... args) {
        ArrayList<Object> values = new ArrayList<>(args.length);
        for (Input<Object> arg : args) {
            values.add(arg.value());
        }
        return values;
    }

    public static void register(ScalarFunctionModule scalarFunctionModule) {
        scalarFunctionModule.register(NAME, RESOLVER);
    }
}
