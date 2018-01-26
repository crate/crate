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
import io.crate.metadata.BaseFunctionResolver;
import io.crate.metadata.functions.params.FuncParams;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.metadata.functions.params.Param;
import io.crate.types.CollectionType;
import io.crate.types.DataType;

import java.util.List;

import static io.crate.metadata.functions.params.Param.INTEGER;

public class SubscriptFunction extends Scalar<Object, Object[]> {

    public static final String NAME = "subscript";
    private FunctionInfo info;

    public static void register(ScalarFunctionModule module) {
        module.register(NAME, new Resolver());
    }

    private SubscriptFunction(FunctionInfo info) {
        this.info = info;
    }

    @Override
    public FunctionInfo info() {
        return info;
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

    private static class Resolver extends BaseFunctionResolver {

        private static FunctionInfo createInfo(List<DataType> argumentTypes, DataType returnType) {
            return new FunctionInfo(new FunctionIdent(NAME, argumentTypes), returnType);
        }

        protected Resolver() {
            super(FuncParams.builder(Param.ANY_ARRAY, INTEGER).build());
        }

        @Override
        public FunctionImplementation getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
            DataType returnType = ((CollectionType) dataTypes.get(0)).innerType();
            return new SubscriptFunction(createInfo(dataTypes, returnType));
        }
    }
}
