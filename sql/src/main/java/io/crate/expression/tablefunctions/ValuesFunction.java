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

package io.crate.expression.tablefunctions;

import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.metadata.BaseFunctionResolver;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.params.FuncParams;
import io.crate.metadata.tablefunctions.TableFunctionImplementation;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.ObjectType;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static io.crate.metadata.functions.params.Param.ANY_ARRAY;

public class ValuesFunction {

    public static final String NAME = "_values";

    private static class ValuesTableFunctionImplementation extends TableFunctionImplementation<List<Object>> {

        private final FunctionInfo info;
        private final ObjectType returnType;

        private ValuesTableFunctionImplementation(List<DataType> argTypes) {
            ObjectType.Builder objTypeBuilder = ObjectType.builder();
            for (int i = 0; i < argTypes.size(); i++) {
                DataType<?> dataType = argTypes.get(i);
                assert dataType instanceof ArrayType : "Arguments to _values must be of type array";

                objTypeBuilder.setInnerType("col" + (i + 1), ((ArrayType<?>) dataType).innerType());
            }
            ObjectType objectType = objTypeBuilder.build();
            this.info = new FunctionInfo(
                new FunctionIdent(NAME, argTypes),
                objectType.innerTypes().size() == 1
                    ? objectType.innerTypes().values().iterator().next()
                    : objectType,
                FunctionInfo.Type.TABLE
            );
            this.returnType = objectType;
        }

        @Override
        public FunctionInfo info() {
            return info;
        }

        @Override
        public final Iterable<Row> evaluate(TransactionContext txnCtx,
                                            Input<List<Object>>[] arguments) {
            return new ColumnOrientedRowsIterator(() -> iteratorsFrom(arguments));
        }

        private Iterator<Object>[] iteratorsFrom(Input<List<Object>>[] arguments) {
            Iterator[] iterators = new Iterator[arguments.length];
            for (int i = 0; i < arguments.length; i++) {
                var argument = arguments[i].value();
                if (argument == null) {
                    iterators[i] = Collections.emptyIterator();
                } else {
                    iterators[i] = argument.iterator();
                }
            }
            //noinspection unchecked
            return iterators;
        }

        @Override
        public ObjectType returnType() {
            return returnType;
        }
    }

    public static void register(TableFunctionModule module) {
        module.register(NAME, new BaseFunctionResolver(
            FuncParams.builder().withIndependentVarArgs(ANY_ARRAY).build()) {

            @Override
            public FunctionImplementation getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
                return new ValuesTableFunctionImplementation(dataTypes);
            }
        });
    }
}
