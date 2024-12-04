/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.expression.tablefunctions;

import static io.crate.metadata.functions.TypeVariableConstraint.typeVariableOfAnyType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.metadata.FunctionType;
import io.crate.metadata.Functions;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Scalar.Feature;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.metadata.tablefunctions.TableFunctionImplementation;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.RowType;
import io.crate.types.TypeSignature;

public class ValuesFunction {

    public static final String NAME = "_values";

    public static final Signature SIGNATURE = Signature.builder(NAME, FunctionType.TABLE)
        .argumentTypes(TypeSignature.parse("array(E)"))
        .returnType(RowType.EMPTY.getTypeSignature())
        .typeVariableConstraints(typeVariableOfAnyType("E"))
        .features(Feature.DETERMINISTIC)
        .setVariableArity(true)
        .build();

    public static void register(Functions.Builder builder) {
        builder.add(SIGNATURE, ValuesTableFunctionImplementation::of);
    }

    private static class ValuesTableFunctionImplementation extends TableFunctionImplementation<List<Object>> {

        private final RowType returnType;

        private static ValuesTableFunctionImplementation of(Signature signature, BoundSignature boundSignature) {
            var argTypes = boundSignature.argTypes();
            ArrayList<DataType<?>> fieldTypes = new ArrayList<>(argTypes.size());
            for (int i = 0; i < argTypes.size(); i++) {
                DataType<?> dataType = argTypes.get(i);
                assert dataType instanceof ArrayType : "Arguments to _values must be of type array";

                fieldTypes.add(((ArrayType<?>) dataType).innerType());
            }
            RowType returnType = new RowType(fieldTypes);
            return new ValuesTableFunctionImplementation(signature, argTypes, returnType);
        }

        private ValuesTableFunctionImplementation(Signature signature, List<DataType<?>> argTypes, RowType returnType) {
            super(signature, new BoundSignature(argTypes, returnType));
            this.returnType = returnType;
        }

        @Override
        @SafeVarargs
        public final Iterable<Row> evaluate(TransactionContext txnCtx,
                                            NodeContext nodeCtx,
                                            Input<List<Object>> ... arguments) {
            return new ColumnOrientedRowsIterator(() -> iteratorsFrom(arguments));
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
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
        public RowType returnType() {
            return returnType;
        }

        @Override
        public boolean hasLazyResultSet() {
            return false;
        }
    }
}
