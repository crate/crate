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

import static io.crate.metadata.functions.TypeVariableConstraint.typeVariable;
import static io.crate.metadata.functions.TypeVariableConstraint.typeVariableOfAnyType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import io.crate.common.collections.Iterators;
import io.crate.common.collections.Lists2;
import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.legacy.LegacySettings;
import io.crate.metadata.NodeContext;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.metadata.tablefunctions.TableFunctionImplementation;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.RowType;
import io.crate.types.TypeSignature;

public class UnnestFunction {

    public static final String NAME = "unnest";

    public static void register(TableFunctionModule module) {
        module.register(
            Signature
                .table(
                    NAME,
                    TypeSignature.parse("array(E)"),
                    TypeSignature.parse("E")
                )
                .withTypeVariableConstraints(typeVariable("E")),
            (signature, boundSignature) -> new UnnestTableFunctionImplementation(
                signature,
                boundSignature,
                LegacySettings.LEGACY_TABLE_FUNCTION_COLUMN_NAMING.get(module.settings()) ?
                    new RowType(List.of(boundSignature.returnType())) :
                    new RowType(List.of(boundSignature.returnType()), List.of(NAME)))
        );
        module.register(
            Signature
                .table(
                    NAME,
                    TypeSignature.parse("array(E)"),
                    TypeSignature.parse("array(N)"),
                    RowType.EMPTY.getTypeSignature()
                )
                .withTypeVariableConstraints(typeVariable("E"), typeVariableOfAnyType("N"))
                .withVariableArity(),
            (signature, boundSignature) -> {
                var argTypes = boundSignature.argTypes();
                ArrayList<DataType<?>> fieldTypes = new ArrayList<>(argTypes.size());
                for (int i = 0; i < argTypes.size(); i++) {
                    DataType<?> dataType = argTypes.get(i);
                    fieldTypes.add(ArrayType.unnest(dataType));
                }
                var returnType = new RowType(fieldTypes);
                // the return type of the bound signature has to be resolved
                // and created explicitly based on the arguments of the bounded
                // signature, such as we cannot resolve correctly the field types
                // of the record type from the array of any type with variable arity.
                return new UnnestTableFunctionImplementation(
                    signature,
                    new BoundSignature(boundSignature.argTypes(), returnType),
                    returnType
                );
            }
        );
        // unnest() to keep it compatible with previous versions
        module.register(
            Signature.table(
                NAME,
                DataTypes.UNTYPED_OBJECT.getTypeSignature()
            ),
            (signature, boundSignature) -> new UnnestTableFunctionImplementation(
                signature,
                boundSignature,
                RowType.EMPTY)
        );
    }

    static class UnnestTableFunctionImplementation extends TableFunctionImplementation<List<Object>> {

        private final RowType returnType;
        private final List<DataType<?>> argumentTypes;

        private UnnestTableFunctionImplementation(Signature signature,
                                                  BoundSignature boundSignature,
                                                  RowType returnType) {
            super(signature, boundSignature);
            this.argumentTypes = boundSignature.argTypes();
            this.returnType = returnType;
        }

        @Override
        public RowType returnType() {
            return returnType;
        }

        @Override
        public boolean hasLazyResultSet() {
            return false;
        }

        /**
         * @param arguments collection of array-literals
         *                  e.g. [ [1, 2], [Marvin, Trillian] ]
         * @return Bucket containing the unnested rows.
         * [ [1, Marvin], [2, Trillian] ]
         */
        @SafeVarargs
        @Override
        public final Iterable<Row> evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input<List<Object>>... arguments) {
            ArrayList<List<Object>> valuesPerColumn = new ArrayList<>(arguments.length);
            for (Input<List<Object>> argument : arguments) {
                valuesPerColumn.add(argument.value());
            }
            return new ColumnOrientedRowsIterator(() -> createIterators(valuesPerColumn));
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        private Iterator<Object>[] createIterators(ArrayList<List<Object>> valuesPerColumn) {
            Iterator[] iterators = new Iterator[valuesPerColumn.size()];
            for (int i = 0; i < valuesPerColumn.size(); i++) {
                DataType<?> dataType = argumentTypes.get(i);
                assert dataType instanceof ArrayType : "Argument to unnest must be an array";
                iterators[i] = createIterator(valuesPerColumn.get(i), (ArrayType<?>) dataType);
            }
            return iterators;
        }

        @SuppressWarnings("unchecked")
        private static Iterator<Object> createIterator(List<Object> objects, ArrayType<?> type) {
            if (objects == null) {
                return Collections.emptyIterator();
            }
            if (type.innerType() instanceof ArrayType) {
                List<Iterator<Object>> iterators = Lists2.map(
                    objects,
                    x -> createIterator((List<Object>) x, (ArrayType<?>) type.innerType())
                );
                return Iterators.concat(iterators.toArray(new Iterator[0]));
            } else {
                return objects.iterator();
            }
        }
    }
}
