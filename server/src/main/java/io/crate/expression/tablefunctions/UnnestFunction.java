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

import com.google.common.collect.Iterators;
import io.crate.common.collections.Lists2;
import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.Signature;
import io.crate.metadata.tablefunctions.TableFunctionImplementation;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.RowType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static io.crate.metadata.functions.TypeVariableConstraint.typeVariableOfAnyType;
import static io.crate.types.TypeSignature.parseTypeSignature;

public class UnnestFunction {

    public static final String NAME = "unnest";

    public static void register(TableFunctionModule module) {
        module.register(
            Signature
                .table(
                    NAME,
                    parseTypeSignature("array(E)"),
                    RowType.EMPTY.getTypeSignature())
                .withTypeVariableConstraints(typeVariableOfAnyType("E"))
                .withVariableArity(),
            UnnestTableFunctionImplementation::new
        );
        // unnest() to keep it compatible with previous versions
        module.register(
            Signature.table(
                NAME,
                RowType.EMPTY.getTypeSignature()),
            UnnestTableFunctionImplementation::new
        );
    }

    static class UnnestTableFunctionImplementation extends TableFunctionImplementation<List<Object>> {

        private final FunctionInfo info;
        private final RowType returnType;
        private final Signature signature;

        private UnnestTableFunctionImplementation(Signature signature, List<DataType> argTypes) {
            this.signature = signature;
            ArrayList<DataType<?>> fieldTypes = new ArrayList<>(argTypes.size());
            for (int i = 0; i < argTypes.size(); i++) {
                DataType<?> dataType = argTypes.get(i);
                fieldTypes.add(ArrayType.unnest(dataType));
            }
            this.returnType = new RowType(fieldTypes);
            this.info = new FunctionInfo(
                new FunctionIdent(null, NAME, argTypes),
                returnType.numElements() == 1
                    ? returnType.getFieldType(0)
                    : returnType,
                FunctionInfo.Type.TABLE
            );
        }

        @Override
        public FunctionInfo info() {
            return info;
        }

        @Override
        public Signature signature() {
            return signature;
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
         *
         * @param arguments collection of array-literals
         *                  e.g. [ [1, 2], [Marvin, Trillian] ]
         * @return Bucket containing the unnested rows.
         * [ [1, Marvin], [2, Trillian] ]
         */
        @SafeVarargs
        @Override
        public final Iterable<Row> evaluate(TransactionContext txnCtx, Input<List<Object>>... arguments) {
            ArrayList<List<Object>> valuesPerColumn = new ArrayList<>(arguments.length);
            for (Input<List<Object>> argument : arguments) {
                valuesPerColumn.add(argument.value());
            }
            return new ColumnOrientedRowsIterator(() -> createIterators(valuesPerColumn));
        }

        private Iterator<Object>[] createIterators(ArrayList<List<Object>> valuesPerColumn) {
            Iterator[] iterators = new Iterator[valuesPerColumn.size()];
            for (int i = 0; i < valuesPerColumn.size(); i++) {
                DataType<?> dataType = info.ident().argumentTypes().get(i);
                assert dataType instanceof ArrayType : "Argument to unnest must be an array";
                iterators[i] = createIterator(valuesPerColumn.get(i), (ArrayType<?>) dataType);
            }
            //noinspection unchecked
            return iterators;
        }

        private static Iterator<Object> createIterator(List<Object> objects, ArrayType<?> type) {
            if (objects == null) {
                return Collections.emptyIterator();
            }
            if (type.innerType() instanceof ArrayType) {
                @SuppressWarnings("unchecked")
                List<Iterator<Object>> iterators = Lists2.map(
                    objects,
                    x -> createIterator((List<Object>) x, (ArrayType<?>) type.innerType())
                );
                return Iterators.concat(iterators.iterator());
            } else {
                return objects.iterator();
            }
        }
    }
}
