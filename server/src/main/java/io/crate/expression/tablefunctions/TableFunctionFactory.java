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

import java.util.List;
import java.util.Locale;

import io.crate.common.collections.Lists2;
import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.Signature;
import io.crate.metadata.tablefunctions.TableFunctionImplementation;
import io.crate.types.RowType;
import io.crate.types.TypeSignature;

public class TableFunctionFactory {

    public static TableFunctionImplementation<?> from(FunctionImplementation functionImplementation) {
        TableFunctionImplementation<?> tableFunction;
        switch (functionImplementation.signature().getKind()) {
            case TABLE:
                tableFunction = (TableFunctionImplementation<?>) functionImplementation;
                break;
            case SCALAR:
                tableFunction = new ScalarTableFunctionImplementation<>((Scalar<?, ?>) functionImplementation);
                break;
            case WINDOW:
            case AGGREGATE:
                throw new UnsupportedOperationException(
                    String.format(
                        Locale.ENGLISH,
                        "Window or Aggregate function: '%s' is not allowed in function in FROM clause",
                        functionImplementation.signature().getName().displayName()));
            default:
                throw new UnsupportedOperationException(
                    String.format(
                        Locale.ENGLISH,
                        "Unknown type function: '%s' is not allowed in function in FROM clause",
                        functionImplementation.signature().getName().displayName()));
        }
        return tableFunction;
    }

    /**
     * Evaluates the {@link Scalar} function and emits scalar result as a 1x1 table
     */
    private static class ScalarTableFunctionImplementation<T> extends TableFunctionImplementation<T> {

        private final Scalar<?, T> functionImplementation;
        private final RowType returnType;
        private final Signature signature;
        private final Signature boundSignature;

        private ScalarTableFunctionImplementation(Scalar<?, T> functionImplementation) {
            this.functionImplementation = functionImplementation;
            FunctionInfo info = functionImplementation.info();
            returnType = new RowType(List.of(info.returnType()), List.of(info.ident().name()));
            signature = Signature.table(
                functionImplementation.signature().getName(),
                Lists2.concat(
                    functionImplementation.signature().getArgumentTypes(),
                    functionImplementation.signature().getReturnType()
                ).toArray(new TypeSignature[0])
            );
            boundSignature = Signature.table(
                functionImplementation.boundSignature().getName(),
                Lists2.concat(
                    functionImplementation.boundSignature().getArgumentTypes(),
                    functionImplementation.boundSignature().getReturnType()
                ).toArray(new TypeSignature[0])
            );
        }

        @Override
        public Signature signature() {
            return signature;
        }

        @Override
        public Signature boundSignature() {
            return boundSignature;
        }

        @Override
        public Iterable<Row> evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input<T>[] args) {
            return List.of(new Row1(functionImplementation.evaluate(txnCtx, nodeCtx, args)));
        }

        @Override
        public RowType returnType() {
            return returnType;
        }

        @Override
        public boolean hasLazyResultSet() {
            return true;
        }
    }
}
