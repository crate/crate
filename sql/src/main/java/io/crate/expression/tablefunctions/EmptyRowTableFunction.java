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
import io.crate.types.DataType;
import io.crate.types.ObjectType;

import java.util.List;

/**
 * Generates a one row, no column, empty table.
 */
public class EmptyRowTableFunction {

    private static final String NAME = "empty_row";

    static class EmptyRowTableFunctionImplementation extends TableFunctionImplementation<Object> {

        private final FunctionInfo info;

        private EmptyRowTableFunctionImplementation(FunctionInfo info) {
            this.info = info;
        }

        @Override
        public FunctionInfo info() {
            return info;
        }

        @Override
        @SafeVarargs
        public final Iterable<Row> evaluate(TransactionContext txnCtx, Input<Object>... args) {
            return List.of(Row.EMPTY);
        }

        @Override
        public ObjectType returnType() {
            return ObjectType.untyped();
        }
    }

    public static void register(TableFunctionModule module) {
        module.register(NAME, new BaseFunctionResolver(FuncParams.NONE) {

            @Override
            public FunctionImplementation getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
                return new EmptyRowTableFunctionImplementation(
                    new FunctionInfo(new FunctionIdent(NAME, dataTypes), ObjectType.untyped(), FunctionInfo.Type.TABLE));
            }
        });
    }
}
