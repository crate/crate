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

import java.util.List;

import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.expression.symbol.FuncArg;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.FunctionResolver;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.RowType;

public final class SubscriptRecordFunction extends Scalar<Object, Object> {

    public static final String NAME = "_subscript_record";

    public static void register(ScalarFunctionModule module) {
        module.register(
            NAME,
            new FunctionResolver() {

                @Override
                public List<DataType> getSignature(List<? extends FuncArg> funcArgs) {
                    if (funcArgs.size() < 2) {
                        return null;
                    }
                    var firstArg = funcArgs.get(0);
                    if (firstArg.valueType().id() != RowType.ID) {
                        return null;
                    }
                    var secondArg = funcArgs.get(1);
                    if (secondArg.valueType().id() != DataTypes.STRING.id()) {
                        return null;
                    }
                    return List.of(firstArg.valueType(), secondArg.valueType());
                }

                @Override
                public FunctionImplementation getForTypes(List<DataType> args) throws IllegalArgumentException {
                    return new SubscriptRecordFunction((RowType) args.get(0));
                }
            }
        );
    }


    private final RowType rowType;
    private final FunctionInfo info;


    public SubscriptRecordFunction(RowType rowType) {
        this.rowType = rowType;
        this.info = new FunctionInfo(
            new FunctionIdent(NAME, List.of(rowType, DataTypes.STRING)),
            DataTypes.UNDEFINED
        );
    }

    @Override
    public FunctionInfo info() {
        return info;
    }


    @Override
    @SafeVarargs
    public final Object evaluate(TransactionContext txnCtx, Input<Object>... args) {
        Row record = (Row) args[0].value();
        if (record == null) {
            return null;
        }
        String fieldName = (String) args[1].value();
        int idx = rowType.fieldNames().indexOf(fieldName);
        if (idx < 0) {
            // The ExpressionAnalyzer should prevent this case
            throw new IllegalStateException("Couldn't find fieldname `" + fieldName + "` within RowType `" + rowType + "`");
        }
        return record.get(idx);
    }
}
