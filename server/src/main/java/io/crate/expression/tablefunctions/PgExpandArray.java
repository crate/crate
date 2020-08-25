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
import io.crate.data.RowN;
import io.crate.metadata.FunctionName;
import io.crate.metadata.NodeContext;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.Signature;
import io.crate.metadata.information.InformationSchemaInfo;
import io.crate.metadata.tablefunctions.TableFunctionImplementation;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;
import io.crate.types.RowType;

import java.util.List;
import java.util.function.Function;

import static io.crate.metadata.functions.TypeVariableConstraint.typeVariable;
import static io.crate.types.TypeSignature.parseTypeSignature;

public final class PgExpandArray extends TableFunctionImplementation<List<Object>> {

    private static final String NAME = "_pg_expandarray";
    private static final FunctionName FUNCTION_NAME = new FunctionName(InformationSchemaInfo.NAME, NAME);

    public static void register(TableFunctionModule module) {
        module.register(
            Signature.table(
                FUNCTION_NAME,
                parseTypeSignature("array(E)"),
                parseTypeSignature("record(x E, n integer)")
            ).withTypeVariableConstraints(typeVariable("E")),
            PgExpandArray::new
        );
    }

    private final Signature signature;
    private final Signature boundSignature;
    private final RowType resultType;

    public PgExpandArray(Signature signature, Signature boundSignature) {
        this.signature = signature;
        this.boundSignature = boundSignature;
        ArrayType<?> argType = (ArrayType<?>) boundSignature.getArgumentDataTypes().get(0);
        resultType = new RowType(
            List.of(argType.innerType(), DataTypes.INTEGER),
            List.of("x", "n")
        );
    }

    @Override
    @SafeVarargs
    public final Iterable<Row> evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input<List<Object>>... args) {
        List<Object> values = args[0].value();
        if (values == null) {
            return List.of();
        }
        return () -> values.stream()
            .map(new Function<Object, Row>() {

                final Object[] columns = new Object[2];
                final RowN row = new RowN(columns);

                int idx = 0;

                @Override
                public Row apply(Object val) {
                    idx++;
                    columns[0] = val;
                    columns[1] = idx;
                    return row;
                }
            }).iterator();
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
    public RowType returnType() {
        return resultType;
    }

    @Override
    public boolean hasLazyResultSet() {
        return true;
    }
}
