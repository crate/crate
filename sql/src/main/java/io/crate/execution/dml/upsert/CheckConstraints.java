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

package io.crate.execution.dml.upsert;

import io.crate.data.Input;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.expression.InputFactory;
import io.crate.expression.reference.ReferenceResolver;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.Reference;
import io.crate.metadata.doc.DocTableInfo;

import java.util.ArrayList;
import java.util.List;

public final class CheckConstraints<T, E extends CollectExpression<T, ?>> {

    private final List<Input<?>> inputs = new ArrayList<>();
    private final List<E> expressions;
    private final List<ColumnIdent> notNullColumns;

    public CheckConstraints(TransactionContext txnCtx,
                            InputFactory inputFactory,
                            ReferenceResolver<E> refResolver,
                            DocTableInfo table) {
        InputFactory.Context<E> ctx = inputFactory.ctxForRefs(txnCtx, refResolver);
        notNullColumns = new ArrayList<>(table.notNullColumns());
        for (int i = 0; i < notNullColumns.size(); i++) {
            Reference notNullRef = table.getReference(notNullColumns.get(i));
            inputs.add(ctx.add(notNullRef));
        }
        expressions = ctx.expressions();
    }

    public void validate(T values) {
        for (int i = 0; i < expressions.size(); i++) {
            expressions.get(i).setNextRow(values);
        }
        for (int i = 0; i < inputs.size(); i++) {
            Object val = inputs.get(i).value();
            if (val == null) {
                throw new IllegalArgumentException("\"" + notNullColumns.get(i) + "\" must not be null");
            }
        }
    }
}
