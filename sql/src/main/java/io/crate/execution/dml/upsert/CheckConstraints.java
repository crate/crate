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

import io.crate.common.collections.Lists2;
import io.crate.data.Input;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.expression.InputFactory;
import io.crate.expression.reference.ReferenceResolver;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.sql.tree.CheckConstraint;
import org.elasticsearch.common.collect.Tuple;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public final class CheckConstraints<T, E extends CollectExpression<T, ?>> {

    private final List<Input<?>> inputs = new ArrayList<>();
    private final List<E> expressions;
    private final List<ColumnIdent> notNullColumns;
    private final List<Tuple<? extends Input<?>, CheckConstraint<Symbol>>> checkConstraints;

    CheckConstraints(TransactionContext txnCtx,
                     InputFactory inputFactory,
                     ReferenceResolver<E> refResolver,
                     DocTableInfo table) {
        InputFactory.Context<E> ctx = inputFactory.ctxForRefs(txnCtx, refResolver);
        notNullColumns = new ArrayList<>(table.notNullColumns());
        for (int i = 0; i < notNullColumns.size(); i++) {
            ColumnIdent columnIdent = notNullColumns.get(i);
            Reference notNullRef = table.getReadReference(columnIdent);
            assert notNullRef != null
                : "ColumnIdent retrieved via `table.notNullColumns` must be available via `table.getReadReference`";
            inputs.add(ctx.add(notNullRef));
        }
        expressions = ctx.expressions();
        checkConstraints = Lists2.map(table.checkConstraints(), chk -> new Tuple<>(ctx.add(chk.expression()), chk));
    }

    /**
     * The method must be called on {@code T values}, e.g. source maps,
     * rows that already contain values of evaluated generated column
     * expressions.
     */
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
        for (int i = 0; i < checkConstraints.size(); i++) {
            Tuple<? extends Input<?>, CheckConstraint<Symbol>> checkEntry = checkConstraints.get(i);
            Input<?> checkInput = checkEntry.v1();
            Boolean value = (Boolean) checkInput.value();
            if (value == null) {
                // SQL semantics: If a column is omitted from an INSERT/UPDATE statement,
                // CHECK constraints should not fail. Same for writing explicit `null` values.
                continue;
            }
            if (!value.booleanValue()) {
                CheckConstraint<Symbol> chk = checkEntry.v2();
                throw new IllegalArgumentException(String.format(
                    Locale.ENGLISH,
                    "Failed CONSTRAINT %s CHECK (%s) and values %s",
                    chk.name(), chk.expressionStr(), values));
            }
        }
    }
}
