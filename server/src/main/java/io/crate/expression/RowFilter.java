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

package io.crate.expression;

import java.util.List;
import java.util.function.Predicate;

import org.jetbrains.annotations.Nullable;

import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.TransactionContext;

public class RowFilter implements Predicate<Row> {

    private final Input<Boolean> filterCondition;
    private final List<CollectExpression<Row, ?>> expressions;

    public static Predicate<Row> create(TransactionContext txnCtx, InputFactory inputFactory, @Nullable Symbol filterSymbol) {
        if (filterSymbol == null) {
            return i -> true;
        }
        return new RowFilter(txnCtx, inputFactory, filterSymbol);
    }

    @SuppressWarnings("unchecked")
    private RowFilter(TransactionContext txnCtx, InputFactory inputFactory, Symbol filterSymbol) {
        InputFactory.Context<CollectExpression<Row, ?>> ctx = inputFactory.ctxForInputColumns(txnCtx);
        filterCondition = (Input<Boolean>) ctx.add(filterSymbol);
        expressions = ctx.expressions();
    }

    @Override
    public boolean test(@Nullable Row row) {
        //noinspection ForLoopReplaceableByForEach // avoids iterator allocation - rowFilter test is invoked per row
        for (int i = 0, expressionsSize = expressions.size(); i < expressionsSize; i++) {
            CollectExpression<Row, ?> expression = expressions.get(i);
            expression.setNextRow(row);
        }
        return InputCondition.matches(filterCondition);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RowFilter rowFilter = (RowFilter) o;

        if (!filterCondition.equals(rowFilter.filterCondition)) return false;
        return expressions.equals(rowFilter.expressions);
    }

    @Override
    public int hashCode() {
        int result = filterCondition.hashCode();
        result = 31 * result + expressions.hashCode();
        return result;
    }
}
