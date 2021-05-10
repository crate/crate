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

package io.crate.execution.engine.sort;

import com.google.common.collect.Ordering;
import io.crate.analyze.OrderBy;
import io.crate.data.ArrayRow;
import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.expression.ExpressionsInput;
import io.crate.expression.InputFactory;
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.Symbol;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.Supplier;

import static io.crate.execution.engine.sort.OrderingByPosition.arrayOrdering;

public final class Comparators {

    @Nullable
    public static <T extends CollectExpression<Row, ?>> Comparator<Object[]> createComparator(
        Supplier<InputFactory.Context<T>> createInputFactoryCtx,
        @Nullable OrderBy orderBy) {

        if (orderBy == null) {
            return null;
        }
        List<Symbol> orderBySymbols = orderBy.orderBySymbols();
        int[] positions = new int[orderBySymbols.size()];
        for (int i = 0; i < orderBySymbols.size(); i++) {
            Symbol symbol = orderBySymbols.get(i);
            if (symbol instanceof InputColumn) {
                positions[i] = ((InputColumn) symbol).index();
            } else {
                return createComparatorWithEval(createInputFactoryCtx, orderBy);
            }
        }
        return arrayOrdering(positions, orderBy.reverseFlags(), orderBy.nullsFirst());
    }

    private static <T extends CollectExpression<Row, ?>> Comparator<Object[]> createComparatorWithEval(
        Supplier<InputFactory.Context<T>> createInputFactoryCtx,
        OrderBy orderBy) {

        var orderBySymbols = orderBy.orderBySymbols();
        ArrayList<Comparator<Object[]>> comparators = new ArrayList<>(orderBySymbols.size());
        for (int i = 0; i < orderBySymbols.size(); i++) {
            var orderSymbol = orderBySymbols.get(i);
            var ctx = createInputFactoryCtx.get();
            var input = (Input<Object>) ctx.add(orderSymbol);
            var expressionsInput = new ExpressionsInput<>(input, ctx.expressions());
            var row = new ArrayRow();
            comparators.add(new NullAwareComparator<>(
                cells -> {
                    Comparable<Object> value;
                    synchronized (row) {
                        row.cells(cells);
                        value = (Comparable<Object>) expressionsInput.value(row);
                    }
                    return value;
                },
                orderBy.reverseFlags()[i],
                orderBy.nullsFirst()[i]
            ));
        }
        return Ordering.compound(comparators);
    }
}
