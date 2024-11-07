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

package io.crate.analyze.relations;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import org.jetbrains.annotations.Nullable;

import io.crate.analyze.OrderBy;
import io.crate.expression.symbol.Symbol;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.SortItem;

public final class OrderyByAnalyzer {

    private OrderyByAnalyzer() {}

    @Nullable
    public static OrderBy analyzeSortItems(List<SortItem> sortItems,
                                           Function<Expression, Symbol> expressionToSymbolFunction) {
        int size = sortItems.size();
        if (size == 0) {
            return null;
        }
        List<Symbol> symbols = new ArrayList<>(size);
        boolean[] reverseFlags = new boolean[size];
        boolean[] nullsFirst = new boolean[size];

        for (int i = 0; i < size; i++) {
            SortItem sortItem = sortItems.get(i);
            Expression sortKey = sortItem.getSortKey();
            Symbol symbol = expressionToSymbolFunction.apply(sortKey);
            symbols.add(symbol);
            switch (sortItem.getNullOrdering()) {
                case FIRST:
                    nullsFirst[i] = true;
                    break;
                case LAST:
                    nullsFirst[i] = false;
                    break;
                case UNDEFINED:
                    nullsFirst[i] = sortItem.getOrdering() == SortItem.Ordering.DESCENDING;
                    break;
                default:
            }
            reverseFlags[i] = sortItem.getOrdering() == SortItem.Ordering.DESCENDING;
        }
        return new OrderBy(symbols, reverseFlags, nullsFirst);
    }
}
