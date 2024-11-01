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

package io.crate.planner.consumer;

import java.util.Collection;
import java.util.List;

import org.elasticsearch.common.inject.Singleton;
import org.jetbrains.annotations.Nullable;

import com.carrotsearch.hppc.IntArrayList;

import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolVisitor;

/**
 * Extract 0-based integer positions for order by symbols.
 * <p>
 * This can only be used under the following restriction:
 * <ul>
 * <li>if an <code>orderBySymbol</code> is no input column with explicit index,
 * it must be part of <code>outputSymbols</code> otherwise it's skipped.
 * </li>
 * </ul>
 */
@Singleton
public class OrderByPositionVisitor extends SymbolVisitor<OrderByPositionVisitor.Context, Void> {

    private static final OrderByPositionVisitor INSTANCE = new OrderByPositionVisitor();

    public static class Context {
        final List<? extends Symbol> sourceSymbols;
        IntArrayList orderByPositions;

        public Context(List<? extends Symbol> sourceSymbols) {
            this.sourceSymbols = sourceSymbols;
            this.orderByPositions = new IntArrayList();
        }

        public int[] orderByPositions() {
            return orderByPositions.toArray();
        }
    }

    private OrderByPositionVisitor() {}

    @Nullable
    public static int[] orderByPositionsOrNull(Collection<? extends Symbol> orderBySymbols,
                                               List<? extends Symbol> outputSymbols) {
        Context context = new Context(outputSymbols);
        for (Symbol orderBySymbol : orderBySymbols) {
            orderBySymbol.accept(INSTANCE, context);
        }
        if (context.orderByPositions.size() == orderBySymbols.size()) {
            return context.orderByPositions();
        }
        return null;
    }

    public static int[] orderByPositions(Collection<? extends Symbol> orderBySymbols,
                                         List<? extends Symbol> outputSymbols) {
        int[] positions = orderByPositionsOrNull(orderBySymbols, outputSymbols);
        if (positions == null) {
            throw new IllegalArgumentException(
                "Must have an orderByPosition for each symbol. Got ORDER BY " + orderBySymbols + " and outputs: " +
                outputSymbols);
        }
        return positions;
    }

    @Override
    public Void visitInputColumn(InputColumn inputColumn, Context context) {
        context.orderByPositions.add(inputColumn.index());
        return null;
    }

    @Override
    protected Void visitSymbol(Symbol symbol, Context context) {
        int idx = context.sourceSymbols.indexOf(symbol);
        if (idx >= 0) {
            context.orderByPositions.add(idx);
        }
        return null;
    }
}
