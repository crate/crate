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

package io.crate.planner.consumer;

import com.carrotsearch.hppc.IntArrayList;
import io.crate.planner.symbol.*;
import org.elasticsearch.common.inject.Singleton;

import java.util.List;

/**
 * Extract 0-based integer positions for order by symbols.
 *
 * This can only be used under the following restriction:
 * <ul>
 *   <li>if an <code>orderBySymbol</code> is no input column with explicit index,
 *    it must be part of <code>sourceSymbols</code>.
 */
@Singleton
public class OrderByPositionVisitor extends SymbolVisitor<OrderByPositionVisitor.Context, Void> {

    private static OrderByPositionVisitor INSTANCE = new OrderByPositionVisitor();

    public static class Context {
        final List<Symbol> sourceSymbols;
        IntArrayList orderByPositions;

        public Context(List<Symbol> sourceSymbols) {
            this.sourceSymbols = sourceSymbols;
            this.orderByPositions = new IntArrayList();
        }

        public int[] orderByPositions() {
            return orderByPositions.toArray();
        }
    }

    private OrderByPositionVisitor() {
    }

    public static int[] orderByPositions(List<Symbol> orderBySymbols, List<Symbol> sourceSymbols) {
        Context context = new Context(sourceSymbols);
        for (Symbol orderBySymbol : orderBySymbols) {
            INSTANCE.process(orderBySymbol, context);
        }
        return context.orderByPositions();
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
        } else {
            throw new IllegalArgumentException(SymbolFormatter.format("Cannot sort by: %s - not part of source symbols", symbol));
        }
        return null;
    }
}
