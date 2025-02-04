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

package io.crate.planner.operators;

import java.util.List;
import java.util.Map;

import io.crate.expression.scalar.SubscriptFunctions;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.FunctionCopyVisitor;
import io.crate.expression.symbol.ScopedSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;

public final class MapBackedSymbolReplacer extends FunctionCopyVisitor<Map<Symbol, Symbol>> {

    private static final MapBackedSymbolReplacer INSTANCE = new MapBackedSymbolReplacer();

    private MapBackedSymbolReplacer() {
    }

    public static Symbol convert(Symbol symbol, Map<Symbol, Symbol> replacements) {
        return replacements.getOrDefault(symbol, symbol.accept(INSTANCE, replacements));
    }

    @Override
    protected Symbol visitSymbol(Symbol symbol, Map<Symbol, Symbol> map) {
        return map.getOrDefault(symbol, symbol);
    }

    @Override
    public Symbol visitField(ScopedSymbol symbol, Map<Symbol, Symbol> map) {
        Symbol replacedSymbol = map.get(symbol);
        if (replacedSymbol != null) {
            return replacedSymbol;
        } else {
            ColumnIdent symbolCol = symbol.toColumn();
            for (var entry : map.entrySet()) {
                ColumnIdent col = entry.getKey().toColumn();
                if (symbolCol.isChildOf(col)) {
                    Symbol fetchStub = entry.getValue();
                    List<String> subscriptPath = symbolCol.getRelativePath(col);
                    return SubscriptFunctions.tryCreateSubscript(fetchStub, subscriptPath);
                }
            }
        }
        return symbol;
    }

    @Override
    public Symbol visitFunction(Function func, Map<Symbol, Symbol> map) {
        Symbol mappedFunc = map.get(func);
        if (mappedFunc == null) {
            return processAndMaybeCopy(func, map);
        } else {
            return mappedFunc;
        }
    }
}
