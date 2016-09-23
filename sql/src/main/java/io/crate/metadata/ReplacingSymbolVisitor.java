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

package io.crate.metadata;

import io.crate.analyze.symbol.DynamicReference;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.SymbolVisitor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;


public class ReplacingSymbolVisitor<C> extends SymbolVisitor<C, Symbol> {

    private final ReplaceMode mode;

    public ReplacingSymbolVisitor(ReplaceMode mode) {
        this.mode = mode;
    }

    private Symbol copyFunction(Function function, C context) {
        List<Symbol> newArgs = process(function.arguments(), context);
        if (newArgs != function.arguments()) {
            function = new Function(function.info(), newArgs);
        }
        return function;
    }

    @Override
    public Symbol visitFunction(Function symbol, C context) {
        if (mode == ReplaceMode.MUTATE) {
            processInplace(symbol.arguments(), context);
            return symbol;
        }
        return copyFunction(symbol, context);
    }

    public List<Symbol> process(Collection<Symbol> symbols, C context) {
        ArrayList<Symbol> copy = new ArrayList<>(symbols.size());
        for (Symbol symbol : symbols) {
            copy.add(process(symbol, context));
        }
        return copy;
    }

    public void processInplace(List<Symbol> symbols, C context) {
        for (int i = 0; i < symbols.size(); i++) {
            symbols.set(i, process(symbols.get(i), context));
        }
    }

    @Override
    protected Symbol visitSymbol(Symbol symbol, C context) {
        return symbol;
    }

    @Override
    public Symbol visitDynamicReference(DynamicReference symbol, C context) {
        return visitReference(symbol, context);
    }

}
