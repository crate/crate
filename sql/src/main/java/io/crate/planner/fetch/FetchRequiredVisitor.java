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

package io.crate.planner.fetch;

import io.crate.analyze.symbol.*;
import io.crate.metadata.Reference;
import io.crate.metadata.doc.DocSysColumns;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class FetchRequiredVisitor extends SymbolVisitor<FetchRequiredVisitor.Context, Boolean> {

    public static final FetchRequiredVisitor INSTANCE = new FetchRequiredVisitor();

    private FetchRequiredVisitor() {
    }

    public static class Context {

        private Set<Symbol> querySymbols;

        public Context(){};

        public Context(Set<Symbol> querySymbols) {
            this.querySymbols = querySymbols;
        }

        boolean isQuerySymbol(Symbol symbol) {
            return querySymbols != null && querySymbols.contains(symbol);
        }

        void allocateQuerySymbol(Symbol symbol) {
            if (querySymbols == null) {
                querySymbols = new HashSet<>(1);
            }
            querySymbols.add(symbol);
        }

        public Set<Symbol> querySymbols() {
            return querySymbols;
        }

    }

    public boolean process(List<Symbol> symbols, Context context) {
        boolean result = false;
        for (Symbol symbol : symbols) {
            result = process(symbol, context) || result;
        }
        return result;
    }

    @Override
    public Boolean visitReference(Reference symbol, Context context) {
        if (context.isQuerySymbol(symbol)) {
            return false;
        } else if (symbol.ident().columnIdent().equals(DocSysColumns.SCORE)) {
            context.allocateQuerySymbol(symbol);
            return false;
        }
        return true;
    }

    @Override
    public Boolean visitField(Field field, Context context) {
        if (context.isQuerySymbol(field)) {
            return false;
        } else {
            if (field.path().equals(DocSysColumns.SCORE)) {
                context.allocateQuerySymbol(field);
                return false;
            }
        }
        return true;
    }

    @Override
    public Boolean visitDynamicReference(DynamicReference symbol, Context context) {
        return visitReference(symbol, context);
    }

    @Override
    protected Boolean visitSymbol(Symbol symbol, Context context) {
        return false;
    }

    @Override
    public Boolean visitAggregation(Aggregation symbol, Context context) {
        return !context.isQuerySymbol(symbol) && process(symbol.inputs(), context);
    }

    @Override
    public Boolean visitFunction(Function symbol, Context context) {
        return !context.isQuerySymbol(symbol) && process(symbol.arguments(), context);
    }
}
