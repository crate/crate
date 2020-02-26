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

package io.crate.expression.symbol;

import java.util.List;
import java.util.function.Predicate;

public class SymbolVisitors {

    private static final AnyPredicateVisitor ANY_VISITOR = new AnyPredicateVisitor();

    public static boolean any(Predicate<? super Symbol> predicate, List<? extends Symbol> symbols) {
        for (int i = 0; i < symbols.size(); i++) {
            if (any(predicate, symbols.get(i))) {
                return true;
            }
        }
        return false;
    }

    public static boolean any(Predicate<? super Symbol> symbolPredicate, Symbol symbol) {
        return symbol.accept(ANY_VISITOR, symbolPredicate);
    }

    private static class AnyPredicateVisitor extends SymbolVisitor<Predicate<? super Symbol>, Boolean> {

        @Override
        public Boolean visitFunction(Function symbol, Predicate<? super Symbol> symbolPredicate) {
            if (symbolPredicate.test(symbol)) {
                return true;
            }
            for (Symbol arg : symbol.arguments()) {
                if (arg.accept(this, symbolPredicate)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public Boolean visitWindowFunction(WindowFunction symbol, Predicate<? super Symbol> context) {
            return visitFunction(symbol, context);
        }

        @Override
        public Boolean visitFetchReference(FetchReference fetchReference, Predicate<? super Symbol> symbolPredicate) {
            return symbolPredicate.test(fetchReference)
                   || fetchReference.fetchId().accept(this, symbolPredicate)
                   || fetchReference.ref().accept(this, symbolPredicate);
        }

        @Override
        public Boolean visitMatchPredicate(MatchPredicate matchPredicate, Predicate<? super Symbol> symbolPredicate) {
            if (symbolPredicate.test(matchPredicate)) {
                return true;
            }
            for (Symbol field : matchPredicate.identBoostMap().keySet()) {
                if (field.accept(this, symbolPredicate)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public Boolean visitAlias(AliasSymbol aliasSymbol, Predicate<? super Symbol> predicate) {
            if (predicate.test(aliasSymbol)) {
                return true;
            }
            return predicate.test(aliasSymbol.symbol());
        }

        @Override
        protected Boolean visitSymbol(Symbol symbol, Predicate<? super Symbol> symbolPredicate) {
            return symbolPredicate.test(symbol);
        }
    }

}
