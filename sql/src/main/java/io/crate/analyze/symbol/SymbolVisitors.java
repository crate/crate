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

package io.crate.analyze.symbol;

import com.google.common.base.Predicate;

public class SymbolVisitors {

    private static final AnyPredicateVisitor ANY_VISITOR = new AnyPredicateVisitor();

    public static boolean any(Predicate<? super Symbol> symbolPredicate, Symbol symbol) {
        return ANY_VISITOR.process(symbol, symbolPredicate);
    }

    private static class AnyPredicateVisitor extends SymbolVisitor<Predicate<? super Symbol>, Boolean> {

        @Override
        public Boolean visitFunction(Function symbol, Predicate<? super Symbol> symbolPredicate) {
            if (symbolPredicate.apply(symbol)) {
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
        public Boolean visitFetchReference(FetchReference fetchReference, Predicate<? super Symbol> symbolPredicate) {
            return symbolPredicate.apply(fetchReference)
                   || fetchReference.fetchId().accept(this, symbolPredicate)
                   || fetchReference.ref().accept(this, symbolPredicate);
        }

        @Override
        public Boolean visitMatchPredicate(MatchPredicate matchPredicate, Predicate<? super Symbol> symbolPredicate) {
            if (symbolPredicate.apply(matchPredicate)) {
                return true;
            }
            for (Field field : matchPredicate.identBoostMap().keySet()) {
                if (field.accept(this, symbolPredicate)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        protected Boolean visitSymbol(Symbol symbol, Predicate<? super Symbol> symbolPredicate) {
            return symbolPredicate.apply(symbol);
        }
    }

}
