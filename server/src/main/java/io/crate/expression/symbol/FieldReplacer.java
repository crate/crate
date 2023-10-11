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

package io.crate.expression.symbol;

import java.util.HashMap;
import java.util.function.Function;

public final class FieldReplacer extends FunctionCopyVisitor<Function<? super ScopedSymbol, ? extends Symbol>> {

    private static final FieldReplacer REPLACER = new FieldReplacer();

    private FieldReplacer() {
        super();
    }

    public static Symbol replaceFields(Symbol tree, Function<? super ScopedSymbol, ? extends Symbol> replaceFunc) {
        if (tree == null) {
            return null;
        }
        return tree.accept(REPLACER, replaceFunc);
    }

    public static Function<? super Symbol, ? extends Symbol> bind(Function<? super ScopedSymbol, ? extends Symbol> replaceFunc) {
        return st -> replaceFields(st, replaceFunc);
    }

    @Override
    public Symbol visitField(ScopedSymbol field, Function<? super ScopedSymbol, ? extends Symbol> replaceFunc) {
        return replaceFunc.apply(field);
    }

    @Override
    public Symbol visitMatchPredicate(MatchPredicate matchPredicate,
                                      Function<? super ScopedSymbol, ? extends Symbol> mapper) {
        HashMap<Symbol, Symbol> newIdentBoost = new HashMap<>();
        for (var entry : matchPredicate.identBoostMap().entrySet()) {
            var key = entry.getKey();
            if (key instanceof ScopedSymbol scopedSymbol) {
                Symbol newKey = mapper.apply(scopedSymbol);
                newIdentBoost.put(newKey, entry.getValue().accept(this, mapper));
            } else {
                newIdentBoost.put(key, entry.getValue().accept(this, mapper));
            }
        }
        return new MatchPredicate(
            newIdentBoost,
            matchPredicate.queryTerm().accept(this, mapper),
            matchPredicate.matchType(),
            matchPredicate.options().accept(this, mapper)
        );
    }
}
