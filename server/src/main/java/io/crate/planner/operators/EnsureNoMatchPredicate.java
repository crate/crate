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

import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.expression.predicate.MatchPredicate;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolVisitor;

public final class EnsureNoMatchPredicate extends SymbolVisitor<String, Void> {

    private static final EnsureNoMatchPredicate NO_PREDICATE_VISITOR = new EnsureNoMatchPredicate();

    private EnsureNoMatchPredicate() {}

    public static void ensureNoMatchPredicate(Symbol symbolTree, String errorMsg) {
        symbolTree.accept(NO_PREDICATE_VISITOR, errorMsg);
    }

    @Override
    public Void visitMatchPredicate(io.crate.expression.symbol.MatchPredicate matchPredicate, String errorMsg) {
        throw new UnsupportedFeatureException(errorMsg);
    }

    @Override
    public Void visitFunction(Function symbol, String errorMsg) {
        if (symbol.name().equals(MatchPredicate.NAME)) {
            throw new UnsupportedFeatureException(errorMsg);
        }
        for (Symbol argument : symbol.arguments()) {
            argument.accept(this, errorMsg);
        }
        return null;
    }
}
