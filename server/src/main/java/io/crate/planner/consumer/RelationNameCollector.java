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

import io.crate.expression.symbol.DefaultTraversalSymbolVisitor;
import io.crate.expression.symbol.MatchPredicate;
import io.crate.expression.symbol.ScopedSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;

import java.util.LinkedHashSet;
import java.util.Set;

public class RelationNameCollector extends DefaultTraversalSymbolVisitor<Set<RelationName>, Void> {

    private static final RelationNameCollector INSTANCE = new RelationNameCollector();

    public static Set<RelationName> collect(Symbol symbol) {
        var names = new LinkedHashSet<RelationName>();
        symbol.accept(INSTANCE, names);
        return names;
    }

    @Override
    public Void visitField(ScopedSymbol field, Set<RelationName> context) {
        context.add(field.relation());
        return null;
    }

    @Override
    public Void visitReference(Reference symbol, Set<RelationName> context) {
        context.add(symbol.ident().tableIdent());
        return null;
    }

    @Override
    public Void visitMatchPredicate(MatchPredicate matchPredicate, Set<RelationName> context) {
        for (Symbol field : matchPredicate.identBoostMap().keySet()) {
            field.accept(this, context);
        }
        return null;
    }
}
