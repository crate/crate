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

package io.crate.analyze.relations;


import io.crate.expression.operator.AndOperator;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.MatchPredicate;
import io.crate.expression.symbol.ScopedSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolVisitor;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.planner.consumer.RelationNameCollector;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

public class QuerySplitter {

    private static final SplitVisitor SPLIT_VISITOR = new SplitVisitor();

    /**
     * <p>
     * Splits a (function) symbol on <code>AND</code> based on relation occurrences of {@link ScopedSymbol}
     * into multiple symbols.
     * </p>
     * <p>
     * <h3>Examples:</h3>
     * <p>
     * Splittable down to single relation:
     * <pre>
     *     t1.x = 30 and ty2.y = 20
     *
     *     Output:
     *
     *     set(t1) -> t1.x = 30
     *     set(t2) -> t2.y = 20
     * </pre>
     * <p>
     * <p>
     * Pairs of two relations:
     * <pre>
     *     t1.x = t2.x and t2.x = t3.x
     *
     *     Output:
     *
     *     set(t1, t2) -> t1.x = t2.x
     *     set(t2, t3) -> t2.x = t3.x
     * </pre>
     * <p>
     * <p>
     * Mixed:
     * <pre>
     *     t1.x = 30 and t2.x = t3.x
     *
     *     Output:
     *
     *     set(t1)      -> t1.x = 30
     *     set(t2, t3)  -> t2.x = t3.x
     * </pre>
     */
    public static Map<Set<RelationName>, Symbol> split(Symbol query) {
        Context context = new Context(query);
        query.accept(SPLIT_VISITOR, context);
        return context.parts;
    }

    private static class Context {
        final Set<RelationName> allNames;
        final LinkedHashMap<Set<RelationName>, Symbol> parts;

        public Context(Symbol query) {
            allNames = RelationNameCollector.collect(query);
            parts = new LinkedHashMap<>();
        }
    }

    private static class SplitVisitor extends SymbolVisitor<Context, Void> {

        @Override
        public Void visitLiteral(Literal literal, Context ctx) {
            Symbol prevQuery = ctx.parts.put(ctx.allNames, literal);
            if (prevQuery != null) {
                ctx.parts.put(ctx.allNames, AndOperator.of(prevQuery, literal));
            }
            return null;
        }

        @Override
        public Void visitFunction(Function function, Context ctx) {
            var signature = function.signature();
            assert signature != null : "Expecting functions signature not to be null";
            if (!signature.equals(AndOperator.SIGNATURE)) {
                Set<RelationName> qualifiedNames = RelationNameCollector.collect(function);
                Symbol prevQuery = ctx.parts.put(qualifiedNames, function);
                if (prevQuery != null) {
                    ctx.parts.put(qualifiedNames, AndOperator.of(prevQuery, function));
                }
                return null;
            }
            for (Symbol arg : function.arguments()) {
                arg.accept(this, ctx);
            }
            return null;
        }

        @Override
        public Void visitField(ScopedSymbol field, Context ctx) {
            ctx.parts.put(Collections.singleton(field.relation()), field);
            return null;
        }

        @Override
        public Void visitReference(Reference ref, Context ctx) {
            ctx.parts.put(Set.of(ref.ident().tableIdent()), ref);
            return null;
        }

        @Override
        public Void visitMatchPredicate(MatchPredicate matchPredicate, Context ctx) {
            LinkedHashSet<RelationName> relationNames = new LinkedHashSet<>();
            for (Symbol field : matchPredicate.identBoostMap().keySet()) {
                if (field instanceof ScopedSymbol) {
                    relationNames.add(((ScopedSymbol) field).relation());
                } else if (field instanceof Reference) {
                    relationNames.add(((Reference) field).ident().tableIdent());
                }
            }
            ctx.parts.put(relationNames, matchPredicate);
            return null;
        }
    }
}
