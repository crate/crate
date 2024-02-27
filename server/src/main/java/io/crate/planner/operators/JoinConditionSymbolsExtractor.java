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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import io.crate.expression.operator.AndOperator;
import io.crate.expression.operator.EqOperator;
import io.crate.expression.symbol.DefaultTraversalSymbolVisitor;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.ScopedSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolVisitor;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;

/**
 * Extracts all symbols per relations from any EQ join conditions to use them for building hashes for the hash join
 * algorithm. The extractor will detect if a symbol occurs in multiple EQ condition and if so won't extract any symbols
 * from this EQ condition (skipping all EQ operator arguments).
 * <p>
 * Example:
 * <pre>
 *     t1.a = t2.a AND function(t1.a) = t2.b AND function(t1.a) = t2.c AND t1.x > t2.y
 * </pre>
 *
 * will result in:
 *
 * <pre>
 *  t1: [t1.a, function(t1.a)]
 *  t2: [t2.a, t2.b]
 *  </pre>
 * <p>
 * as t2.c is compared to the same symbol as t2.b and only EQ operators are processed.
 *
 * It is expected that each expression argument of a EQ operator only contains symbols of one relation.
 * This can be ensured by using the {@link EquiJoinDetector} upfront.
 */
public final class JoinConditionSymbolsExtractor {

    private static final SymbolExtractor SYMBOL_EXTRACTOR = new SymbolExtractor();
    private static final RelationExtractor RELATION_EXTRACTOR = new RelationExtractor();

    /**
     * Extracts all symbols per relations from any EQ join conditions. See {@link JoinConditionSymbolsExtractor}
     * class documentation for details.
     */
    public static Map<RelationName, List<Symbol>> extract(Symbol symbol) {
        Context ctx = new Context();
        symbol.accept(SYMBOL_EXTRACTOR, ctx);
        return ctx.symbolsPerRelation;
    }

    private static class Context {
        boolean insideEqOperator = false;
        Map<RelationName, List<Symbol>> symbolsPerRelation = new LinkedHashMap<>();
    }

    private static class SymbolExtractor extends DefaultTraversalSymbolVisitor<Context, Void> {

        @Override
        public Void visitFunction(Function function, Context context) {
            String functionName = function.name();
            switch (functionName) {
                case AndOperator.NAME:
                    return super.visitFunction(function, context);
                case EqOperator.NAME:
                    context.insideEqOperator = true;
                    int duplicatePos = 0;
                    for (Symbol arg : function.arguments()) {
                        var relation = arg.accept(RELATION_EXTRACTOR, null);
                        List<Symbol> symbols = context.symbolsPerRelation.computeIfAbsent(relation, k -> new ArrayList<>());
                        if (symbols.contains(arg)) {
                            // duplicate detected, use the current size as the position of the other relation symbol we
                            // want to remove (if any)
                            duplicatePos = symbols.size();
                            continue;
                        }
                        symbols.add(arg);
                    }
                    // if a duplicate is found, we must remove already processed argument symbols of the other relation
                    if (duplicatePos > 0) {
                        for (Map.Entry<RelationName, List<Symbol>> entry : context.symbolsPerRelation.entrySet()) {
                            List<Symbol> symbols = entry.getValue();
                            if (symbols.size() > duplicatePos) {
                                symbols.remove(duplicatePos);
                            }
                        }
                    }
                    context.insideEqOperator = false;
                    return null;
                default:
                    if (context.insideEqOperator) {
                        return super.visitFunction(function, context);
                    } else {
                        return null;
                    }

            }
        }
    }

    private static class RelationExtractor extends SymbolVisitor<Void, RelationName> {

        @Override
        public RelationName visitFunction(Function symbol, Void context) {
            for (Symbol arg : symbol.arguments()) {
                var relation = arg.accept(this, context);
                if (relation != null) {
                    return relation;
                }
            }
            return null;
        }

        @Override
        public RelationName visitField(ScopedSymbol field, Void context) {
            return field.relation();
        }

        @Override
        public RelationName visitReference(Reference ref, Void context) {
            return ref.ident().tableIdent();
        }
    }

    private JoinConditionSymbolsExtractor() {
    }
}
