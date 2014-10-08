/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.analyze.orderby;

import io.crate.analyze.ReferencedTables;
import io.crate.metadata.relation.AnalyzedRelation;
import io.crate.metadata.relation.RelationVisitor;
import io.crate.metadata.relation.TableRelation;
import io.crate.planner.symbol.*;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * The OrderBySplitter splits a list of Symbols (used for sorting) into separate lists of Symbols for each relation
 * that is used by a nested relation (like a join or subselect-chain).
 * <br />
 * Symbols that reference multiple relations of a nested relation are referenced in a separate list of Symbols.
 *
 * <h2>Example</h2>
 *
 * <p>
 *     <code>
 *         SELECT *
 *         FROM t1 CROSS JOIN t2
 *         ORDER BY t1.a, t2.b, (t1.c + t2.d)
 *     </code>
 * </p>
 *
 * The method {@linkplain #split(java.util.List, io.crate.metadata.relation.AnalyzedRelation)} will return a map of
 * list of Symbols with the relation as their keys:
 *
 * <p>
 *     <code>
 *         {
 *             t1: t1.a,
 *             t2: t2.b,
 *             crossjoin(t1,t2): (t1.c + t2.d)
 *         }
 *     </code>
 * </p>
 */

public class OrderBySplitter extends SymbolVisitor<OrderBySplitter.Context, ReferencedTables> {

    public static class Context {
        private final AnalyzedRelation relation;
        private final Map<AnalyzedRelation, List<Symbol>> relationSymbolsMap;

        private Context(AnalyzedRelation relation) {
            this.relation = relation;
            this.relationSymbolsMap = new HashMap<>(this.relation.numRelations(), 1.0F);
        }

        public List<Symbol> relationStack(AnalyzedRelation relation) {
            List<Symbol> stack = relationSymbolsMap.get(relation);
            if (stack == null) {
                stack = new LinkedList<>();
                relationSymbolsMap.put(relation, stack);
            }
            return stack;
        }

        public Map<AnalyzedRelation, List<Symbol>> symbolMap() {
            return relationSymbolsMap;
        }
    }

    /**
     * Context for {@linkplain io.crate.analyze.orderby.OrderBySplitter.Visitor}.
     */
    private static class RelationVisitorCtx {
        final Symbol symbol;
        final Context orderByContext;
        final ReferencedTables tables;

        public RelationVisitorCtx(Symbol symbol, ReferencedTables tables, Context context) {
            this.symbol = symbol;
            this.tables = tables;
            this.orderByContext = context;
        }
    }

    private static class Visitor extends RelationVisitor<RelationVisitorCtx, Void> {

        @Override
        public Void visitRelation(AnalyzedRelation relation, RelationVisitorCtx context) {
            List<Symbol> stack = context.orderByContext.relationStack(relation);
            if (context.tables.referencesMany()) {
                stack.add(context.symbol);
            } else {
                for (AnalyzedRelation child : relation.children()) {
                    process(child, context);
                }
            }
            return null;
        }

        @Override
        public Void visitTableRelation(TableRelation tableRelation, RelationVisitorCtx context) {
            List<Symbol> stack = context.orderByContext.relationStack(tableRelation);
            if (context.tables.referencesOnly(tableRelation.tableInfo().ident())) {
                stack.add(context.symbol);
            }
            return null;
        }
    }

    private final Visitor relationVisitor = new Visitor();

    /**
     * Return a map of relations and symbols from a a given list of sort symbols
     *
     * @param sortSymbols a list of symbols used for sorting
     * @param relation the root relation
     * @return A map from Relation to a List of Symbols, which are applicable for
     *         this relation.
     */
    public Map<AnalyzedRelation, List<Symbol>> split(List<Symbol> sortSymbols, AnalyzedRelation relation) {
        Context ctx = new Context(relation);
        for (Symbol symbol : sortSymbols) {
            ReferencedTables referencedTables = process(symbol, ctx);
            RelationVisitorCtx relationVisitorCtx = new RelationVisitorCtx(symbol, referencedTables, ctx);
            relationVisitor.process(ctx.relation, relationVisitorCtx);
        }
        return ctx.symbolMap();
    }

    @Override
    public ReferencedTables visitFunction(Function symbol, Context context) {
        ReferencedTables tables = new ReferencedTables();
        for (Symbol arg : symbol.arguments()) {
            ReferencedTables argTables = process(arg, context);
            tables.merge(argTables);
        }
        return tables;
    }

    @Override
    public ReferencedTables visitReference(Reference symbol, Context context) {
        return new ReferencedTables(symbol.info().ident().tableIdent());
    }

    @Override
    public ReferencedTables visitDynamicReference(DynamicReference symbol, Context context) {
        return visitReference(symbol, context);
    }

    @Override
    protected ReferencedTables visitSymbol(Symbol symbol, Context context) {
        return ReferencedTables.EMPTY;
    }
}
