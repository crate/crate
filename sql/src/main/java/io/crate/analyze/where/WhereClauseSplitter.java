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

package io.crate.analyze.where;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.crate.analyze.relation.AnalyzedRelation;
import io.crate.analyze.relation.RelationVisitor;
import io.crate.metadata.TableIdent;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.operator.AndOperator;
import io.crate.operation.operator.OrOperator;
import io.crate.operation.predicate.NotPredicate;
import io.crate.planner.symbol.*;
import io.crate.types.DataTypes;

import java.util.*;

public class WhereClauseSplitter extends SymbolVisitor<WhereClauseSplitter.Context, WhereClauseSplitter.ReferencedTables> {

    private static final ImmutableSet<String> LOGICAL_OPERATORS = ImmutableSet.of(
            AndOperator.NAME, OrOperator.NAME, NotPredicate.NAME
    );

    public static class ReferencedTables {
        private static final ReferencedTables EMPTY = new ReferencedTables();
        private final Set<TableIdent> idents = new HashSet<>();

        public static ReferencedTables of(TableIdent ... idents) {
            if (idents.length == 0) {
                return EMPTY;
            }
            ReferencedTables tables = new ReferencedTables();
            Collections.addAll(tables.idents, idents);
            return tables;
        }

        public void merge(ReferencedTables tables) {
            idents.addAll(tables.idents);
        }

        public boolean referencesMany() {
            return idents.size() > 1;
        }

        public boolean referencesOnly(TableIdent ident) {
            return idents.size() == 1 && references(ident);
        }

        public boolean references(TableIdent ident) {
            return idents.contains(ident);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ReferencedTables that = (ReferencedTables) o;

            if (!idents.equals(that.idents)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            return idents.hashCode();
        }
    }

    public static class Context {
        private final AnalyzedRelation relation;
        private final Map<AnalyzedRelation, Stack<Symbol>> relationQueryMap;

        private Context(AnalyzedRelation relation) {
            this.relation = relation;
            this.relationQueryMap = new HashMap<>(this.relation.numChildren(), 1.0F);
        }

        public Stack<Symbol> relationStack(AnalyzedRelation relation) {
            Stack<Symbol> stack = relationQueryMap.get(relation);
            if (stack == null) {
                stack = new Stack<>();
                relationQueryMap.put(relation, stack);
            }
            return stack;
        }

        public Map<AnalyzedRelation, Symbol> queryMap() {
            Map<AnalyzedRelation, Symbol> queryMap = new HashMap<>(relationQueryMap.size(), 1.0F);
            for (Map.Entry<AnalyzedRelation, Stack<Symbol>> entry : relationQueryMap.entrySet()) {
                if (entry.getValue() == null || entry.getValue().isEmpty()) {
                    queryMap.put(entry.getKey(), null);
                } else if (entry.getValue().size() == 1) {
                    queryMap.put(entry.getKey(), entry.getValue().pop());
                } else {
                    throw new IllegalStateException(
                            String.format(
                                    Locale.ENGLISH,
                                    "query for relation %s not yet built",
                                    entry.getKey())
                    );
                }
            }
            return queryMap;
        }
    }

    /**
     * Context for {@linkplain io.crate.analyze.where.WhereClauseSplitter.Visitor}.
     */
    private static class RelationVisitorCtx {
        final Function symbol;
        final Context whereClauseContext;
        final ReferencedTables tables;

        public RelationVisitorCtx(Function symbol, ReferencedTables tables, Context context) {
            this.symbol = symbol;
            this.tables = tables;
            this.whereClauseContext = context;
        }
    }

    private static class Visitor extends RelationVisitor<RelationVisitorCtx, Void> {

        @Override
        public Void visitRelation(AnalyzedRelation relation, RelationVisitorCtx context) {
            for (AnalyzedRelation child : relation.children()) {
                process(child, context);
            }
            // TODO: extend logic to build queries for JOIN relations and subselects too
            return null;
        }

        @Override
        public Void visitTableInfo(TableInfo tableRelation, RelationVisitorCtx context) {
            Stack<Symbol> stack = context.whereClauseContext.relationStack(tableRelation);
            if (LOGICAL_OPERATORS.contains(context.symbol.info().ident().name())) {
                switch (context.symbol.info().ident().name()) {
                    case AndOperator.NAME:
                        assert stack.size() >= 2 : "stack too small";
                        stack.add(
                                new Function(
                                        AndOperator.INFO,
                                        Arrays.asList(
                                                Objects.firstNonNull(stack.pop(), Literal.newLiteral(true)),
                                                Objects.firstNonNull(stack.pop(), Literal.newLiteral(true))
                                        )
                                )
                        );
                        break;
                    case OrOperator.NAME:
                        assert stack.size() >= 2 : "stack too small";
                        stack.add(
                                new Function(
                                        OrOperator.INFO,
                                        Arrays.asList(
                                                Objects.firstNonNull(stack.pop(), Literal.newLiteral(false)),
                                                Objects.firstNonNull(stack.pop(), Literal.newLiteral(false))
                                        )
                                )
                        );
                        break;
                    case NotPredicate.NAME:
                        assert stack.size() >= 1 : "stack too small";
                        stack.add(
                                new Function(
                                        NotPredicate.INFO,
                                        Arrays.asList(
                                                Objects.firstNonNull(stack.pop(), Literal.newLiteral(false))
                                        )
                                )
                        );
                        break;
                    default:
                        throw new IllegalArgumentException(
                                SymbolFormatter.format(
                                        "somehow we do not have a logical operator here: %s, strange",
                                        context.symbol)
                        );
                }
            } else if (context.tables.referencesOnly(tableRelation.ident())) {
                stack.add(context.symbol);
            } else {
                stack.add(null);
            }
            return null;
        }
    }

    private Visitor relationVisitor = new Visitor();

    /**
     * create new queries from the given whereClause and return them in a map
     *
     * @param whereClause the where clause to extract the part that affects the tables
     *                    referenced in <code>relation</code>
     * @param relation the root Relation
     * @return A map from Relation to a (nullable) Symbol, representing the query
     *         which is applicable for this relation (containing clauses that cannot
     *         be resolved further down the relation-tree).
     */
    public Map<AnalyzedRelation, Symbol> split(WhereClause whereClause, AnalyzedRelation relation) {
        if (whereClause.hasQuery()) {
            Context ctx = new Context(relation);
            process(whereClause.query(), ctx);
            return ctx.queryMap();
        }
        return ImmutableMap.of();
    }

    @Override
    public ReferencedTables visitFunction(Function symbol, Context context) {
        ReferencedTables tables = new ReferencedTables();
        for (Symbol arg : symbol.arguments()) {
            ReferencedTables argTables = process(arg, context);
            tables.merge(argTables);
        }
        if (symbol.info().returnType().equals(DataTypes.BOOLEAN)) {
            // go through the tree of relations for this function
            // only do it for predicate functions as others pollute our stack
            RelationVisitorCtx relationVisitorCtx = new RelationVisitorCtx(symbol, tables, context);
            relationVisitor.process(context.relation, relationVisitorCtx);
        }
        return tables;
    }


    @Override
    public ReferencedTables visitReference(Reference symbol, Context context) {
        return ReferencedTables.of(symbol.info().ident().tableIdent());
    }

    @Override
    public ReferencedTables visitDynamicReference(DynamicReference symbol, Context context) {
        return visitReference(symbol, context);
    }

    @Override
    protected ReferencedTables visitSymbol(Symbol symbol, Context context) {
        return ReferencedTables.of();
    }
}
