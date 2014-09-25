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

/**
 * splits a WhereClause query of a query possibly using a nested relation,
 * like a join, a subselect-chain, into a separate query for each relation from the
 * possibly deeply nested relation-tree.
 * <br/>
 * The query assigned to a relation contains those conditions that cannot be
 * resolved further down the tree. That is the leafs, table-relations, will be assigned
 * a query with all the conditions only referencing this table.
 * <br />
 * For a join it will contain all the conditions referencing the tables
 * taking part in the join only.
 * <br />
 * All the parts referencing other tables will be <em>blanked out</em> using
 * boolean literals preserving the queries semantics for that relation.
 * Querying all the relations in the tree from bottom-up using the splitted relations
 * has the same semantics that applying the not-splitted query at the uppermost relation.
 *
 *
 * <h2>Boring technical detail</h2>
 *
 * We iterate over the where clause query, which is a tree of functions
 * (Symbols to be exact), as we represent every operator as function
 * (AND, OR, NOT, =, >, ...).
 * <br />
 * We track which function references which table and for every function we iterate
 * over the relation tree, looking for the right relation to apply this function to.
 *
 * <h2>Example</h2>
 *
 * given a query like:
 * <p>
 *
 *      <code>
 *          SELECT *
 *          FROM t1 CROSS JOIN t2
 *          WHERE t1.a = 1 AND ( t2.b = TRUE or NOT t1.b = 'foo' )
 *      </code>
 *
 * </p>
 *
 * We get the following map back from the {@linkplain #split(WhereClause, io.crate.analyze.relation.AnalyzedRelation)} method:
 * <p>
 *
 *      <code>
 *          {
 *              t1: t1.a = 1 AND ( FALSE or NOT t1.b = 'foo' ),
 *              t2: TRUE AND ( t2.b = TRUE or FALSE )
 *          }
 *      </code>
 * <p>
 *
 * TODO: currently we only build separate queries for table relation, not for joins
 * or other intermediate relations.
 */
public class WhereClauseSplitter extends SymbolVisitor<WhereClauseSplitter.Context, WhereClauseSplitter.ReferencedTables> {

    private static final ImmutableSet<String> LOGICAL_OPERATORS = ImmutableSet.of(
            AndOperator.NAME, OrOperator.NAME, NotPredicate.NAME
    );

    public static class ReferencedTables {
        private static final ReferencedTables EMPTY = new ReferencedTables();
        private final Set<TableIdent> idents;

        public ReferencedTables(TableIdent ... idents) {
            this.idents = new HashSet<>();
            Collections.addAll(this.idents, idents);
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
            this.relationQueryMap = new HashMap<>(this.relation.numRelations(), 1.0F);
        }

        /**
         * Return a stack of symbols used for building the query
         * for the given <code>relation</code> during the splitting process
         * of the WhereClause.
         * <br />
         * Will create an empty stack lazily when there is none yet.
         * @param relation the relation to build a query for.
         * @return a Stack of Symbols.
         */
        public Stack<Symbol> relationStack(AnalyzedRelation relation) {
            Stack<Symbol> stack = relationQueryMap.get(relation);
            if (stack == null) {
                stack = new Stack<>();
                relationQueryMap.put(relation, stack);
            }
            return stack;
        }

        /**
         * build a map of Relations and queries (possibly null)
         * from the stacks populated during the splitting process
         * @return a map from AnalyzedRelation to Symbol which can be null,
         *         which denotes a <em>match all</em> for this relation
         * @throws java.lang.IllegalStateException if the relation stacks have not been finished yet
         */
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

        /**
         * when we hit a table <code>tableRelation</code> in the relation tree
         * and the currently visited predicate (in <code>context</code>):
         *
         *  <ul>
         *      <li>
         *          is a logical operator (OR, AND or NOT) we pull the child
         *          symbols from the stack and put this operator on top of it,
         *          with the child symbols as arguments. This way we rebuild the query
         *          for every relation with only the parts that reference the current relation.
         *      </li>
         *      <li>
         *          only references the table, then we put it on the stack.
         *      </li>
         *  </ul>
         *
         */
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
                        Symbol child = stack.pop();
                        if (child == null) {
                            // NOT does not matter -> does not matter
                            stack.add(null);
                        } else {
                            stack.add(
                                    new Function(
                                            NotPredicate.INFO,
                                            Arrays.asList(
                                                    Objects.firstNonNull(child, Literal.newLiteral(false))
                                            )
                                    )
                            );
                        }
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
