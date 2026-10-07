/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.planner.optimizer.joinorder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.UnaryOperator;

import io.crate.expression.operator.AndOperator;
import io.crate.expression.operator.EqOperator;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.ScopedSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.Reference;
import io.crate.planner.operators.Filter;
import io.crate.planner.operators.JoinPlan;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.operators.LogicalPlanVisitor;
import io.crate.planner.optimizer.iterative.GroupReference;
import io.crate.sql.tree.JoinType;

/**
 * JoinGraph is an undirected multi-graph representing a sequence of Joins.
 * The nodes are logical plans and edges are built based on equi-join
 * conditions between two nodes.
 *
 * <p>
 * The following join plan:
 * </p>
 *
 * <pre>
 * JoinPlan[INNER | (z = y)]
 * ├ JoinPlan[INNER | (x = z)]
 * │  ├ Collect[doc.t1 | [x] | true]
 * │  └ Collect[doc.t3 | [z] | true]
 * └ Collect[doc.t2 | [y] | true]
 * </pre>
 *
 * <p>
 * becomes the following join-graph:
 * </p>
 *
 *<pre>
 * +----+               +----+               +----+
 * | t1 |--t1.x = t3.z--| t3 |--t3.z = t2.y--| t2 |
 * +----+               +----+               +----+
 *</pre>
 *
 * <p>
 * Edges are created and indexed for each equi-join condition
 * from both directions so a.x = b.y becomes:
 * </p>
 * <pre>
 * a -> Edge[b, a.x, b.y]
 * b -> Edge[a, a.x, b.y]
 * </pre>
 */
// todo comments for fields
public record JoinGraph(List<LogicalPlan> nodes,
                        Map<LogicalPlan, List<Edge>> edges,
                        List<Symbol> filters,
                        int originalCrossJoins) {

    public record Edge(LogicalPlan to, Symbol left, Symbol right) {}

    public int size() {
        return nodes().size();
    }

    public List<Edge> edges(LogicalPlan node) {
        return edges.getOrDefault(node, List.of());
    }

    public boolean hasCrossJoin() {
        return originalCrossJoins > 0;
    }

    public static JoinGraph create(LogicalPlan join, UnaryOperator<LogicalPlan> resolvePlan) {
        var builder = new GraphBuilder(resolvePlan);
        // todo figure out the context
        join.accept(builder, join);
        return builder.build();
    }

    private static class GraphBuilder extends LogicalPlanVisitor<LogicalPlan, Void> {
        private final UnaryOperator<LogicalPlan> resolvePlan;
        private final List<LogicalPlan> nodes = new ArrayList<>();
        // Maps symbols to source plan that produce those, i.e. that have them in their outputs.
        // Needed to build edges.
        private final Map<Symbol, LogicalPlan> symbolSources = new HashMap<>();
        // Set, because the same condition can appear multiple times (e.g. in a join condition and a filter)
        private final Set<Symbol> conditions = new LinkedHashSet<>();
        private int crossJoins = 0;

        GraphBuilder(UnaryOperator<LogicalPlan> resolvePlan) {
            this.resolvePlan = resolvePlan;
        }

        @Override
        public Void visitPlan(LogicalPlan plan, LogicalPlan node) {
            nodes.add(node);
            for (Symbol output : node.outputs()) {
                symbolSources.put(output, node);
            }
            return null;
        }

        @Override
        public Void visitGroupReference(GroupReference groupReference, LogicalPlan context) {
            return resolvePlan.apply(groupReference).accept(this, context);
        }

        @Override
        public Void visitFilter(Filter filter, LogicalPlan node) {
            if (!includeInGraph(filter)) {
                return visitPlan(filter, node);
            }
            conditions.addAll(AndOperator.split(filter.query()));
            filter.source().accept(this, filter.source());
            return null;
        }

        @Override
        public Void visitJoinPlan(JoinPlan join, LogicalPlan node) {
            if (!includeInGraph(join)) {
                return visitPlan(join, node);
            }
            join.lhs().accept(this, join.lhs());
            join.rhs().accept(this, join.rhs());
            if (join.joinType() == JoinType.CROSS) {
                crossJoins++;
            }
            if (join.joinCondition() != null) {
                conditions.addAll(AndOperator.split(join.joinCondition()));
            }
            return null;
        }

        /**
         * INNER/CROSS joins, and Filters on top of them, can be included in the JoinGraph.
         * A Filter on anything else belongs to the node/plan below it (e.g. Filter -> Collect).
         */
        private boolean includeInGraph(LogicalPlan plan) {
            LogicalPlan resolved = resolvePlan.apply(plan);
            if (resolved instanceof JoinPlan join) {
                return join.joinType() == JoinType.INNER || join.joinType() == JoinType.CROSS;
            }
            return resolved instanceof Filter filter && includeInGraph(filter.source());
        }

        JoinGraph build() {
            Map<LogicalPlan, List<Edge>> edges = new HashMap<>();
            // Anything that isn't an equi-join is put into `filters`.
            List<Symbol> filters = new ArrayList<>();
            for (Symbol condition : conditions) {
                if (!addEdges(condition, edges)) {
                    filters.add(condition);
                }
            }
            return new JoinGraph(List.copyOf(nodes), edges, filters, crossJoins);
        }

        /// Adds the edges for a `left = right` condition, if:
        /// - each side references exactly one node/plan
        /// - the referenced nodes/plans are different.
        /// Examples:
        /// - `t1.x = t2.y` -> edges are `t1 -> t2` and `t2 -> t1`
        /// - `t1.x + t2.y = t3.z` -> not an edge (left side references two plans)
        /// - `t1.x = 1` -> not an edge (right side references no plan)
        /// - `t1.x = t1.y` -> not an edge (same plan on both sides)
        private boolean addEdges(Symbol condition, Map<LogicalPlan, List<Edge>> edges) {
            if (!(condition instanceof Function eq && eq.name().equals(EqOperator.NAME))) {
                return false;
            }
            Symbol leftSym = eq.arguments().get(0);
            Symbol rightSym = eq.arguments().get(1);
            Set<LogicalPlan> leftPlans = sourceOf(leftSym);
            Set<LogicalPlan> rightPlans = sourceOf(rightSym);
            if (leftPlans.size() != 1 || rightPlans.size() != 1 || leftPlans.equals(rightPlans)) {
                return false;
            }
            LogicalPlan leftNode = leftPlans.iterator().next();
            LogicalPlan rightNode = rightPlans.iterator().next();
            edges.computeIfAbsent(leftNode, _ -> new ArrayList<>()).add(new Edge(rightNode, leftSym, rightSym));
            edges.computeIfAbsent(rightNode, _ -> new ArrayList<>()).add(new Edge(leftNode, leftSym, rightSym));
            return true;
        }

        /// Returns the nodes/plans which produce the columns used in `symbol`.
        /// For more details, see [#collectSources(Symbol, Set)].
        private Set<LogicalPlan> sourceOf(Symbol symbol) {
            Set<LogicalPlan> result = new HashSet<>();
            collectSources(symbol, result);
            return result;
        }

        /// Collects the nodes/plans which produce the columns used in `symbol`,
        /// i.e. the nodes/plans which have those columns in their `outputs()`.
        ///
        /// Example: `t1.a = t2.b`
        ///
        /// - The whole `=` expression isn't in [#symbolSources], so its arguments are visited.
        /// - `t1.a` maps to the plan which outputs it, e.g. `Collect[t1]`
        /// - `t2.b` maps to `Collect[t2]`.
        /// - Result: `{Collect[t1], Collect[t2]}`, the two plans this condition joins.
        ///
        /// The symbol itself is looked up first, so outputs which are expressions
        /// (e.g. `x + 1` from an Eval) are resolved as a whole.
        /// Symbols which aren't columns (literals, outer columns, ...) don't add any node/plan.
        private void collectSources(Symbol symbol, Set<LogicalPlan> result) {
            LogicalPlan node = symbolSources.get(symbol);
            if (node != null) {
                result.add(node);
            } else if (symbol instanceof Function func) {
                for (Symbol argument : func.arguments()) {
                    collectSources(argument, result);
                }
            } else {
                assert !(symbol instanceof Reference || symbol instanceof ScopedSymbol)
                    : "Column " + symbol + " must be an output of a node in the graph";
            }
        }
    }
}
