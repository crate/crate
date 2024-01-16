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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.UnaryOperator;

import io.crate.analyze.relations.QuerySplitter;
import io.crate.common.collections.Lists;
import io.crate.common.collections.Maps;
import io.crate.common.collections.Sets;
import io.crate.expression.operator.EqOperator;
import io.crate.expression.symbol.ScopedSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolVisitor;
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
public record JoinGraph(
    List<LogicalPlan> nodes,
    Map<LogicalPlan, Set<Edge>> edges,
    List<Symbol> filters,
    boolean hasCrossJoin) {

    public record Edge(LogicalPlan to, Symbol left, Symbol right) {}

    JoinGraph joinWith(JoinGraph other) {
        for (var node : other.nodes) {
            assert !edges.containsKey(node) : "LogicalPlan" + node + " can't be in both graphs";
        }

        var newNodes = Lists.concat(this.nodes, other.nodes);
        var newEdges = Maps.merge(this.edges, other.edges, Sets::union);
        var newFilters = Lists.concat(this.filters, other.filters);
        var hasCrossJoin = this.hasCrossJoin || other.hasCrossJoin();

        return new JoinGraph(
            newNodes,
            newEdges,
            newFilters,
            hasCrossJoin
        );
    }

    JoinGraph withEdges(Map<LogicalPlan, Set<Edge>> edges) {
        var newEdges = Maps.merge(this.edges, edges, Sets::union);
        return new JoinGraph(this.nodes, newEdges, this.filters, this.hasCrossJoin);
    }

    JoinGraph withFilters(List<Symbol> filters) {
        if (filters.isEmpty()) {
            return this;
        }
        var newFilters = Lists.concat(this.filters, filters);
        return new JoinGraph(this.nodes, edges, newFilters, this.hasCrossJoin);
    }

    JoinGraph withCrossJoin(boolean hasCrossJoin) {
        return new JoinGraph(this.nodes, edges, filters, hasCrossJoin);
    }

    public int size() {
        return nodes.size();
    }

    public Set<Edge> edges(LogicalPlan node) {
        var result = edges.get(node);
        if (result == null) {
            return Set.of();
        }
        return result;
    }

    public static JoinGraph create(LogicalPlan plan, UnaryOperator<LogicalPlan> resolvePlan) {
        return plan.accept(new GraphBuilder(resolvePlan), new HashMap<>());
    }

    private static class GraphBuilder extends LogicalPlanVisitor<Map<Symbol, LogicalPlan>, JoinGraph> {

        private final UnaryOperator<LogicalPlan> resolvePlan;

        GraphBuilder(UnaryOperator<LogicalPlan> resolvePlan) {
            this.resolvePlan = resolvePlan;
        }

        @Override
        public JoinGraph visitPlan(LogicalPlan logicalPlan, Map<Symbol, LogicalPlan> context) {
            for (Symbol output : logicalPlan.outputs()) {
                context.put(output, logicalPlan);
            }
            return new JoinGraph(List.of(logicalPlan), Map.of(), List.of(), false);
        }

        @Override
        public JoinGraph visitGroupReference(GroupReference groupReference, Map<Symbol, LogicalPlan> context) {
            return resolvePlan.apply(groupReference).accept(this, context);
        }

        @Override
        public JoinGraph visitFilter(Filter filter, Map<Symbol, LogicalPlan> context) {
            var source = filter.source().accept(this, context);
            return source.withFilters(List.of(filter.query()));
        }

        @Override
        public JoinGraph visitJoinPlan(JoinPlan joinPlan, Map<Symbol, LogicalPlan> context) {

            var left = joinPlan.lhs().accept(this, context);
            var right = joinPlan.rhs().accept(this, context);

            if (joinPlan.joinType() == JoinType.CROSS) {
                return left.joinWith(right).withCrossJoin(true);
            }

            if (joinPlan.joinType() != JoinType.INNER) {
                return left.joinWith(right);
            }

            var joinCondition = joinPlan.joinCondition();
            var edgeCollector = new EdgeCollector();
            var filters = new ArrayList<Symbol>();
            if (joinCondition != null) {
                var split = QuerySplitter.split(joinCondition);
                for (var entry : split.entrySet()) {
                    // we are only interested in equi-join conditions between
                    // two tables e.g.: a.x = b.y will result in
                    // (a,b) -> (a.x = b.y) and we can ignore any other
                    // filters. Therefore, we only want entries where we have
                    // two keys.
                    if (entry.getKey().size() == 2) {
                        entry.getValue().accept(edgeCollector, context);
                    } else {
                        filters.add(entry.getValue());
                    }
                }
            }
            return left.joinWith(right).withEdges(edgeCollector.edges).withFilters(filters);
        }

        private static class EdgeCollector extends SymbolVisitor<Map<Symbol, LogicalPlan>, Void> {

            private final Map<LogicalPlan, Set<Edge>> edges = new HashMap<>();
            private final List<LogicalPlan> sources = new ArrayList<>();

            @Override
            public Void visitField(ScopedSymbol s, Map<Symbol, LogicalPlan> context) {
                sources.add(context.get(s));
                return null;
            }

            @Override
            public Void visitReference(Reference ref, Map<Symbol, LogicalPlan> context) {
                sources.add(context.get(ref));
                return null;
            }

            @Override
            public Void visitFunction(io.crate.expression.symbol.Function f, Map<Symbol, LogicalPlan> context) {
                var sizeSource = sources.size();
                f.arguments().forEach(x -> x.accept(this, context));
                if (f.name().equals(EqOperator.NAME)) {
                    assert sources.size() == sizeSource + 2 : "Source must be collected for each argument";
                    var fromSymbol = f.arguments().get(0);
                    var toSymbol = f.arguments().get(1);
                    var fromRelation = sources.get(sources.size() - 2);
                    var toRelation = sources.get(sources.size() - 1);
                    if (fromRelation != null && toRelation != null) {
                        // Edges are created and indexed for each equi-join condition
                        // from both directions e.g.:
                        // a.x = b.y
                        // becomes:
                        // a -> Edge[b, a.x, b.y]
                        // b -> Edge[a, a.x, b.y]
                        addEdge(fromRelation, new Edge(toRelation, fromSymbol, toSymbol));
                        addEdge(toRelation, new Edge(fromRelation, fromSymbol, toSymbol));
                    }
                }
                return null;
            }

            private void addEdge(LogicalPlan from, Edge edge) {
                var values = edges.get(from);
                if (values == null) {
                    values = Set.of(edge);
                } else {
                    values = new HashSet<>(values);
                    values.add(edge);
                }
                edges.put(from, values);
            }
        }
    }
}
