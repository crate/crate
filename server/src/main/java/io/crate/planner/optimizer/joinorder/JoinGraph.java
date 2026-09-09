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

import static io.crate.planner.operators.EquiJoinDetector.isEquiJoin;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.UnaryOperator;

import io.crate.analyze.relations.QuerySplitter;
import io.crate.common.collections.Lists;
import io.crate.common.collections.Maps;
import io.crate.expression.operator.EqOperator;
import io.crate.expression.symbol.ScopedSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolVisitor;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
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
public record JoinGraph(List<LogicalPlan> nodes,
                        Map<LogicalPlan, List<Edge>> edges,
                        List<Symbol> filters,
                        boolean hasCrossJoin) {

    public record Edge(LogicalPlan to, Symbol left, Symbol right) {}

    JoinGraph joinWith(JoinGraph other) {
        for (var node : other.nodes) {
            assert !edges.containsKey(node) : "LogicalPlan" + node + " can't be in both graphs";
        }

        var newNodes = Lists.concat(this.nodes, other.nodes);
        var newEdges = Maps.merge(this.edges, other.edges, Lists::concat);
        var newFilters = Lists.concat(this.filters, other.filters);
        var hasCrossJoin = this.hasCrossJoin || other.hasCrossJoin();

        return new JoinGraph(
            newNodes,
            newEdges,
            newFilters,
            hasCrossJoin
        );
    }

    JoinGraph withEdges(Map<LogicalPlan, List<Edge>> edges) {
        var newEdges = Maps.merge(this.edges, edges, Lists::concat);
        return new JoinGraph(this.nodes, newEdges, this.filters, this.hasCrossJoin);
    }

    JoinGraph withFilters(List<Symbol> filters) {
        if (filters.isEmpty()) {
            return this;
        }
        var newFilters = Lists.concat(this.filters, filters);
        return new JoinGraph(this.nodes, edges, newFilters, this.hasCrossJoin);
    }

    JoinGraph withCrossJoin() {
        return new JoinGraph(this.nodes, edges, filters, true);
    }

    public int size() {
        return nodes.size();
    }

    public List<Edge> edges(LogicalPlan node) {
        return edges.getOrDefault(node, List.of());
    }

    public static JoinGraph create(LogicalPlan plan, UnaryOperator<LogicalPlan> resolvePlan) {
        return plan.accept(new GraphBuilder(resolvePlan), new LinkedHashMap<>());
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
            JoinGraph source = filter.source().accept(this, context);
            return source.withFilters(List.of(filter.query()));
        }

        @Override
        public JoinGraph visitJoinPlan(JoinPlan joinPlan, Map<Symbol, LogicalPlan> context) {
            JoinGraph left = joinPlan.lhs().accept(this, context);
            JoinGraph right = joinPlan.rhs().accept(this, context);

            if (joinPlan.joinType() == JoinType.CROSS) {
                return left.joinWith(right).withCrossJoin();
            }

            Symbol joinCondition = joinPlan.joinCondition();
            if (joinPlan.joinType() != JoinType.INNER) {
                JoinGraph result = left.joinWith(right);
                return joinCondition == null
                    ? result
                    : result.withFilters(List.of(joinCondition));
            }

            ArrayList<Symbol> filters = new ArrayList<>();
            Map<LogicalPlan, List<Edge>> edges;
            if (joinCondition == null) {
                edges = Map.of();
            } else {
                var edgeCollector = new EdgeCollector(context);
                Map<Set<RelationName>, Symbol> split = QuerySplitter.split(joinCondition);
                for (var entry : split.entrySet()) {
                    Set<RelationName> relations = entry.getKey();
                    Symbol expression = entry.getValue();
                    // we are only interested in equi-join conditions between
                    // two tables e.g.: a.x = b.y will result in
                    // (a,b) -> (a.x = b.y) and we can ignore any other
                    // filters. Therefore, we only want entries where we have
                    // two keys.
                    if (relations.size() == 2 && isEquiJoin(expression)) {
                        expression.accept(edgeCollector, null);
                    } else {
                        filters.add(expression);
                    }
                }
                edges = edgeCollector.edges;
                assert (!edges.isEmpty() || !filters.isEmpty())
                    : "Must have either edges or filters - otherwise we'd be dropping the join condition";
            }
            return left
                .joinWith(right)
                .withEdges(edges)
                .withFilters(filters);
        }

        private static class EdgeCollector extends SymbolVisitor<Set<LogicalPlan>, Void> {

            private final Map<LogicalPlan, List<Edge>> edges = new HashMap<>();
            private final Map<Symbol, LogicalPlan> outputsToPlan;

            private EdgeCollector(Map<Symbol, LogicalPlan> outputsToPlan) {
                this.outputsToPlan = outputsToPlan;
            }

            @Override
            public Void visitField(ScopedSymbol s, Set<LogicalPlan> sources) {
                if (sources != null) {
                    LogicalPlan logicalPlan = outputsToPlan.get(s);
                    assert logicalPlan != null : "ScopedSymbol part of joinCondition must exist in outputsToPlan";
                    sources.add(logicalPlan);
                }
                return null;
            }

            @Override
            public Void visitReference(Reference ref, Set<LogicalPlan> sources) {
                if (sources != null) {
                    LogicalPlan logicalPlan = outputsToPlan.get(ref);
                    assert logicalPlan != null : "Reference part of joinCondition must exist in outputsToPlan";
                    sources.add(logicalPlan);
                }
                return null;
            }

            @Override
            public Void visitFunction(io.crate.expression.symbol.Function f, Set<LogicalPlan> sources) {
                List<Symbol> arguments = f.arguments();
                if (f.name().equals(EqOperator.NAME)) {
                    var lhsSymbol = arguments.get(0);
                    var rhsSymbol = arguments.get(1);

                    Set<LogicalPlan> lhsRelations = new HashSet<>();
                    lhsSymbol.accept(this, lhsRelations);

                    Set<LogicalPlan> rhsRelations = new HashSet<>();
                    rhsSymbol.accept(this, rhsRelations);

                    for (LogicalPlan lhsRelation : lhsRelations) {
                        for (LogicalPlan rhsRelation : rhsRelations) {
                            addEdge(lhsRelation, new Edge(rhsRelation, lhsSymbol, rhsSymbol));
                            addEdge(rhsRelation, new Edge(lhsRelation, lhsSymbol, rhsSymbol));
                        }
                    }
                } else {
                    arguments.forEach(arg -> arg.accept(this, sources));
                }
                return null;
            }

            private void addEdge(LogicalPlan from, Edge edge) {
                var values = edges.get(from);
                if (values == null) {
                    values = List.of(edge);
                } else {
                    values = new ArrayList<>(values);
                    values.add(edge);
                }
                edges.put(from, values);
            }
        }
    }
}
