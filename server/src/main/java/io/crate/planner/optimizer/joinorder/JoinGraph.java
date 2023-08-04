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
import java.util.function.Function;


import io.crate.analyze.relations.FieldResolver;
import io.crate.analyze.relations.QuerySplitter;
import io.crate.common.collections.Lists2;
import io.crate.common.collections.Maps;
import io.crate.exceptions.InvalidArgumentException;
import io.crate.expression.operator.AndOperator;
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
import io.crate.planner.operators.Rename;
import io.crate.planner.optimizer.iterative.GroupReference;
import io.crate.sql.tree.JoinType;

public record JoinGraph(
    LogicalPlan root,
    List<LogicalPlan> nodes,
    Map<LogicalPlan, Set<Edge>> edges,
    List<Symbol> filters,
    boolean hasCrossJoin) {

    public record Edge(LogicalPlan to, Symbol left, Symbol right) {};

    JoinGraph joinWith(LogicalPlan root, JoinGraph other) {
        for (var node : other.nodes) {
            assert !edges.containsKey(node) : "LogicalPlan" + node + " can't be in both graphs";
        }

        var newNodes = Lists2.concat(this.nodes, other.nodes);
        var newEdges = Maps.concatMultiMap(this.edges, other.edges);
        var newFilters = Lists2.concat(this.filters, other.filters);
        var hasCrossJoin = this.hasCrossJoin || other.hasCrossJoin();

        return new JoinGraph(
            root,
            newNodes,
            newEdges,
            newFilters,
            hasCrossJoin
        );
    }

    JoinGraph withEdges(Map<LogicalPlan, Set<Edge>> edges) {
        var newEdges = Maps.concatMultiMap(this.edges, edges);
        return new JoinGraph(this.root, this.nodes, newEdges, this.filters, this.hasCrossJoin);
    }

    JoinGraph withFilters(List<Symbol> filters) {
        var newFilters = Lists2.concat(this.filters, filters);
        return new JoinGraph(this.root, this.nodes, edges, newFilters, this.hasCrossJoin);
    }

    JoinGraph withCrossJoin(boolean crossJoin) {
        var newFilters = Lists2.concat(this.filters, filters);
        return new JoinGraph(this.root, this.nodes, edges, newFilters, crossJoin);
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

    public LogicalPlan buildLogicalPlan() {
        return reorder(this.nodes);
    }

    public LogicalPlan reorder(List<LogicalPlan> order) {
        var result = order.get(0);
        var alreadyJoinedNodes = new HashSet<LogicalPlan>();
        alreadyJoinedNodes.add(result);

        for (int i = 1; i < order.size(); i++) {
            var rightNode = order.get(i);
            alreadyJoinedNodes.add(rightNode);

            var criteria = new ArrayList<Symbol>();

            for (var edge : this.edges(rightNode)) {
                var toNode = edge.to();
                if (alreadyJoinedNodes.contains(toNode)) {
                    criteria.add(EqOperator.of(edge.left(), edge.right()));
                }
            }

            if(criteria.isEmpty()) {
               var errorMessage = new ArrayList<String>();
                for (var plan : order) {
                    for (var relationName : plan.getRelationNames()) {
                        errorMessage.add(relationName.fqn());
                    }
                }
                throw new InvalidArgumentException("JoinPlan cannot be built with the provided order " + errorMessage);
            }

            result = new JoinPlan(
                result,
                rightNode,
                JoinType.INNER,
                AndOperator.join(criteria, null),
                false,
                true
            );
        }

        for (var filter : this.filters()) {
            result = new Filter(result, filter);
        }
        return result;
    }

    public static JoinGraph create(LogicalPlan plan, Function<LogicalPlan, LogicalPlan> resolvePlan) {
        return plan.accept(new GraphBuilder(resolvePlan), new GraphBuilder.Context());
    }

    private static class GraphBuilder extends LogicalPlanVisitor<GraphBuilder.Context, JoinGraph> {

        private static class Context {
            final Map<Symbol, LogicalPlan> referencesToRelations = new HashMap<>();
            final Map<RelationName, FieldResolver> resolver = new HashMap<>();
        }

        private final Function<LogicalPlan, LogicalPlan> resolvePlan;

        GraphBuilder(Function<LogicalPlan, LogicalPlan> resolvePlan) {
            this.resolvePlan = resolvePlan;
        }

        @Override
        public JoinGraph visitPlan(LogicalPlan logicalPlan, Context context) {
            for (Symbol output : logicalPlan.outputs()) {
                context.referencesToRelations.put(output, logicalPlan);
            }
            for (LogicalPlan source : logicalPlan.sources()) {
                source.accept(this, context);
            }
            return new JoinGraph(logicalPlan, List.of(logicalPlan), Map.of(), List.of(), false);
        }

        @Override
        public JoinGraph visitRename(Rename rename, Context context) {
            context.resolver.put(rename.name(), rename);
            return rename.source().accept(this,context);
        }

        @Override
        public JoinGraph visitGroupReference(GroupReference groupReference, Context context) {
            return resolvePlan.apply(groupReference).accept(this, context);
        }

        @Override
        public JoinGraph visitFilter(Filter filter, Context context) {
            var source = filter.source().accept(this, context);
            return source.withFilters(List.of(filter.query()));
        }

        @Override
        public JoinGraph visitJoinPlan(JoinPlan joinPlan, Context context) {

            var left = joinPlan.lhs().accept(this, context);
            var right = joinPlan.rhs().accept(this, context);

            if (joinPlan.joinType() == JoinType.CROSS) {
                return left.joinWith(joinPlan, right).withCrossJoin(true);
            }

            if (joinPlan.joinType() != JoinType.INNER) {
                return visitPlan(joinPlan, context);
            }

            var joinCondition = joinPlan.joinCondition();
            var edgeCollector = new EdgeCollector();
            if (joinCondition != null) {
                var split = QuerySplitter.split(joinCondition);
                for (var entry : split.entrySet()) {
                    if (entry.getKey().size() == 2) {
                        entry.getValue().accept(edgeCollector, context);
                    }
                }
            }
            return left.joinWith(joinPlan, right).withEdges(edgeCollector.edges);
        }

        private static class EdgeCollector extends SymbolVisitor<Context, LogicalPlan> {

            final Map<LogicalPlan, Set<Edge>> edges = new HashMap<>();

            @Override
            public LogicalPlan visitField(ScopedSymbol s, Context context) {
                var fieldResolver = context.resolver.get(s.relation());
                var resolved = fieldResolver.resolveField(s);
                return context.referencesToRelations.get(resolved);
            }

            @Override
            public LogicalPlan visitReference(Reference ref, Context context) {
                 return context.referencesToRelations.get(ref);
            }

            @Override
            public LogicalPlan visitFunction(io.crate.expression.symbol.Function f, Context context) {
                if (f.name().equals(EqOperator.NAME)) {
                    var fromSymbol = f.arguments().get(0);
                    var toSymbol = f.arguments().get(1);
                    var fromRelation = fromSymbol.accept(this, context);
                    var toRelation = toSymbol.accept(this, context);
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
                } else {
                    f.arguments().forEach(x -> x.accept(this, context));
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
