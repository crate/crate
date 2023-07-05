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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.jetbrains.annotations.Nullable;

import io.crate.analyze.relations.QuerySplitter;
import io.crate.expression.operator.EqOperator;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.format.Style;
import io.crate.planner.operators.Collect;
import io.crate.planner.operators.HashJoin;
import io.crate.planner.operators.JoinPlan;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.operators.LogicalPlanVisitor;
import io.crate.planner.operators.NestedLoopJoin;
import io.crate.planner.operators.Rename;
import io.crate.planner.optimizer.iterative.GroupReference;

public class Graph {

    private final LogicalPlan root;
    private final List<LogicalPlan> nodes;
    private final Map<Integer, LogicalPlan> nodesById;
    private final Map<Integer, Set<Edge>> edges;

    public Graph(LogicalPlan root, List<LogicalPlan> nodes, Map<Integer, Set<Edge>> edges) {
        this.root = root;
        this.edges = edges;
        this.nodes = nodes;
        this.nodesById = new HashMap<>();
        for (LogicalPlan node : nodes) {
            this.nodesById.put(node.id(), node);
        }
    }

    public Graph joinWith(LogicalPlan root, Graph other, Map<Integer, Set<Edge>> moreEdges) {
        for (LogicalPlan node : other.nodes) {
            assert !edges.containsKey(node) : "Nodes can not be in both graphs";
        }

        var newNodes = new ArrayList<LogicalPlan>();
        newNodes.addAll(nodes);
        newNodes.addAll(other.nodes);

        var newEdges = new HashMap<Integer, Set<Edge>>();
        newEdges.putAll(edges);
        newEdges.putAll(other.edges);
        newEdges.putAll(moreEdges);


        return new Graph(root, newNodes, newEdges);
    }

    public List<LogicalPlan> nodes() {
        return nodes;
    }

    public Map<Integer, Set<Edge>> edges() {
        return edges;
    }

    public LogicalPlan root() {
        return root;
    }

    public int size() {
        return nodes.size();
    }

    @Nullable
    public LogicalPlan nodeByPosition(int i) {
        return nodes.get(i);
    }

    @Nullable
    public LogicalPlan nodeById(int i) {
        return nodesById.get(i);
    }

    public Collection<Edge> getEdges(LogicalPlan node) {
        var result = edges.get(node.id());
        if (result == null) {
            return null;
        }
        return result;
    }

    public record Edge(LogicalPlan from, Symbol fromVariable, LogicalPlan to, Symbol toVariable) {

        public Set<Integer> ids() {
            return Set.of(from.id(), to.id());
        }

        @Override
            public String toString() {
                return "Edge{" +
                       "from=" + fromVariable.toString(Style.QUALIFIED) +
                       ", to=" + toVariable.toString(Style.QUALIFIED) +
                       '}';
            }
        }

    public static Graph create(LogicalPlan plan, Function<LogicalPlan, LogicalPlan> resolvePlan) {
        var visitor = new Visitor(resolvePlan);
        var context = new HashMap<Symbol, LogicalPlan>();
        return plan.accept(visitor, context);
    }

    private static class Visitor extends LogicalPlanVisitor<Map<Symbol, LogicalPlan>, Graph> {

        private final Function<LogicalPlan, LogicalPlan> resolvePlan;

        public Visitor(Function<LogicalPlan, LogicalPlan> resolvePlan) {
            this.resolvePlan = resolvePlan;
        }

        @Override
        public Graph visitPlan(LogicalPlan logicalPlan, Map<Symbol, LogicalPlan> context) {
            for (LogicalPlan source : logicalPlan.sources()) {
                source.accept(this, context);
            }
            return new Graph(logicalPlan, List.of(logicalPlan), Map.of());
        }

        @Override
        public Graph visitCollect(Collect collect, Map<Symbol, LogicalPlan> context) {
            for (Symbol output : collect.outputs()) {
                context.put(output, collect);
            }
            return new Graph(collect, List.of(collect), Map.of());
        }

        @Override
        public Graph visitGroupReference(GroupReference groupReference, Map<Symbol, LogicalPlan> context) {
            var resolved = resolvePlan.apply(groupReference);
            return resolved.accept(this, context);
        }

        //TODO Handle filters

        @Override
        public Graph visitJoinPlan(JoinPlan joinPlan, Map<Symbol, LogicalPlan> context) {
            var left = joinPlan.lhs().accept(this, context);
            var right = joinPlan.rhs().accept(this, context);

            var joinCondition = joinPlan.joinCondition();
            // if join condition is null, we have a cross-join
//            assert joinCondition != null : "Join condition cannot be null to build graph";
            if (joinCondition != null) {
                var edges = new HashMap<Integer, Set<Edge>>();
                // find equi-join conditions such as `a.x = b.y` and create edges
                // TODO deal with the rest of the filters such as `a.x >= 1`
                var split = QuerySplitter.split(joinCondition);
                for (var entry : split.entrySet()) {
                    if (entry.getKey().size() == 2) {
                        if (entry.getValue() instanceof io.crate.expression.symbol.Function f) {
                            if (f.name().equals(EqOperator.NAME)) {
                                var fromSymbol = f.arguments().get(0);
                                var toSymbol = f.arguments().get(1);
                                var from = context.get(fromSymbol);
                                var to = context.get(toSymbol);
                                assert from != null & to != null :
                                    "Invalid join condition to build graph " + joinCondition.toString(Style.QUALIFIED);
                                var edge = new Edge(from, fromSymbol, to, toSymbol);
                                insertEdge(edges, edge);
                            }
                        }
                    }
                }
                return left.joinWith(joinPlan, right, edges);
            }
            return left.joinWith(joinPlan, right, Map.of());
        }

        private static void insertEdge(Map<Integer, Set<Edge>> edges, Edge edge) {
            for (var id : edge.ids()) {
                var result = edges.get(id);
                if (result == null) {
                    result = Set.of(edge);
                } else {
                    result = new HashSet<>(result);
                    result.add(edge);
                }
                edges.put(id, result);
            }
        }
    }

}
