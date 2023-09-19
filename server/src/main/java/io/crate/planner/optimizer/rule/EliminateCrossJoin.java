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

package io.crate.planner.optimizer.rule;

import static io.crate.planner.optimizer.matcher.Pattern.typeOf;
import static java.util.Comparator.comparing;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;
import java.util.function.Function;

import org.jetbrains.annotations.Nullable;

import io.crate.exceptions.InvalidArgumentException;
import io.crate.expression.operator.AndOperator;
import io.crate.expression.operator.EqOperator;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.NodeContext;
import io.crate.metadata.TransactionContext;
import io.crate.planner.operators.Eval;
import io.crate.planner.operators.Filter;
import io.crate.planner.operators.JoinPlan;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.optimizer.Rule;
import io.crate.planner.optimizer.costs.PlanStats;
import io.crate.planner.optimizer.joinorder.JoinGraph;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.planner.optimizer.matcher.Pattern;
import io.crate.sql.tree.JoinType;

public class EliminateCrossJoin implements Rule<JoinPlan> {

    private final Pattern<JoinPlan> pattern = typeOf(JoinPlan.class);

    @Override
    public Pattern<JoinPlan> pattern() {
        return pattern;
    }

    @Override
    public LogicalPlan apply(JoinPlan join,
                             Captures captures,
                             PlanStats planStats,
                             TransactionContext txnCtx,
                             NodeContext nodeCtx,
                             Function<LogicalPlan, LogicalPlan> resolvePlan) {
        if (join.getRelationNames().size() >= 3) {
            var joinGraph = JoinGraph.create(join, resolvePlan);
            if (joinGraph.hasCrossJoin()) {
                var newOrder = eliminateCrossJoin(joinGraph);
                if (newOrder != null) {
                    var originalOrder = joinGraph.nodes();
                    if (originalOrder.equals(newOrder) == false) {
                        var newJoinPlan = reorder(joinGraph, newOrder);
                        if (newJoinPlan != null) {
                            return Eval.create(
                                newJoinPlan,
                                join.outputs()
                            );
                        }
                    }
                }
            }
        }
        return null;
    }

    /**
     * Cross-joins are eliminated by traversing the graph over the edges
     * which are based on inner-joins. Any graph traversal algorithm could be used,
     * but we want to preserve to the original order as much as possible.
     * Therefore, we use a PriorityQueue where the priority of the node is the position of
     * the original join order.
     **/
    @Nullable
    static List<LogicalPlan> eliminateCrossJoin(JoinGraph joinGraph) {
        if (joinGraph.edges().size() == 0) {
            return null;
        }
        // This is the minimum number of edges which we need to have to be able to visit each node in the graph
        if (joinGraph.nodes().size() >= (joinGraph.edges().size() / 2) - 1 == false) {
            return null;
        }

        var newJoinOrder = new ArrayList<LogicalPlan>();

        var priorities = new HashMap<LogicalPlan, Integer>();
        for (int i = 0; i < joinGraph.size(); i++) {
            priorities.put(joinGraph.nodes().get(i), i);
        }

        var nodesToVisit = new PriorityQueue<LogicalPlan>(joinGraph.size(), comparing(priorities::get));
        var visited = new HashSet<LogicalPlan>();

        nodesToVisit.add(joinGraph.nodes().get(0));

        while (!nodesToVisit.isEmpty()) {
            var node = nodesToVisit.poll();
            if (!visited.contains(node)) {
                visited.add(node);
                newJoinOrder.add(node);
                for (var edge : joinGraph.edges(node)) {
                    nodesToVisit.add(edge.to());
                }
            }
            if (nodesToVisit.isEmpty() && visited.size() < joinGraph.size()) {
                // disconnected graph, find new starting point
                for (var graphNode : joinGraph.nodes()) {
                    if (visited.contains(graphNode) == false) {
                        nodesToVisit.add(graphNode);
                    }
                }
            }
        }
        assert visited.size() == joinGraph.size() : "Invalid state, each node needs to be visited";
        return newJoinOrder;
    }

    @Nullable
    static LogicalPlan reorder(JoinGraph graph, List<LogicalPlan> order) {
        assert graph.nodes().size() == order.size() : "Size must be equal";

        if (graph.edges().isEmpty()) {
            throw new InvalidArgumentException("JoinPlan cannot be built with the provided order.");
        }

        var result = order.get(0);
        var alreadyJoinedNodes = new HashSet<LogicalPlan>();
        alreadyJoinedNodes.add(result);

        for (int i = 1; i < order.size(); i++) {
            var rightNode = order.get(i);
            alreadyJoinedNodes.add(rightNode);

            var criteria = new ArrayList<Symbol>();

            for (var edge : graph.edges(rightNode)) {
                var toNode = edge.to();
                if (alreadyJoinedNodes.contains(toNode)) {
                    criteria.add(EqOperator.of(edge.left(), edge.right()));
                }
            }

            if (criteria.isEmpty()) {
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
                false
            );
        }

        for (var filter : graph.filters()) {
            result = new Filter(result, filter);
        }
        return result;
    }
}
