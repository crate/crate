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

import io.crate.metadata.NodeContext;
import io.crate.metadata.TransactionContext;
import io.crate.planner.operators.Eval;
import io.crate.planner.operators.JoinPlan;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.optimizer.Rule;
import io.crate.planner.optimizer.costs.PlanStats;
import io.crate.planner.optimizer.joinorder.JoinGraph;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.planner.optimizer.matcher.Pattern;

public class EliminateCrossJoin implements Rule<JoinPlan> {

    private final Pattern<JoinPlan> pattern = typeOf(JoinPlan.class).with(j -> j.crossJoinEliminationRulePassed() == false);

    @Override
    public Pattern<JoinPlan> pattern() {
        return pattern;
    }

    @Override
    public LogicalPlan apply(JoinPlan plan,
                             Captures captures,
                             PlanStats planStats,
                             TransactionContext txnCtx,
                             NodeContext nodeCtx,
                             Function<LogicalPlan, LogicalPlan> resolvePlan) {
        JoinPlan join = (JoinPlan) planStats.memo().extract(plan);
        if (join.getRelationNames().size() >= 3) {
            var joinGraph = JoinGraph.create(join, resolvePlan);
            if (joinGraph.hasCrossJoin()) {
                var originalOrder = joinGraph.nodes();
                var newOrder = eliminateCrossJoin(joinGraph);
                if (originalOrder.equals(newOrder) == false) {
                    var newJoinPlan = joinGraph.reorder(newOrder);
                    return Eval.create(
                        newJoinPlan,
                        join.outputs()
                    );
                }
            }
        }
        return new JoinPlan(
            join.lhs(),
            join.rhs(),
            join.joinType(),
            join.joinCondition(),
            join.isFiltered(),
            true
        );
    }

    /**
     * Cross-joins are eliminated by traversing the graph over the edges
     * which are based on inner-joins. Any graph traversal algorithm could be used
     * here but we want to preserve to the original JoinOrder as mush as it is possible.
     * Therefore we use a PriorityQueue where the priority of the node is the position of
     * the original join order.
     **/
    static List<LogicalPlan> eliminateCrossJoin(JoinGraph joinGraph) {
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
                    try {
                        nodesToVisit.add(edge.to());
                    } catch(Exception e) {
                        System.out.println();
                    }
                }
            }
            if (nodesToVisit.isEmpty() && visited.size() < joinGraph.size()) {
                // disconnected graph, find new starting point
                for (var graphNode :  joinGraph.nodes()) {
                    if (visited.contains(graphNode) == false) {
                        nodesToVisit.add(graphNode);
                    }
                }
            }
        }
        assert visited.size() == joinGraph.size() : "Invalid state, each node needs to be visited";
        return newJoinOrder;
    }
}
