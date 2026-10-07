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
import static io.crate.planner.optimizer.matcher.Patterns.source;
import static java.util.Comparator.comparing;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

import org.jspecify.annotations.Nullable;

import io.crate.expression.operator.AndOperator;
import io.crate.expression.operator.EqOperator;
import io.crate.expression.symbol.Symbol;
import io.crate.planner.operators.Eval;
import io.crate.planner.operators.Filter;
import io.crate.planner.operators.JoinPlan;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.optimizer.Rule;
import io.crate.planner.optimizer.joinorder.JoinGraph;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.planner.optimizer.matcher.Pattern;
import io.crate.sql.tree.JoinType;

public class EliminateCrossJoin implements Rule<LogicalPlan> {

    private final Pattern<LogicalPlan> pattern = typeOf(JoinPlan.class)
        .or()
        .typeOf(Filter.class)
        .with(source(), typeOf(JoinPlan.class));

    @Override
    public Pattern<LogicalPlan> pattern() {
        return pattern;
    }

    @Override
    public LogicalPlan apply(LogicalPlan filterOrJoin,
                             Captures captures,
                             Rule.Context context) {
        var joinGraph = JoinGraph.create(filterOrJoin, context.resolvePlan());
        if (!joinGraph.hasCrossJoin()) {
            return null;
        }
        List<LogicalPlan> newOrder = orderNodes(joinGraph);

        if (!isBetterThanOriginal(joinGraph, newOrder)) {
            return null;
        }

        LogicalPlan newJoinPlan = rebuild(joinGraph, newOrder);
        if (newJoinPlan == null) {
            return null;
        }

        return Eval.create(newJoinPlan, filterOrJoin.outputs());
    }

    /**
     * Cross-joins are eliminated by traversing the graph over the edges
     * which are based on inner-joins. Any graph traversal algorithm could be used,
     * but we want to preserve to the original order as much as possible.
     * Therefore, we use a PriorityQueue where the priority of the node is the position of
     * the original join order.
     **/
    static List<LogicalPlan> orderNodes(JoinGraph joinGraph) {
        Map<LogicalPlan, Integer> position = new HashMap<>();
        for (int i = 0; i < joinGraph.size(); i++) {
            position.put(joinGraph.nodes().get(i), i);
        }
        // Connected nodes/plans which aren't joined yet, lowest original position first
        PriorityQueue<LogicalPlan> candidates = new PriorityQueue<>(comparing(position::get));
        Set<LogicalPlan> joined = new HashSet<>();
        List<LogicalPlan> order = new ArrayList<>(joinGraph.size());
        int firstUnjoined = 0;
        while (order.size() < joinGraph.size()) {
            if (candidates.isEmpty()) {
                // Nothing is connected to the joined nodes/plans.
                // Only add the first unjoined one, so its neighbours are preferred again afterwards.
                while (joined.contains(joinGraph.nodes().get(firstUnjoined))) {
                    firstUnjoined++;
                }
                candidates.add(joinGraph.nodes().get(firstUnjoined));
            }
            LogicalPlan node = candidates.poll();
            if (joined.add(node)) {
                order.add(node);
                for (JoinGraph.Edge edge : joinGraph.edges(node)) {
                    if (!joined.contains(edge.to())) {
                        candidates.add(edge.to());
                    }
                }
            }
        }
        return order;
    }

    private static boolean isBetterThanOriginal(JoinGraph joinGraph, List<LogicalPlan> order) {
        List<Integer> positions = crossJoinPositions(joinGraph, order);
        if (positions.size() != joinGraph.originalCrossJoins()) {
            return positions.size() < joinGraph.originalCrossJoins();
        }
        List<Integer> originalPositions = crossJoinPositions(joinGraph, joinGraph.nodes());
        for (int i = 0; i < Math.min(positions.size(), originalPositions.size()); i++) {
            int cmp = Integer.compare(positions.get(i), originalPositions.get(i));
            if (cmp != 0) {
                return cmp > 0;
            }
        }
        return false;
    }

    private static List<Integer> crossJoinPositions(JoinGraph joinGraph, List<LogicalPlan> order) {
        Set<LogicalPlan> joined = new HashSet<>();
        joined.add(order.getFirst());
        List<Integer> positions = new ArrayList<>();
        for (int i = 1; i < order.size(); i++) {
            LogicalPlan node = order.get(i);
            boolean connected = false;
            for (JoinGraph.Edge edge : joinGraph.edges(node)) {
                if (joined.contains(edge.to())) {
                    connected = true;
                    break;
                }
            }
            if (!connected) {
                positions.add(i);
            }
            joined.add(node);
        }
        return positions;
    }


    @Nullable
    static LogicalPlan rebuild(JoinGraph graph, List<LogicalPlan> order) {
        assert order.size() == graph.size() : "Order must contain all nodes/plans";
        LogicalPlan result = order.getFirst();
        Set<LogicalPlan> joined = new HashSet<>();
        joined.add(result);
        for (LogicalPlan node : order.subList(1, order.size())) {
            List<Symbol> joinConditions = new ArrayList<>();
            for (JoinGraph.Edge edge : graph.edges(node)) {
                if (joined.contains(edge.to())) {
                    joinConditions.add(EqOperator.of(edge.left(), edge.right()));
                }
            }
            result = joinConditions.isEmpty()
                ? new JoinPlan(result, node, JoinType.CROSS, null)
                : new JoinPlan(result, node, JoinType.INNER, AndOperator.join(joinConditions));
            joined.add(node);
        }
        for (Symbol leftover : graph.filters()) {
            result = new Filter(result, leftover);
        }
        return result;
    }
}
