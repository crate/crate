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
import java.util.function.UnaryOperator;

import io.crate.analyze.relations.QuerySplitter;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.RelationName;
import io.crate.planner.operators.AbstractJoinPlan;
import io.crate.planner.operators.EquiJoinDetector;
import io.crate.planner.operators.Eval;
import io.crate.planner.operators.Filter;
import io.crate.planner.operators.JoinPlan;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.optimizer.Rule;
import io.crate.planner.optimizer.iterative.GroupReferenceResolver;
import io.crate.planner.optimizer.matcher.Capture;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.planner.optimizer.matcher.Pattern;
import io.crate.sql.tree.JoinType;

/**
 * Reorders the leaves of a CROSS-join chain sitting directly beneath a
 * {@link Filter}, using equi-join conjuncts found in that Filter's own
 * predicate as graph edges -- not just conditions that already happen to be
 * attached to an INNER {@link JoinPlan} (which is all {@code JoinGraph} sees;
 * a Filter's content is opaque to it).
 *
 * <p>
 * This rule does not change join types or the number of cross joins: every
 * pair in the rebuilt chain stays a CROSS join, and the whole original Filter
 * is kept, unmodified, on top. It only changes the left-to-right order of the
 * chain's leaves, so that relations connected by a not-yet-promoted equi-join
 * predicate end up adjacent -- which is what lets a later pass (e.g.
 * {@link RewriteFilterOnCrossJoinToInnerJoin} / {@link EliminateCrossJoin})
 * turn them into an actual INNER join.
 * </p>
 */
public class ReorderCrossJoinSidesByFilterOrder implements Rule<Filter> {

    private final Capture<JoinPlan> joinCapture = new Capture<>();
    private final Pattern<Filter> pattern = typeOf(Filter.class)
        .with(
            source(),
            typeOf(JoinPlan.class)
                .capturedAs(joinCapture)
                .with(j -> j.joinType() == JoinType.CROSS));

    @Override
    public Pattern<Filter> pattern() {
        return pattern;
    }

    private static JoinPlan resolveFully(JoinPlan join, UnaryOperator<LogicalPlan> resolvePlan) {
        return (JoinPlan) GroupReferenceResolver.resolveFully(resolvePlan, join);
    }

    @Override
    public LogicalPlan apply(Filter filter, Captures captures, Rule.Context context) {
        JoinPlan crossJoin = resolveFully(captures.get(joinCapture), context.resolvePlan());
        List<LogicalPlan> nodes = AbstractJoinPlan.orderedPlans(crossJoin);
        if (nodes.size() < 3) {
            return null;
        }

        Map<RelationName, LogicalPlan> relationToNode = new HashMap<>();
        for (LogicalPlan node : nodes) {
            for (RelationName relationName : node.relationNames()) {
                relationToNode.put(relationName, node);
            }
        }

        Map<LogicalPlan, List<LogicalPlan>> edges = new HashMap<>();
        for (var entry : QuerySplitter.split(filter.query()).entrySet()) {
            Set<RelationName> relations = entry.getKey();
            Symbol conjunct = entry.getValue();
            if (relations.size() == 2 && EquiJoinDetector.isEquiJoin(conjunct)) {
                addEdge(edges, relationToNode, relations);
            }
        }
        if (edges.isEmpty()) {
            return null;
        }

        List<LogicalPlan> newOrder = orderNodes(nodes, edges);
        if (newOrder.equals(nodes)) {
            // Nothing to gain by rebuilding into the same order.
            return null;
        }

        LogicalPlan rebuilt = rebuild(newOrder);
        LogicalPlan withFilter = Filter.create(rebuilt, filter.query());
        return Eval.create(withFilter, crossJoin.outputs());
    }

    private static void addEdge(Map<LogicalPlan, List<LogicalPlan>> edges,
                                Map<RelationName, LogicalPlan> relationToNode,
                                Set<RelationName> relations) {
        var it = relations.iterator();
        LogicalPlan node1 = relationToNode.get(it.next());
        LogicalPlan node2 = relationToNode.get(it.next());
        if (node1 == null || node2 == null || node1 == node2) {
            // Either relation isn't a leaf of this cross-join chain (e.g. it
            // belongs to an outer scope), or both sides already sit on the
            // same leaf -- nothing to connect.
            return;
        }
        edges.computeIfAbsent(node1, ignored -> new ArrayList<>()).add(node2);
        edges.computeIfAbsent(node2, ignored -> new ArrayList<>()).add(node1);
    }

    /**
     * Same traversal as {@link EliminateCrossJoin#orderNodes}: a priority
     * queue keyed by original position, preferring to preserve original
     * order where the graph allows it.
     */
    private static List<LogicalPlan> orderNodes(List<LogicalPlan> nodes, Map<LogicalPlan, List<LogicalPlan>> edges) {
        Map<LogicalPlan, Integer> priorities = new HashMap<>();
        for (int i = 0; i < nodes.size(); i++) {
            priorities.put(nodes.get(i), i);
        }

        List<LogicalPlan> newOrder = new ArrayList<>();
        PriorityQueue<LogicalPlan> nodesToVisit = new PriorityQueue<>(nodes.size(), comparing(priorities::get));
        Set<LogicalPlan> visited = new HashSet<>();
        nodesToVisit.add(nodes.get(0));

        while (!nodesToVisit.isEmpty()) {
            LogicalPlan node = nodesToVisit.poll();
            if (visited.add(node)) {
                newOrder.add(node);
                for (LogicalPlan neighbor : edges.getOrDefault(node, List.of())) {
                    nodesToVisit.add(neighbor);
                }
            }
            if (nodesToVisit.isEmpty() && visited.size() < nodes.size()) {
                for (LogicalPlan candidate : nodes) {
                    if (!visited.contains(candidate)) {
                        nodesToVisit.add(candidate);
                    }
                }
            }
        }
        return newOrder;
    }

    /** Rebuilds a left-deep chain of CROSS joins only -- join type/count never changes. */
    private static LogicalPlan rebuild(List<LogicalPlan> order) {
        LogicalPlan result = order.get(0);
        for (int i = 1; i < order.size(); i++) {
            result = new JoinPlan(result, order.get(i), JoinType.CROSS, null);
        }
        return result;
    }
}
