/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.planner.operators;

import com.google.common.base.Preconditions;
import io.crate.planner.operators.PushDownDefinition.Node.Type;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Supplier;

/**
 * A "push-down" context which verifies a plan/operator can be moved towards
 * the leafs of the {@link LogicalPlan}. That means the operation will be
 * executed earlier and possibly make queries execute faster.
 *
 * For example, pushing down an *Order*:
 *
 *              *Order*                      Union
 *                 |                        /     \
 *                 |                       /       \
 *               Union                 *Order*  *Order*
 *              /     \                   |        |
 *             /       \        =>        |        |
 *          Collect   Order             Collect  Order
 *                      |                          |
 *                      |                          |
 *                   Collect                    Collect
 *
 *
 * This class accepts a partial tree which holds the classes of {@link LogicalPlan}s.
 * While traversing the plan hierarchy, the matching is performed against this tree.
 * The results of the push-down have to be checked with the {@code matches} method
 * to ensure that the graph traversal let to a valid push down scenario.
 */
public class PushDownDefinition {

    static final Supplier<PushDownDefinition> UNION_ORDER_BY_PUSH_DOWN;

    static {
        UNION_ORDER_BY_PUSH_DOWN = () -> {
            Node pattern =
                new Node(Order.class, Type.PUSHDOWN,
                    new Node(RelationBoundary.class, Type.REGULAR,
                        new Node(Union.class, Type.REGULAR,
                            new Node(RelationBoundary.class, Type.REGULAR,
                                new Node(Collect.class, Type.TARGET)),
                            new Node(RelationBoundary.class,  Type.REGULAR,
                                new Node(Collect.class, Type.TARGET))
                        )
                    )
                );
            return new PushDownDefinition(pattern);
        };
    }

    static class Node {

        enum Type {
            // Just a pattern matching node
            REGULAR,
            // A node to be pushed down
            PUSHDOWN,
            // A target for the push down node
            TARGET
        }

        private final Class<? extends LogicalPlan> logicalPlanClass;

        private final Type nodeType;

        private final Node[] children;

        Node(Class<? extends LogicalPlan> logicalPlanClass, Type nodeType, Node... children) {
            this.logicalPlanClass = logicalPlanClass;
            this.nodeType = nodeType;
            this.children = children;
        }

    }

    private final Deque<Node> toBeVisited;

    /** We can only push down plans with one input */
    private OneInputPlan pushDownPlan;
    private final Set<LogicalPlan> targetPlans;

    private boolean matchPossible;
    private boolean patternFound;

    private PushDownDefinition(Node treePattern) {
        this.toBeVisited = new ArrayDeque<>();
        this.targetPlans = new HashSet<>();
        toBeVisited.push(treePattern);
        matchPossible = true;
    }

    /**
     * Accepts a new {@link LogicalPlan} as part of the plan hierarchy.
     * This methods needs to be called by every plan/operator.
     * The traversal has to be performed in depth-first order.
     * @param plan The (sub)plan to match against the tree pattern.
     */
    public void accept(LogicalPlan plan) {
        Preconditions.checkNotNull(plan, "Plan must never be null");
        if (!matchPossible || patternFound) {
            return;
        }
        Node currentNode = toBeVisited.removeFirst();
        if (currentNode.logicalPlanClass.equals(plan.getClass())) {
            if (currentNode.nodeType == Type.PUSHDOWN) {
                Preconditions.checkState(pushDownPlan == null,
                    "Only one push down allowed per PushDownDefinition");
                if (plan instanceof OneInputPlan) {
                    pushDownPlan = (OneInputPlan) plan;
                }
            } else if (currentNode.nodeType == Type.TARGET) {
                targetPlans.add(plan);
            }
            for (int i = currentNode.children.length - 1; i >= 0; i--) {
                // add children right to left to ensure left is traversed first
                toBeVisited.addFirst(currentNode.children[i]);
            }
            if (toBeVisited.isEmpty()) {
                patternFound = true;
            }
        } else {
            matchPossible = false;
        }
    }

    boolean pushDown(LogicalPlan plan) {
        Preconditions.checkNotNull(plan, "Plan must never be null");
        return matchPossible && pushDownPlan == plan;
    }

    /**
     * Performs the actual push-down on returnPlan, if necessary.
     * @param original The target plan which is the child of the new push down location.
     * @param returnPlan The updated plan that should be used when inserting a push down plan.
     * @return The newly updated returnPlan {@link LogicalPlan} if a push down was performed.
     */
    LogicalPlan insertPushDownPlan(LogicalPlan original, LogicalPlan returnPlan) {
        Preconditions.checkNotNull(original, "Plan must never be null");
        if (!matchPossible) {
            return returnPlan;
        }
        if (targetPlans.contains(original)) {
            if (pushDownPlan != null) {
                return pushDownPlan.newInstance(returnPlan);
            } else {
                matchPossible = false;
            }
        }
        return returnPlan;
    }

    /**
     * Get the status of the matching of this definition against the input tree.
     * @return True if the definition matched and the push down has been performed correctly.
     */
    public boolean matches() {
        return patternFound;
    }

}
