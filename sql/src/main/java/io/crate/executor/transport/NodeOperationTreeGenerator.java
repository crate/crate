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

package io.crate.executor.transport;

import io.crate.operation.NodeOperation;
import io.crate.operation.NodeOperationTree;
import io.crate.planner.Merge;
import io.crate.planner.MultiPhasePlan;
import io.crate.planner.Plan;
import io.crate.planner.PlanVisitor;
import io.crate.planner.distribution.DistributionType;
import io.crate.planner.distribution.UpstreamPhase;
import io.crate.planner.node.ExecutionPhase;
import io.crate.planner.node.ExecutionPhases;
import io.crate.planner.node.dql.Collect;
import io.crate.planner.node.dql.CountPlan;
import io.crate.planner.node.dql.QueryThenFetch;
import io.crate.planner.node.dql.join.NestedLoop;

import javax.annotation.Nullable;
import java.util.*;

/**
 * Class used to generate a NodeOperationTree
 * <p>
 * <p>
 * E.g. a plan like NL:
 *
 * <pre>
 *              NL
 *           1 NLPhase
 *           2 MergePhase
 *        /               \
 *       /                 \
 *     QAF                 QAF
 *   3 CollectPhase      5 CollectPhase
 *   4 MergePhase        6 MergePhase
 * </pre>
 *
 * Will have a data flow like this:
 *
 * <pre>
 *   3 -- 4
 *          -- 1 -- 2
 *   5 -- 6
 * </pre>
 * The NodeOperation tree will have 5 NodeOperations (3-4, 4-1, 5-6, 6-1, 1-2)
 * And leaf will be 2 (the Phase which will provide the final result)
 * <p>
 * <p>
 * Implementation detail:
 * <p>
 * <p>
 * The phases are added in the following order
 * <p>
 * 2 - 1 [new branch 0]  4 - 3
 * [new branch 1]  5 - 6
 * <p>
 * every time addPhase is called a NodeOperation is added
 * that connects the previous phase (if there is one) to the current phase
 */
public final class NodeOperationTreeGenerator extends PlanVisitor<NodeOperationTreeGenerator.NodeOperationTreeContext, Void> {

    private final static NodeOperationTreeGenerator INSTANCE = new NodeOperationTreeGenerator();

    private NodeOperationTreeGenerator() {
    }

    private static class Branch {
        private final Deque<ExecutionPhase> phases = new ArrayDeque<>();
        private final byte inputId;

        Branch(byte inputId) {
            this.inputId = inputId;
        }
    }

    static class NodeOperationTreeContext {
        private final String localNodeId;
        private final List<NodeOperation> nodeOperations = new ArrayList<>();

        private final Deque<Branch> branches = new ArrayDeque<>();
        private final Branch root;
        private Branch currentBranch;

        NodeOperationTreeContext(String localNodeId) {
            this.localNodeId = localNodeId;
            root = new Branch((byte) 0);
            currentBranch = root;
        }

        /**
         * adds a Phase to the "NodeOperation execution tree"
         * should be called in the reverse order of how data flows.
         * <p>
         * E.g. in a plan where data flows from CollectPhase to MergePhase
         * it should be called first for MergePhase and then for CollectPhase
         */
        void addPhase(@Nullable ExecutionPhase executionPhase) {
            addPhase(executionPhase, nodeOperations, true);
        }

        void addContextPhase(@Nullable ExecutionPhase executionPhase) {
            addPhase(executionPhase, nodeOperations, false);
        }

        private void addPhase(@Nullable ExecutionPhase executionPhase,
                              List<NodeOperation> nodeOperations,
                              boolean setDownstreamNodes) {
            if (executionPhase == null) {
                return;
            }
            if (branches.size() == 0 && currentBranch.phases.isEmpty()) {
                currentBranch.phases.add(executionPhase);
                return;
            }

            byte inputId;
            ExecutionPhase previousPhase;
            if (currentBranch.phases.isEmpty()) {
                previousPhase = branches.peekLast().phases.getLast();
                inputId = currentBranch.inputId;
            } else {
                previousPhase = currentBranch.phases.getLast();
                // same branch, so use the default input id
                inputId = 0;
            }
            if (setDownstreamNodes) {
                assert saneConfiguration(executionPhase, previousPhase.nodeIds()) : String.format(Locale.ENGLISH,
                    "NodeOperation with %s and %s as downstreams cannot work",
                    ExecutionPhases.debugPrint(executionPhase), previousPhase.nodeIds());

                nodeOperations.add(NodeOperation.withDownstream(executionPhase, previousPhase, inputId, localNodeId));
            } else {
                nodeOperations.add(NodeOperation.withoutDownstream(executionPhase));
            }
            currentBranch.phases.add(executionPhase);
        }

        private boolean saneConfiguration(ExecutionPhase executionPhase, Collection<String> downstreamNodes) {
            if (executionPhase instanceof UpstreamPhase &&
                ((UpstreamPhase) executionPhase).distributionInfo().distributionType() ==
                DistributionType.SAME_NODE) {
                return downstreamNodes.isEmpty() || downstreamNodes.equals(executionPhase.nodeIds());
            }
            return true;
        }

        void branch(byte inputId) {
            branches.add(currentBranch);
            currentBranch = new Branch(inputId);
        }

        void leaveBranch() {
            currentBranch = branches.pollLast();
        }

        Collection<NodeOperation> nodeOperations() {
            return nodeOperations;
        }
    }

    public static NodeOperationTree fromPlan(Plan plan, String localNodeId) {
        NodeOperationTreeContext nodeOperationTreeContext = new NodeOperationTreeContext(localNodeId);
        INSTANCE.process(plan, nodeOperationTreeContext);
        return new NodeOperationTree(nodeOperationTreeContext.nodeOperations(),
            nodeOperationTreeContext.root.phases.getFirst());
    }

    @Override
    public Void visitCountPlan(CountPlan plan, NodeOperationTreeContext context) {
        context.addPhase(plan.mergePhase());
        context.addPhase(plan.countPhase());
        return null;
    }

    @Override
    public Void visitCollect(Collect plan, NodeOperationTreeContext context) {
        context.addPhase(plan.collectPhase());
        return null;
    }

    @Override
    public Void visitMerge(Merge merge, NodeOperationTreeContext context) {
        context.addPhase(merge.mergePhase());
        process(merge.subPlan(), context);
        return null;
    }

    public Void visitQueryThenFetch(QueryThenFetch node, NodeOperationTreeContext context) {
        process(node.subPlan(), context);
        context.addContextPhase(node.fetchPhase());
        return null;
    }

    @Override
    public Void visitMultiPhasePlan(MultiPhasePlan multiPhasePlan, NodeOperationTreeContext context) {
        // MultiPhasePlan's should be executed by the MultiPhaseExecutor, but it doesn't remove
        // them from the tree in order to avoid re-creating plans with the MultiPhasePlan removed,
        // so here it's fine to just skip over the multiPhasePlan because it has already been executed
        process(multiPhasePlan.rootPlan(), context);
        return null;
    }

    @Override
    public Void visitNestedLoop(NestedLoop plan, NodeOperationTreeContext context) {
        context.addPhase(plan.nestedLoopPhase());

        context.branch((byte) 0);
        process(plan.left(), context);
        context.leaveBranch();

        context.branch((byte) 1);
        process(plan.right(), context);
        context.leaveBranch();

        return null;
    }

    @Override
    protected Void visitPlan(Plan plan, NodeOperationTreeContext context) {
        throw new UnsupportedOperationException(String.format(Locale.ENGLISH, "Can't create NodeOperationTree from plan %s", plan));
    }
}
