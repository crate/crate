/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.execution.engine;

import io.crate.data.Paging;
import io.crate.execution.dsl.phases.ExecutionPhase;
import io.crate.execution.dsl.phases.ExecutionPhases;
import io.crate.execution.dsl.phases.NodeOperation;
import io.crate.execution.dsl.phases.NodeOperationTree;
import io.crate.execution.dsl.phases.UpstreamPhase;
import io.crate.execution.engine.distribution.DistributingConsumerFactory;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.ExecutionPlanVisitor;
import io.crate.planner.Merge;
import io.crate.planner.UnionExecutionPlan;
import io.crate.planner.distribution.DistributionType;
import io.crate.planner.node.dql.Collect;
import io.crate.planner.node.dql.CountPlan;
import io.crate.planner.node.dql.QueryThenFetch;
import io.crate.planner.node.dql.join.Join;

import org.jspecify.annotations.Nullable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.List;
import java.util.Locale;

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
public final class NodeOperationTreeGenerator extends ExecutionPlanVisitor<NodeOperationTreeGenerator.NodeOperationTreeContext, Void> {

    private static final NodeOperationTreeGenerator INSTANCE = new NodeOperationTreeGenerator();

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

        void addContextPhase(@Nullable ExecutionPhase executionPhase) {
            if (executionPhase != null) {
                nodeOperations.add(NodeOperation.withoutDownstream(executionPhase));
            }
        }

        void addPhase(@Nullable ExecutionPhase executionPhase) {
            addPhase(executionPhase, false);
        }

        /**
         * adds a Phase to the "NodeOperation execution tree"
         * should be called in the reverse order of how data flows.
         * <p>
         * E.g. in a plan where data flows from CollectPhase to MergePhase
         * it should be called first for MergePhase and then for CollectPhase
         */
        void addPhase(@Nullable ExecutionPhase executionPhase, boolean directResponse) {
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
            assert saneConfiguration(executionPhase, previousPhase.nodeIds()) : String.format(Locale.ENGLISH,
                "NodeOperation with %s and %s as downstreams cannot work",
                ExecutionPhases.debugPrint(executionPhase), previousPhase.nodeIds());

            NodeOperation nodeOperation;
            if (directResponse) {
                nodeOperation = NodeOperation.withDirectResponse(executionPhase, previousPhase, inputId, localNodeId);
            } else {
                nodeOperation = NodeOperation.withDownstream(executionPhase, previousPhase, inputId);
            }
            nodeOperations.add(nodeOperation);
            currentBranch.phases.add(executionPhase);
        }

        private boolean saneConfiguration(ExecutionPhase executionPhase, Collection<String> downstreamNodes) {
            if (executionPhase instanceof UpstreamPhase &&
                ((UpstreamPhase) executionPhase).distributionInfo().distributionType() ==
                DistributionType.SAME_NODE) {
                return downstreamNodes.isEmpty() || downstreamNodes.containsAll(executionPhase.nodeIds());
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

        boolean noPreviousPhases() {
            return branches.isEmpty() && currentBranch.phases.isEmpty();
        }
    }

    public static NodeOperationTree fromPlan(ExecutionPlan executionPlan, String localNodeId) {
        NodeOperationTreeContext nodeOperationTreeContext = new NodeOperationTreeContext(localNodeId);
        INSTANCE.process(executionPlan, nodeOperationTreeContext);
        return new NodeOperationTree(nodeOperationTreeContext.nodeOperations(),
            nodeOperationTreeContext.root.phases.getFirst());
    }

    @Override
    public Void visitCountPlan(CountPlan plan, NodeOperationTreeContext context) {
        boolean useDirectResponse = context.noPreviousPhases();
        context.addPhase(plan.mergePhase());
        context.addPhase(plan.countPhase(), useDirectResponse);
        return null;
    }

    @Override
    public Void visitCollect(Collect plan, NodeOperationTreeContext context) {
        context.addPhase(plan.collectPhase());
        return null;
    }

    @Override
    public Void visitMerge(Merge merge, NodeOperationTreeContext context) {
        ExecutionPlan subExecutionPlan = merge.subPlan();

        boolean useDirectResponse = context.noPreviousPhases() &&
                                    subExecutionPlan instanceof Collect &&
                                    !Paging.shouldPage(subExecutionPlan.resultDescription().maxRowsPerNode());
        context.addPhase(merge.mergePhase());
        if (useDirectResponse) {
            context.addPhase(((Collect) subExecutionPlan).collectPhase(), true);
        } else {
            process(subExecutionPlan, context);
        }
        return null;
    }

    /**
     * Generates the {@link NodeOperation}s for executing Union.
     *
     * We branch off for both sides of the Union with different input ids. In contrast
     * to the {@code visitNestedLoop}, we don't have a special iterator which merges the
     * result from both sides. The {@link DistributingConsumerFactory}
     * generates buckets ids based on the input id and the node id which creates all
     * buckets required to merge the results of both branches.
     */
    @Override
    public Void visitUnionPlan(UnionExecutionPlan unionExecutionPlan, NodeOperationTreeContext context) {
        context.addPhase(unionExecutionPlan.mergePhase());

        context.branch((byte) 0);
        process(unionExecutionPlan.left(), context);
        context.leaveBranch();

        context.branch((byte) 1);
        process(unionExecutionPlan.right(), context);
        context.leaveBranch();

        return null;
    }

    public Void visitQueryThenFetch(QueryThenFetch node, NodeOperationTreeContext context) {
        process(node.subPlan(), context);
        context.addContextPhase(node.fetchPhase());
        return null;
    }

    @Override
    public Void visitJoin(Join plan, NodeOperationTreeContext context) {
        context.addPhase(plan.joinPhase());

        context.branch((byte) 0);
        process(plan.left(), context);
        context.leaveBranch();

        context.branch((byte) 1);
        process(plan.right(), context);
        context.leaveBranch();

        return null;
    }

    @Override
    protected Void visitPlan(ExecutionPlan executionPlan, NodeOperationTreeContext context) {
        throw new UnsupportedOperationException(String.format(Locale.ENGLISH, "Can't create NodeOperationTree from plan %s", executionPlan));
    }
}
