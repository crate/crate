/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.executor.transport;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.crate.action.job.ContextPreparer;
import io.crate.action.sql.DDLStatementDispatcher;
import io.crate.action.sql.ShowStatementDispatcher;
import io.crate.breaker.CrateCircuitBreakerService;
import io.crate.executor.*;
import io.crate.executor.task.DDLTask;
import io.crate.executor.task.NoopTask;
import io.crate.executor.transport.task.*;
import io.crate.executor.transport.task.elasticsearch.*;
import io.crate.jobs.JobContextService;
import io.crate.metadata.Functions;
import io.crate.metadata.NestedReferenceResolver;
import io.crate.operation.ImplementationSymbolVisitor;
import io.crate.operation.NodeOperation;
import io.crate.operation.NodeOperationTree;
import io.crate.operation.PageDownstreamFactory;
import io.crate.operation.projectors.ProjectionToProjectorVisitor;
import io.crate.planner.*;
import io.crate.planner.node.ExecutionPhase;
import io.crate.planner.node.PlanNode;
import io.crate.planner.node.PlanNodeVisitor;
import io.crate.planner.node.ddl.*;
import io.crate.planner.node.dml.*;
import io.crate.planner.node.dql.*;
import io.crate.planner.node.dql.join.NestedLoop;
import io.crate.planner.node.management.GenericShowPlan;
import io.crate.planner.node.management.KillPlan;
import org.elasticsearch.action.bulk.BulkRetryCoordinatorPool;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;

import javax.annotation.Nullable;
import java.util.*;

public class TransportExecutor implements Executor, TaskExecutor {

    private final Functions functions;
    private final TaskCollectingVisitor planVisitor;
    private DDLStatementDispatcher ddlAnalysisDispatcherProvider;
    private ShowStatementDispatcher showStatementDispatcherProvider;
    private final NodeVisitor nodeVisitor;
    private final ThreadPool threadPool;

    private final ClusterService clusterService;
    private final JobContextService jobContextService;
    private final ContextPreparer contextPreparer;
    private final TransportActionProvider transportActionProvider;
    private final BulkRetryCoordinatorPool bulkRetryCoordinatorPool;

    private final ProjectionToProjectorVisitor globalProjectionToProjectionVisitor;

    // operation for handler side collecting
    private final CircuitBreaker circuitBreaker;

    private final PageDownstreamFactory pageDownstreamFactory;

    private final static BulkNodeOperationTreeGenerator BULK_NODE_OPERATION_VISITOR = new BulkNodeOperationTreeGenerator();


    @Inject
    public TransportExecutor(Settings settings,
                             JobContextService jobContextService,
                             ContextPreparer contextPreparer,
                             TransportActionProvider transportActionProvider,
                             ThreadPool threadPool,
                             Functions functions,
                             NestedReferenceResolver referenceResolver,
                             PageDownstreamFactory pageDownstreamFactory,
                             DDLStatementDispatcher ddlAnalysisDispatcherProvider,
                             ShowStatementDispatcher showStatementDispatcherProvider,
                             ClusterService clusterService,
                             CrateCircuitBreakerService breakerService,
                             BulkRetryCoordinatorPool bulkRetryCoordinatorPool) {
        this.jobContextService = jobContextService;
        this.contextPreparer = contextPreparer;
        this.transportActionProvider = transportActionProvider;
        this.pageDownstreamFactory = pageDownstreamFactory;
        this.threadPool = threadPool;
        this.functions = functions;
        this.ddlAnalysisDispatcherProvider = ddlAnalysisDispatcherProvider;
        this.showStatementDispatcherProvider = showStatementDispatcherProvider;
        this.clusterService = clusterService;
        this.bulkRetryCoordinatorPool = bulkRetryCoordinatorPool;
        nodeVisitor = new NodeVisitor();
        planVisitor = new TaskCollectingVisitor();
        circuitBreaker = breakerService.getBreaker(CrateCircuitBreakerService.QUERY_BREAKER);
        ImplementationSymbolVisitor globalImplementationSymbolVisitor = new ImplementationSymbolVisitor(
                referenceResolver, functions, RowGranularity.CLUSTER);
        globalProjectionToProjectionVisitor = new ProjectionToProjectorVisitor(
                clusterService,
                threadPool,
                settings,
                transportActionProvider,
                bulkRetryCoordinatorPool,
                globalImplementationSymbolVisitor);
    }

    @Override
    public Job newJob(Plan plan) {
        final Job job = new Job(plan.jobId());
        List<? extends Task> tasks = planVisitor.process(plan, job);
        job.addTasks(tasks);
        return job;
    }

    @Override
    public List<? extends ListenableFuture<TaskResult>> execute(Job job) {
        assert job.tasks().size() > 0;
        return execute(job.tasks());

    }

    @Override
    public List<Task> newTasks(PlanNode planNode, UUID jobId) {
        return planNode.accept(nodeVisitor, jobId);
    }

    @Override
    public List<? extends ListenableFuture<TaskResult>> execute(Collection<Task> tasks) {
        Task lastTask = null;
        assert tasks.size() > 0 : "need at least one task to execute";
        for (Task task : tasks) {
            // chaining tasks
            if (lastTask != null) {
                task.upstreamResult(lastTask.result());
            }
            task.start();
            lastTask = task;
        }
        assert lastTask != null;
        return lastTask.result();
    }

    class TaskCollectingVisitor extends PlanVisitor<Job, List<? extends Task>> {

        @Override
        public List<Task> visitIterablePlan(IterablePlan plan, Job job) {
            List<Task> tasks = new ArrayList<>();
            for (PlanNode planNode : plan) {
                tasks.addAll(planNode.accept(nodeVisitor, job.id()));
            }
            return tasks;
        }

        @Override
        public List<Task> visitNoopPlan(NoopPlan plan, Job job) {
            return ImmutableList.<Task>of(NoopTask.INSTANCE);
        }

        @Override
        public List<? extends Task> visitUpsert(Upsert node, Job context) {
            List<Plan> nonIterablePlans = new ArrayList<>();
            List<Task> tasks = new ArrayList<>();
            for (Plan plan : node.nodes()) {
                if (plan instanceof IterablePlan) {
                    tasks.addAll(process(plan, context));
                } else {
                    nonIterablePlans.add(plan);
                }
            }
            if (!nonIterablePlans.isEmpty()) {
                tasks.add(executionPhasesTask(new Upsert(nonIterablePlans, context.id()), context));
            }
            return tasks;
        }

        @Override
        protected List<? extends Task> visitPlan(Plan plan, Job job) {
            ExecutionPhasesTask task = executionPhasesTask(plan, job);
            return ImmutableList.of(task);
        }

        private ExecutionPhasesTask executionPhasesTask(Plan plan, Job job) {
            List<NodeOperationTree> nodeOperationTrees = BULK_NODE_OPERATION_VISITOR.createNodeOperationTrees(plan);
            return new ExecutionPhasesTask(
                    job.id(),
                    clusterService,
                    contextPreparer,
                    jobContextService,
                    transportActionProvider.transportJobInitAction(),
                    nodeOperationTrees
            );
        }

        @Override
        public List<Task> visitKillPlan(KillPlan killPlan, Job job) {
            Task task = killPlan.jobToKill().isPresent() ?
                    new KillJobTask(transportActionProvider.transportKillJobsNodeAction(),
                            job.id(),
                            killPlan.jobToKill().get()) :
                    new KillTask(clusterService,
                            transportActionProvider.transportKillAllNodeAction(),
                            job.id());
            return ImmutableList.of(task);
        }

        @Override
        public List<? extends Task> visitGenericShowPlan(GenericShowPlan genericShowPlan, Job job) {
            return ImmutableList.<Task>of(new GenericShowTask(job.id(),
                    showStatementDispatcherProvider,
                    genericShowPlan.statement()));
        }
    }

    class NodeVisitor extends PlanNodeVisitor<UUID, ImmutableList<Task>> {

        private ImmutableList<Task> singleTask(Task task) {
            return ImmutableList.of(task);
        }

        @Override
        public ImmutableList<Task> visitGenericDDLNode(GenericDDLNode node, UUID jobId) {
            return singleTask(new DDLTask(jobId, ddlAnalysisDispatcherProvider, node));
        }

        @Override
        public ImmutableList<Task> visitESGetNode(ESGetNode node, UUID jobId) {
            return singleTask(new ESGetTask(
                    jobId,
                    functions,
                    globalProjectionToProjectionVisitor,
                    transportActionProvider.transportMultiGetAction(),
                    transportActionProvider.transportGetAction(),
                    node,
                    jobContextService));
        }

        @Override
        public ImmutableList<Task> visitESDeleteByQueryNode(ESDeleteByQueryNode node, UUID jobId) {
            return singleTask(new ESDeleteByQueryTask(
                    jobId,
                    node,
                    transportActionProvider.transportDeleteByQueryAction(),
                    jobContextService));
        }

        @Override
        public ImmutableList<Task> visitESDeleteNode(ESDeleteNode node, UUID jobId) {
            return singleTask(new ESDeleteTask(
                    jobId,
                    node,
                    transportActionProvider.transportDeleteAction(),
                    jobContextService));
        }

        @Override
        public ImmutableList<Task> visitCreateTableNode(CreateTableNode node, UUID jobId) {
            return singleTask(new CreateTableTask(
                            jobId,
                            clusterService,
                            transportActionProvider.transportCreateIndexAction(),
                            transportActionProvider.transportDeleteIndexAction(),
                            transportActionProvider.transportPutIndexTemplateAction(),
                            node)
            );
        }

        @Override
        public ImmutableList<Task> visitESCreateTemplateNode(ESCreateTemplateNode node, UUID jobId) {
            return singleTask(new ESCreateTemplateTask(jobId,
                    node,
                    transportActionProvider.transportPutIndexTemplateAction()));
        }

        @Override
        public ImmutableList<Task> visitSymbolBasedUpsertByIdNode(SymbolBasedUpsertByIdNode node, UUID jobId) {
            return singleTask(new SymbolBasedUpsertByIdTask(jobId,
                    clusterService,
                    clusterService.state().metaData().settings(),
                    transportActionProvider.symbolBasedTransportShardUpsertActionDelegate(),
                    transportActionProvider.transportCreateIndexAction(),
                    transportActionProvider.transportBulkCreateIndicesAction(),
                    bulkRetryCoordinatorPool,
                    node,
                    jobContextService));
        }

        @Override
        public ImmutableList<Task> visitDropTableNode(DropTableNode node, UUID jobId) {
            return singleTask(new DropTableTask(jobId,
                    transportActionProvider.transportDeleteIndexTemplateAction(),
                    transportActionProvider.transportDeleteIndexAction(),
                    node));
        }

        @Override
        public ImmutableList<Task> visitESDeletePartitionNode(ESDeletePartitionNode node, UUID jobId) {
            return singleTask(new ESDeletePartitionTask(jobId,
                    transportActionProvider.transportDeleteIndexAction(),
                    node));
        }

        @Override
        public ImmutableList<Task> visitESClusterUpdateSettingsNode(ESClusterUpdateSettingsNode node, UUID jobId) {
            return singleTask(new ESClusterUpdateSettingsTask(
                    jobId,
                    transportActionProvider.transportClusterUpdateSettingsAction(),
                    node));
        }

        @Override
        protected ImmutableList<Task> visitPlanNode(PlanNode node, UUID jobId) {
            throw new UnsupportedOperationException(
                    String.format("Can't generate job/task for planNode %s", node));
        }
    }

    static class BulkNodeOperationTreeGenerator extends PlanVisitor<List<NodeOperationTree>, Void> {

        NodeOperationTreeGenerator nodeOperationTreeGenerator = new NodeOperationTreeGenerator();

        public List<NodeOperationTree> createNodeOperationTrees(Plan plan) {
            ArrayList<NodeOperationTree> nodeOperationTrees = new ArrayList<>();
            process(plan, nodeOperationTrees);
            return nodeOperationTrees;
        }

        @Override
        public Void visitUpsert(Upsert node, List<NodeOperationTree> context) {
            for (Plan plan : node.nodes()) {
                context.add(nodeOperationTreeGenerator.fromPlan(plan));
            }
            return null;
        }

        @Override
        protected Void visitPlan(Plan plan, List<NodeOperationTree> context) {
            context.add(nodeOperationTreeGenerator.fromPlan(plan));
            return null;
        }
    }

    /**
     * class used to generate the NodeOperationTree
     *
     *
     * E.g. a plan like NL:
     *
     *              NL
     *           1 NLPhase
     *           2 MergePhase
     *        /               \
     *       /                 \
     *     QAF                 QAF
     *   3 CollectPhase      5 CollectPhase
     *   4 MergePhase        6 MergePhase
     *
     *
     * Will have a data flow like this:
     *
     *   3 -- 4
     *          -- 1 -- 2
     *   5 -- 6
     *
     * The NodeOperation tree will have 5 NodeOperations (3-4, 4-1, 5-6, 6-1, 1-2)
     * And leaf will be 2 (the Phase which will provide the final result)
     *
     *
     * Implementation detail:
     *
     *
     *   The phases are added in the following order
     *
     *   2 - 1 [new branch 0]  4 - 3
     *         [new branch 1]  5 - 6
     *
     *   every time addPhase is called a NodeOperation is added
     *   that connects the previous phase (if there is one) to the current phase
     */
    static class NodeOperationTreeGenerator extends PlanVisitor<NodeOperationTreeGenerator.NodeOperationTreeContext, Void> {

        static class NodeOperationTreeContext {
            private final List<NodeOperation> collectNodeOperations = new ArrayList<>();
            private final List<NodeOperation> nodeOperations = new ArrayList<>();
            private final Stack<ExecutionPhase> root = new Stack<>();

            Stack<ExecutionPhase> currentBranch = root;
            Stack<ExecutionPhase> prevBranch;

            byte nextInputId;


            /**
             * adds a Phase to the "NodeOperation execution tree"
             * should be called in the reverse order of how data flows.
             *
             * E.g. in a plan where data flows from CollectPhase to MergePhase
             * it should be called first for MergePhase and then for CollectPhase
             */
            public void addPhase(@Nullable ExecutionPhase executionPhase) {
                addPhase(executionPhase, nodeOperations);
            }

            /**
             * same as {@link #addPhase(ExecutionPhase)} but those phases will be added
             * in the front of the nodeOperation list to make sure that they are later in the execution started last
             * to avoid race conditions.
             */
            public void addCollectExecutionPhase(@Nullable ExecutionPhase executionPhase) {
                addPhase(executionPhase, collectNodeOperations);
            }

            private void addPhase(@Nullable ExecutionPhase executionPhase, List<NodeOperation> nodeOperations) {
                if (executionPhase == null) {
                    return;
                }
                if (prevBranch == null && currentBranch.isEmpty()) {
                    currentBranch.add(executionPhase);
                    return;
                }

                byte inputId;
                ExecutionPhase previousPhase;
                if (currentBranch.isEmpty()) {
                    inputId = nextInputId;
                    previousPhase = prevBranch.lastElement();
                } else {
                    inputId = 0;
                    previousPhase = currentBranch.lastElement();
                }
                nodeOperations.add(NodeOperation.withDownstream(executionPhase, previousPhase, inputId));
                currentBranch.add(executionPhase);
            }

            public void branch(byte inputId) {
                this.nextInputId = inputId;
                prevBranch = currentBranch;
                currentBranch = new Stack<>();
            }

            public void leaveBranch() {
                nextInputId = 0;
                currentBranch = prevBranch;
            }

            public Collection<NodeOperation> nodeOperations() {
                return ImmutableList.<NodeOperation>builder()
                        // collectNodeOperations must be first so that they're started last
                        // to prevent context-setup race conditions
                        .addAll(collectNodeOperations)
                        .addAll(nodeOperations)
                        .build();
            }
        }

        public NodeOperationTree fromPlan(Plan plan) {
            NodeOperationTreeContext nodeOperationTreeContext = new NodeOperationTreeContext();
            process(plan, nodeOperationTreeContext);
            return new NodeOperationTree(nodeOperationTreeContext.nodeOperations(), nodeOperationTreeContext.root.firstElement());
        }

        @Override
        public Void visitInsertByQuery(InsertFromSubQuery node, NodeOperationTreeContext context) {
            if (node.handlerMergeNode().isPresent()) {
                context.addPhase(node.handlerMergeNode().get());
            }
            process(node.innerPlan(), context);
            return null;
        }

        @Override
        public Void visitDistributedGroupBy(DistributedGroupBy node, NodeOperationTreeContext context) {
            context.addPhase(node.localMergeNode());
            context.addPhase(node.reducerMergeNode());
            context.addCollectExecutionPhase(node.collectNode());
            return null;
        }

        @Override
        public Void visitCountPlan(CountPlan plan, NodeOperationTreeContext context) {
            context.addPhase(plan.mergeNode());
            context.addCollectExecutionPhase(plan.countNode());
            return null;
        }

        @Override
        public Void visitCollectAndMerge(CollectAndMerge plan, NodeOperationTreeContext context) {
            context.addPhase(plan.localMerge());
            context.addCollectExecutionPhase(plan.collectPhase());

            return null;
        }

        @Override
        public Void visitNestedLoop(NestedLoop plan, NodeOperationTreeContext context) {
            context.addPhase(plan.nestedLoopPhase());

            context.branch((byte) 0);
            process(plan.left().plan(), context);
            context.leaveBranch();

            context.branch((byte) 1);
            process(plan.right().plan(), context);
            context.leaveBranch();

            return null;
        }

        @Override
        protected Void visitPlan(Plan plan, NodeOperationTreeContext context) {
            throw new UnsupportedOperationException(String.format("Can't create NodeOperationTree from plan %s", plan));
        }
    }
}
