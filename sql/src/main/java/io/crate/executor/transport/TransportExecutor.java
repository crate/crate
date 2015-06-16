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
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListenableFuture;
import io.crate.action.job.ContextPreparer;
import io.crate.action.sql.DDLStatementDispatcher;
import io.crate.breaker.CrateCircuitBreakerService;
import io.crate.executor.*;
import io.crate.executor.task.DDLTask;
import io.crate.executor.task.NoopTask;
import io.crate.executor.transport.task.CreateTableTask;
import io.crate.executor.transport.task.DropTableTask;
import io.crate.executor.transport.task.KillTask;
import io.crate.executor.transport.task.SymbolBasedUpsertByIdTask;
import io.crate.executor.transport.task.elasticsearch.*;
import io.crate.jobs.JobContextService;
import io.crate.metadata.Functions;
import io.crate.metadata.ReferenceResolver;
import io.crate.operation.ImplementationSymbolVisitor;
import io.crate.operation.PageDownstreamFactory;
import io.crate.operation.projectors.ProjectionToProjectorVisitor;
import io.crate.planner.*;
import io.crate.planner.node.PlanNode;
import io.crate.planner.node.PlanNodeVisitor;
import io.crate.planner.node.StreamerVisitor;
import io.crate.planner.node.ddl.*;
import io.crate.planner.node.dml.*;
import io.crate.planner.node.dql.*;
import io.crate.planner.node.dql.join.NestedLoop;
import io.crate.planner.node.management.KillPlan;
import org.elasticsearch.action.bulk.BulkRetryCoordinatorPool;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.*;

public class TransportExecutor implements Executor, TaskExecutor {

    private final Functions functions;
    private final TaskCollectingVisitor planVisitor;
    private Provider<DDLStatementDispatcher> ddlAnalysisDispatcherProvider;
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

    private final StreamerVisitor streamerVisitor;

    private final ExecutionNodesPlanVisitor executionNodesPlanVisitor;

    @Inject
    public TransportExecutor(Settings settings,
                             JobContextService jobContextService,
                             ContextPreparer contextPreparer,
                             TransportActionProvider transportActionProvider,
                             ThreadPool threadPool,
                             Functions functions,
                             ReferenceResolver referenceResolver,
                             PageDownstreamFactory pageDownstreamFactory,
                             Provider<DDLStatementDispatcher> ddlAnalysisDispatcherProvider,
                             ClusterService clusterService,
                             CrateCircuitBreakerService breakerService,
                             BulkRetryCoordinatorPool bulkRetryCoordinatorPool,
                             StreamerVisitor streamerVisitor) {
        this.jobContextService = jobContextService;
        this.contextPreparer = contextPreparer;
        this.transportActionProvider = transportActionProvider;
        this.pageDownstreamFactory = pageDownstreamFactory;
        this.threadPool = threadPool;
        this.functions = functions;
        this.ddlAnalysisDispatcherProvider = ddlAnalysisDispatcherProvider;
        this.clusterService = clusterService;
        this.bulkRetryCoordinatorPool = bulkRetryCoordinatorPool;
        this.streamerVisitor = streamerVisitor;
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
        executionNodesPlanVisitor = new ExecutionNodesPlanVisitor();
    }

    @Override
    public Job newJob(Plan plan) {
        final Job job = new Job();
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
        protected List<? extends Task> visitPlan(Plan plan, Job job) {
            ExecutionNodesTask task = executionNodesPlanVisitor.process(plan, job);
            return ImmutableList.of(task);
        }

        @Override
        public List<? extends Task> visitUpsert(Upsert plan, Job job) {
            if (plan.nodes().size() == 1 && plan.nodes().get(0) instanceof IterablePlan) {
                return process(plan.nodes().get(0), job);
            }

            ExecutionNodesTask task = executionNodesPlanVisitor.process(plan, job);
            task.rowCountResult(true);
            return ImmutableList.<Task>of(task);
        }

        @Override
        public List<? extends Task> visitInsertByQuery(InsertFromSubQuery node, Job job) {
            List<? extends Task> tasks = process(node.innerPlan(), job);
            if (node.handlerMergeNode().isPresent()) {
                // TODO: remove this hack
                Task previousTask = Iterables.getLast(tasks);
                if (previousTask instanceof ExecutionNodesTask) {
                    ((ExecutionNodesTask) previousTask).addFinalMergeNode(node.handlerMergeNode().get());
                } else {
                    ArrayList<Task> tasks2 = new ArrayList<>(tasks);
                    tasks2.addAll(nodeVisitor.visitMergeNode(node.handlerMergeNode().get(), job.id()));
                    return tasks2;
                }
            }
            return tasks;
        }

        @Override
        public List<Task> visitKillPlan(KillPlan killPlan, Job job) {
            return ImmutableList.<Task>of(new KillTask(
                    clusterService,
                    transportActionProvider.transportKillAllNodeAction(),
                    job.id()));
        }

    }

    class NodeVisitor extends PlanNodeVisitor<UUID, ImmutableList<Task>> {

        private ImmutableList<Task> singleTask(Task task) {
            return ImmutableList.of(task);
        }

        @Override
        public ImmutableList<Task> visitGenericDDLNode(GenericDDLNode node, UUID jobId) {
            return singleTask(new DDLTask(jobId, ddlAnalysisDispatcherProvider.get(), node));
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

    class ExecutionNodesPlanVisitor extends PlanVisitor<ExecutionNodesPlanVisitor.Context, Void> {

        class Context {
            boolean isRootPlan = true;
            ExecutionNodesTask executionNodesTask;

            public Context(ExecutionNodesTask executionNodesTask) {
                this.executionNodesTask = executionNodesTask;
            }
        }

        public ExecutionNodesTask process(Plan plan, Job job) {
            ExecutionNodesTask executionNodesTask = new ExecutionNodesTask(
                    job.id(),
                    clusterService,
                    contextPreparer,
                    jobContextService,
                    pageDownstreamFactory,
                    threadPool,
                    transportActionProvider.transportJobInitAction(),
                    transportActionProvider.transportCloseContextNodeAction(),
                    streamerVisitor,
                    circuitBreaker);
            Context context = new Context(executionNodesTask);
            process(plan, context);
            return executionNodesTask;
        }

        @Override
        public Void visitQueryThenFetch(QueryThenFetch plan, Context context) {
            context.executionNodesTask.addExecutionNode(0, plan.collectNode());
            if (context.isRootPlan && plan.mergeNode() != null) {
                context.executionNodesTask.addFinalMergeNode(plan.mergeNode());
            }
            return null;
        }

        @Override
        public Void visitQueryAndFetch(QueryAndFetch plan, Context context) {
            context.executionNodesTask.addExecutionNode(0, plan.collectNode());
            if (context.isRootPlan && plan.localMergeNode() != null) {
                context.executionNodesTask.addFinalMergeNode(plan.localMergeNode());
            }
            return null;
        }

        @Override
        public Void visitDistributedGroupBy(DistributedGroupBy plan, Context context) {
            context.executionNodesTask.addExecutionNode(0, plan.collectNode());
            context.executionNodesTask.addExecutionNode(0, plan.reducerMergeNode());
            if (context.isRootPlan && plan.localMergeNode() != null) {
                context.executionNodesTask.addFinalMergeNode(plan.localMergeNode());
            }
            return null;
        }

        @Override
        public Void visitNonDistributedGroupBy(NonDistributedGroupBy plan, Context context) {
            context.executionNodesTask.addExecutionNode(0, plan.collectNode());
            if (context.isRootPlan && plan.localMergeNode() != null) {
                context.executionNodesTask.addFinalMergeNode(plan.localMergeNode());
            }
            return null;
        }

        @Override
        public Void visitGlobalAggregate(GlobalAggregate plan, Context context) {
            context.executionNodesTask.addExecutionNode(0, plan.collectNode());
            if (context.isRootPlan && plan.mergeNode() != null) {
                context.executionNodesTask.addFinalMergeNode(plan.mergeNode());
            }
            return null;
        }

        @Override
        public Void visitCollectAndMerge(CollectAndMerge plan, Context context) {
            context.executionNodesTask.addExecutionNode(0, plan.collectNode());
            if (context.isRootPlan && plan.localMergeNode() != null) {
                context.executionNodesTask.addFinalMergeNode(plan.localMergeNode());
            }
            return null;
        }

        @Override
        public Void visitCountPlan(CountPlan plan, Context context) {
            context.executionNodesTask.addExecutionNode(0, plan.countNode());
            if (context.isRootPlan && plan.mergeNode() != null) {
                context.executionNodesTask.addFinalMergeNode(plan.mergeNode());
            }
            return null;
        }

        @Override
        public Void visitUpsert(Upsert plan, Context context) {
            for (int i = 0; i < plan.nodes().size(); i++) {
                Plan subPlan = plan.nodes().get(i);
                assert subPlan instanceof CollectAndMerge;
                context.executionNodesTask.addExecutionNode(i, ((CollectAndMerge) subPlan).collectNode());
                context.executionNodesTask.addFinalMergeNode(((CollectAndMerge) subPlan).localMergeNode());
            }
            return null;
        }

        @Override
        public Void visitNestedLoop(NestedLoop plan, Context context) {
            boolean isRootPlan = context.isRootPlan;
            if (isRootPlan) {
                context.isRootPlan = false;
            }
            process(plan.left().plan(), context);
            process(plan.right().plan(), context);

            if (plan.nestedLoopNode().leftMergeNode() != null) {
                plan.nestedLoopNode().leftMergeNode().jobId(context.executionNodesTask.jobId());
            }
            if (plan.nestedLoopNode().rightMergeNode() != null) {
                plan.nestedLoopNode().rightMergeNode().jobId(context.executionNodesTask.jobId());
            }

            context.executionNodesTask.addExecutionNode(0, plan.nestedLoopNode());
            if (isRootPlan && plan.localMergeNode() != null) {
                context.executionNodesTask.addFinalMergeNode(plan.localMergeNode());
            }
            return null;
        }

        @Override
        protected Void visitPlan(Plan plan, Context context) {
            throw new UnsupportedOperationException(String.format(Locale.ENGLISH,
                    "Plan %s not supported", plan.getClass().getCanonicalName()));
        }
    }
}
