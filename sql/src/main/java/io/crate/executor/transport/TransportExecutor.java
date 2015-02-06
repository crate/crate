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
import io.crate.action.sql.DDLStatementDispatcher;
import io.crate.breaker.CrateCircuitBreakerService;
import io.crate.executor.*;
import io.crate.executor.task.DDLTask;
import io.crate.executor.task.LocalCollectTask;
import io.crate.executor.task.LocalMergeTask;
import io.crate.executor.task.NoopTask;
import io.crate.executor.task.join.NestedLoopTask;
import io.crate.executor.transport.task.*;
import io.crate.executor.transport.task.elasticsearch.*;
import io.crate.metadata.Functions;
import io.crate.metadata.ReferenceResolver;
import io.crate.operation.ImplementationSymbolVisitor;
import io.crate.operation.collect.HandlerSideDataCollectOperation;
import io.crate.operation.collect.StatsTables;
import io.crate.operation.projectors.ProjectionToProjectorVisitor;
import io.crate.operation.qtf.QueryThenFetchOperation;
import io.crate.planner.*;
import io.crate.planner.node.PlanNode;
import io.crate.planner.node.PlanNodeVisitor;
import io.crate.planner.node.ddl.*;
import io.crate.planner.node.dml.*;
import io.crate.planner.node.dql.*;
import io.crate.planner.node.dql.join.NestedLoopNode;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class TransportExecutor implements Executor, TaskExecutor {

    private final Functions functions;
    private final TaskCollectingVisitor planVisitor;
    private Provider<DDLStatementDispatcher> ddlAnalysisDispatcherProvider;
    private final StatsTables statsTables;
    private final NodeVisitor nodeVisitor;
    private final ThreadPool threadPool;

    private final Settings settings;
    private final ClusterService clusterService;
    private final TransportActionProvider transportActionProvider;

    private final ImplementationSymbolVisitor globalImplementationSymbolVisitor;
    private final ProjectionToProjectorVisitor globalProjectionToProjectionVisitor;

    // operation for handler side collecting
    private final HandlerSideDataCollectOperation handlerSideDataCollectOperation;
    private final CircuitBreaker circuitBreaker;

    private final QueryThenFetchOperation queryThenFetchOperation;

    @Inject
    public TransportExecutor(Settings settings,
                             TransportActionProvider transportActionProvider,
                             ThreadPool threadPool,
                             Functions functions,
                             ReferenceResolver referenceResolver,
                             HandlerSideDataCollectOperation handlerSideDataCollectOperation,
                             Provider<DDLStatementDispatcher> ddlAnalysisDispatcherProvider,
                             StatsTables statsTables,
                             ClusterService clusterService,
                             CrateCircuitBreakerService breakerService,
                             QueryThenFetchOperation queryThenFetchOperation) {
        this.settings = settings;
        this.transportActionProvider = transportActionProvider;
        this.handlerSideDataCollectOperation = handlerSideDataCollectOperation;
        this.threadPool = threadPool;
        this.functions = functions;
        this.ddlAnalysisDispatcherProvider = ddlAnalysisDispatcherProvider;
        this.statsTables = statsTables;
        this.clusterService = clusterService;
        this.queryThenFetchOperation = queryThenFetchOperation;
        this.nodeVisitor = new NodeVisitor();
        this.planVisitor = new TaskCollectingVisitor();
        this.circuitBreaker = breakerService.getBreaker(CrateCircuitBreakerService.QUERY_BREAKER);
        this.globalImplementationSymbolVisitor = new ImplementationSymbolVisitor(
                referenceResolver, functions, RowGranularity.CLUSTER);
        this.globalProjectionToProjectionVisitor = new ProjectionToProjectorVisitor(
                clusterService, settings, transportActionProvider,
                globalImplementationSymbolVisitor);
    }

    @Override
    public Job newJob(Plan plan) {
        final Job job = new Job();
        planVisitor.process(plan, job);
        return job;
    }

    @Override
    public List<ListenableFuture<TaskResult>> execute(Job job) {
        assert job.tasks().size() > 0;
        return execute(job.tasks());

    }

    @Override
    public List<Task> newTasks(PlanNode planNode, Job job) {
        return planNode.accept(nodeVisitor, job);
    }

    @Override
    public List<ListenableFuture<TaskResult>> execute(Collection<Task> tasks) {
        Task lastTask = null;
        for (Task task : tasks) {
            // chaining tasks
            if (lastTask != null) {
                task.upstreamResult(lastTask.result());
            }
            task.start();
            lastTask = task;
        }
        return lastTask.result();
    }

    class TaskCollectingVisitor extends PlanVisitor<Job, Void> {

        @Override
        public Void visitIterablePlan(IterablePlan plan, Job job) {
            for (PlanNode planNode : plan) {
                job.addTasks(planNode.accept(nodeVisitor, job));
            }
            return null;
        }

        @Override
        public Void visitNoopPlan(NoopPlan plan, Job job) {
            job.addTask(NoopTask.INSTANCE);
            return null;
        }

        @Override
        public Void visitGlobalAggregate(GlobalAggregate plan, Job job) {
            job.addTasks(nodeVisitor.visitCollectNode(plan.collectNode(), job));
            job.addTasks(nodeVisitor.visitMergeNode(plan.mergeNode(), job));
            return null;
        }

        @Override
        public Void visitQueryAndFetch(QueryAndFetch plan, Job job) {
            job.addTasks(nodeVisitor.visitCollectNode(plan.collectNode(), job));
            job.addTasks(nodeVisitor.visitMergeNode(plan.localMergeNode(), job));
            return null;
        }

        @Override
        public Void visitNonDistributedGroupBy(NonDistributedGroupBy plan, Job job) {
            job.addTasks(nodeVisitor.visitCollectNode(plan.collectNode(), job));
            job.addTasks(nodeVisitor.visitMergeNode(plan.localMergeNode(), job));
            return null;
        }

        @Override
        public Void visitUpsert(Upsert plan, Job job) {
            ImmutableList.Builder<Task> taskBuilder = ImmutableList.builder();
            for (List<DQLPlanNode> childNodes : plan.nodes()) {
                List<Task> subTasks = new ArrayList<>(childNodes.size());
                for (DQLPlanNode childNode : childNodes) {
                    subTasks.addAll(childNode.accept(nodeVisitor, job));
                }
                UpsertTask upsertTask = new UpsertTask(TransportExecutor.this, job.id(), subTasks);
                taskBuilder.add(upsertTask);
            }
            job.addTasks(taskBuilder.build());
            return null;
        }

        @Override
        public Void visitDistributedGroupBy(DistributedGroupBy plan, Job job) {
            job.addTasks(nodeVisitor.visitCollectNode(plan.collectNode(), job));
            job.addTasks(nodeVisitor.visitMergeNode(plan.reducerMergeNode(), job));
            job.addTasks(nodeVisitor.visitMergeNode(plan.localMergeNode(), job));
            return null;
        }

        @Override
        public Void visitInsertByQuery(InsertFromSubQuery node, Job job) {
            this.process(node.innerPlan(), job);
            if(node.handlerMergeNode().isPresent()) {
                job.addTasks(nodeVisitor.visitMergeNode(node.handlerMergeNode().get(), job));
            }
            return null;
        }

    }

    class NodeVisitor extends PlanNodeVisitor<Job, ImmutableList<Task>> {

        private ImmutableList<Task> singleTask(Task task) {
            return ImmutableList.of(task);
        }

        @Override
        public ImmutableList<Task> visitCollectNode(CollectNode node, Job job) {
            node.jobId(job.id()); // add jobId to collectNode
            if (node.isRouted()) {
                return singleTask(new RemoteCollectTask(
                        job.id(),
                        node,
                        transportActionProvider.transportCollectNodeAction(),
                        handlerSideDataCollectOperation,
                        statsTables,
                        circuitBreaker));
            } else {
                return singleTask(new LocalCollectTask(
                        job.id(),
                        handlerSideDataCollectOperation,
                        node,
                        circuitBreaker));
            }

        }

        @Override
        public ImmutableList<Task> visitGenericDDLNode(GenericDDLNode node, Job job) {
            return singleTask(new DDLTask(job.id(), ddlAnalysisDispatcherProvider.get(), node));
        }

        @Override

        public ImmutableList<Task> visitMergeNode(@Nullable MergeNode node, Job job) {

            if (node == null) {
                return ImmutableList.of();
            }
            node.contextId(job.id());
            if (node.executionNodes().isEmpty()) {
                return singleTask(new LocalMergeTask(
                        job.id(),
                        threadPool,
                        clusterService,
                        settings,
                        transportActionProvider,
                        globalImplementationSymbolVisitor,
                        node,
                        statsTables,
                        circuitBreaker));
            } else {
                return singleTask(new DistributedMergeTask(
                        job.id(),
                        transportActionProvider.transportMergeNodeAction(), node));
            }
        }

        @Override
        public ImmutableList<Task> visitQueryThenFetchNode(QueryThenFetchNode node, Job job) {
            return singleTask(new QueryThenFetchTask(
                    job.id(),
                    queryThenFetchOperation,
                    functions,
                    node));
        }

        @Override
        public ImmutableList<Task> visitNestedLoopNode(NestedLoopNode node, Job job) {
            // TODO: optimize for outer and inner being the same relation
            Job outerJob = new Job(job.id());
            planVisitor.process(node.outer(), outerJob);
            Job innerJob = new Job(job.id());
            planVisitor.process(node.inner(), innerJob);
            return singleTask(
                    new NestedLoopTask(
                            job.id(),
                            clusterService.localNode().id(),
                            node,
                            outerJob,
                            innerJob,
                            TransportExecutor.this,
                            globalProjectionToProjectionVisitor,
                            circuitBreaker)
            );
        }

        @Override
        public ImmutableList<Task> visitESGetNode(ESGetNode node, Job job) {
            return singleTask(new ESGetTask(
                    job.id(),
                    functions,
                    globalProjectionToProjectionVisitor,
                    transportActionProvider.transportMultiGetAction(),
                    transportActionProvider.transportGetAction(),
                    node));
        }

        @Override
        public ImmutableList<Task> visitESDeleteByQueryNode(ESDeleteByQueryNode node, Job job) {
            return singleTask(new ESDeleteByQueryTask(
                    job.id(),
                    node,
                    transportActionProvider.transportDeleteByQueryAction()));
        }

        @Override
        public ImmutableList<Task> visitESDeleteNode(ESDeleteNode node, Job job) {
            return singleTask(new ESDeleteTask(
                    job.id(),
                    node,
                    transportActionProvider.transportDeleteAction()));
        }

        @Override
        public ImmutableList<Task> visitCreateTableNode(CreateTableNode node, Job job) {
            return singleTask(new CreateTableTask(
                            job.id(),
                            clusterService,
                            transportActionProvider.transportCreateIndexAction(),
                            transportActionProvider.transportDeleteIndexAction(),
                            transportActionProvider.transportPutIndexTemplateAction(),
                            node)
            );
        }

        @Override
        public ImmutableList<Task> visitESCreateTemplateNode(ESCreateTemplateNode node, Job job) {
            return singleTask(new ESCreateTemplateTask(job.id(),
                    node,
                    transportActionProvider.transportPutIndexTemplateAction()));
        }

        @Override
        public ImmutableList<Task> visitESCountNode(ESCountNode node, Job job) {
            return singleTask(new ESCountTask(job.id(), node,
                    transportActionProvider.transportCountAction()));
        }

        @Override
        public ImmutableList<Task> visitSymbolBasedUpsertByIdNode(SymbolBasedUpsertByIdNode node, Job job) {
            return singleTask(new SymbolBasedUpsertByIdTask(job.id(),
                    clusterService,
                    settings,
                    transportActionProvider.symbolBasedTransportShardUpsertActionDelegate(),
                    transportActionProvider.transportCreateIndexAction(),
                    node));
        }

        @Override
        public ImmutableList<Task> visitUpsertByIdNode(UpsertByIdNode node, Job job) {
            return singleTask(new UpsertByIdTask(
                    job.id(),
                    clusterService,
                    settings,
                    transportActionProvider.transportShardUpsertActionDelegate(),
                    transportActionProvider.transportCreateIndexAction(),
                    node));
        }

        @Override
        public ImmutableList<Task> visitDropTableNode(DropTableNode node, Job job) {
            return singleTask(new DropTableTask(job.id(),
                    transportActionProvider.transportDeleteIndexTemplateAction(),
                    transportActionProvider.transportDeleteIndexAction(),
                    node));
        }

        @Override
        public ImmutableList<Task> visitESDeleteIndexNode(ESDeleteIndexNode node, Job job) {
            return singleTask(new ESDeleteIndexTask(job.id(),
                    transportActionProvider.transportDeleteIndexAction(),
                    node));
        }

        @Override
        public ImmutableList<Task> visitESClusterUpdateSettingsNode(ESClusterUpdateSettingsNode node, Job job) {
            return singleTask(new ESClusterUpdateSettingsTask(
                    job.id(),
                    transportActionProvider.transportClusterUpdateSettingsAction(),
                    node));
        }

        @Override
        protected ImmutableList<Task> visitPlanNode(PlanNode node, Job job) {
            throw new UnsupportedOperationException(
                    String.format("Can't generate job/task for planNode %s", node));
        }
    }
}
