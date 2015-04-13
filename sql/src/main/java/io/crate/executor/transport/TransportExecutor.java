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
import io.crate.executor.transport.task.*;
import io.crate.executor.transport.task.elasticsearch.*;
import io.crate.metadata.Functions;
import io.crate.metadata.ReferenceResolver;
import io.crate.operation.ImplementationSymbolVisitor;
import io.crate.operation.collect.HandlerSideDataCollectOperation;
import io.crate.operation.collect.StatsTables;
import io.crate.operation.PageDownstreamFactory;
import io.crate.operation.projectors.ProjectionToProjectorVisitor;
import io.crate.planner.*;
import io.crate.planner.node.PlanNode;
import io.crate.planner.node.PlanNodeVisitor;
import io.crate.planner.node.StreamerVisitor;
import io.crate.planner.node.ddl.*;
import io.crate.planner.node.dml.*;
import io.crate.planner.node.dql.*;
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
import java.util.UUID;

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

    private final PageDownstreamFactory pageDownstreamFactory;

    private final StreamerVisitor streamerVisitor;

    @Inject
    public TransportExecutor(Settings settings,
                             TransportActionProvider transportActionProvider,
                             ThreadPool threadPool,
                             Functions functions,
                             ReferenceResolver referenceResolver,
                             HandlerSideDataCollectOperation handlerSideDataCollectOperation,
                             PageDownstreamFactory pageDownstreamFactory,
                             Provider<DDLStatementDispatcher> ddlAnalysisDispatcherProvider,
                             StatsTables statsTables,
                             ClusterService clusterService,
                             CrateCircuitBreakerService breakerService,
                             StreamerVisitor streamerVisitor) {
        this.settings = settings;
        this.transportActionProvider = transportActionProvider;
        this.handlerSideDataCollectOperation = handlerSideDataCollectOperation;
        this.pageDownstreamFactory = pageDownstreamFactory;
        this.threadPool = threadPool;
        this.functions = functions;
        this.ddlAnalysisDispatcherProvider = ddlAnalysisDispatcherProvider;
        this.statsTables = statsTables;
        this.clusterService = clusterService;
        this.streamerVisitor = streamerVisitor;
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
    public List<Task> newTasks(PlanNode planNode, UUID jobId) {
        return planNode.accept(nodeVisitor, jobId);
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
                job.addTasks(planNode.accept(nodeVisitor, job.id()));
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
            job.addTasks(nodeVisitor.visitCollectNode(plan.collectNode(), job.id()));
            job.addTasks(nodeVisitor.visitMergeNode(plan.mergeNode(), job.id()));
            return null;
        }

        @Override
        public Void visitQueryAndFetch(QueryAndFetch plan, Job job) {
            job.addTasks(nodeVisitor.visitCollectNode(plan.collectNode(), job.id()));
            job.addTasks(nodeVisitor.visitMergeNode(plan.localMergeNode(), job.id()));
            return null;
        }

        @Override
        public Void visitNonDistributedGroupBy(NonDistributedGroupBy plan, Job job) {
            job.addTasks(nodeVisitor.visitCollectNode(plan.collectNode(), job.id()));
            job.addTasks(nodeVisitor.visitMergeNode(plan.localMergeNode(), job.id()));
            return null;
        }

        @Override
        public Void visitUpsert(Upsert plan, Job job) {
            ImmutableList.Builder<Task> taskBuilder = ImmutableList.builder();
            for (List<DQLPlanNode> childNodes : plan.nodes()) {
                List<Task> subTasks = new ArrayList<>(childNodes.size());
                for (DQLPlanNode childNode : childNodes) {
                    subTasks.addAll(childNode.accept(nodeVisitor, job.id()));
                }
                UpsertTask upsertTask = new UpsertTask(TransportExecutor.this, job.id(), subTasks);
                taskBuilder.add(upsertTask);
            }
            job.addTasks(taskBuilder.build());
            return null;
        }

        @Override
        public Void visitDistributedGroupBy(DistributedGroupBy plan, Job job) {
            job.addTasks(nodeVisitor.visitCollectNode(plan.collectNode(), job.id()));
            job.addTasks(nodeVisitor.visitMergeNode(plan.reducerMergeNode(), job.id()));
            job.addTasks(nodeVisitor.visitMergeNode(plan.localMergeNode(), job.id()));
            return null;
        }

        @Override
        public Void visitInsertByQuery(InsertFromSubQuery node, Job job) {
            this.process(node.innerPlan(), job);
            if(node.handlerMergeNode().isPresent()) {
                job.addTasks(nodeVisitor.visitMergeNode(node.handlerMergeNode().get(), job.id()));
            }
            return null;
        }

        @Override
        public Void visitQueryThenFetch(QueryThenFetch plan, Job job) {
            job.addTasks(nodeVisitor.visitCollectNode(plan.collectNode(), job.id()));
            job.addTasks(nodeVisitor.visitMergeNode(plan.mergeNode(), job.id()));
            return null;
        }
    }

    class NodeVisitor extends PlanNodeVisitor<UUID, ImmutableList<Task>> {

        private ImmutableList<Task> singleTask(Task task) {
            return ImmutableList.of(task);
        }

        @Override
        public ImmutableList<Task> visitCollectNode(CollectNode node, UUID jobId) {
            node.jobId(jobId); // add jobId to collectNode
            if (node.isRouted()) {
                return singleTask(
                    new RemoteCollectTask(
                        jobId,
                        node,
                        transportActionProvider.transportJobInitAction(),
                        transportActionProvider.transportCloseContextNodeAction(),
                        streamerVisitor,
                        handlerSideDataCollectOperation,
                        globalProjectionToProjectionVisitor,
                        statsTables,
                        circuitBreaker
                    )
                );
            } else {
                return singleTask(
                        new LocalCollectTask(
                            jobId,
                            handlerSideDataCollectOperation,
                            node,
                            circuitBreaker
                        )
                );
            }

        }

        @Override
        public ImmutableList<Task> visitGenericDDLNode(GenericDDLNode node, UUID jobId) {
            return singleTask(new DDLTask(jobId, ddlAnalysisDispatcherProvider.get(), node));
        }

        @Override
        public ImmutableList<Task> visitMergeNode(@Nullable MergeNode node, UUID jobId) {
            if (node == null) {
               return ImmutableList.of();
            }
            node.jobId(jobId);
            if (node.executionNodes().isEmpty()) {
                return singleTask(new LocalMergeTask(
                        jobId,
                        pageDownstreamFactory,
                        node,
                        statsTables,
                        circuitBreaker, threadPool));
            } else {
                return singleTask(new DistributedMergeTask(
                        jobId,
                        transportActionProvider.transportDistributedResultAction(), node));
            }
        }

        @Override
        public ImmutableList<Task> visitESGetNode(ESGetNode node, UUID jobId) {
            return singleTask(new ESGetTask(
                    jobId,
                    functions,
                    globalProjectionToProjectionVisitor,
                    transportActionProvider.transportMultiGetAction(),
                    transportActionProvider.transportGetAction(),
                    node));
        }

        @Override
        public ImmutableList<Task> visitESDeleteByQueryNode(ESDeleteByQueryNode node, UUID jobId) {
            return singleTask(new ESDeleteByQueryTask(
                    jobId,
                    node,
                    transportActionProvider.transportDeleteByQueryAction()));
        }

        @Override
        public ImmutableList<Task> visitESDeleteNode(ESDeleteNode node, UUID jobId) {
            return singleTask(new ESDeleteTask(
                    jobId,
                    node,
                    transportActionProvider.transportDeleteAction()));
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
        public ImmutableList<Task> visitESCountNode(ESCountNode node, UUID jobId) {
            return singleTask(new ESCountTask(jobId, node,
                    transportActionProvider.transportCountAction()));
        }

        @Override
        public ImmutableList<Task> visitSymbolBasedUpsertByIdNode(SymbolBasedUpsertByIdNode node, UUID jobId) {
            return singleTask(new SymbolBasedUpsertByIdTask(jobId,
                    clusterService,
                    settings,
                    transportActionProvider.symbolBasedTransportShardUpsertActionDelegate(),
                    transportActionProvider.transportCreateIndexAction(),
                    node));
        }

        @Override
        public ImmutableList<Task> visitUpsertByIdNode(UpsertByIdNode node, UUID jobId) {
            return singleTask(new UpsertByIdTask(jobId,
                    clusterService,
                    settings,
                    transportActionProvider.transportShardUpsertActionDelegate(),
                    transportActionProvider.transportCreateIndexAction(),
                    node));
        }

        @Override
        public ImmutableList<Task> visitDropTableNode(DropTableNode node, UUID jobId) {
            return singleTask(new DropTableTask(jobId,
                    transportActionProvider.transportDeleteIndexTemplateAction(),
                    transportActionProvider.transportDeleteIndexAction(),
                    node));
        }

        @Override
        public ImmutableList<Task> visitESDeleteIndexNode(ESDeleteIndexNode node, UUID jobId) {
            return singleTask(new ESDeleteIndexTask(jobId,
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
}
