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
import io.crate.executor.task.join.NestedLoopTask;
import io.crate.executor.transport.task.CreateTableTask;
import io.crate.executor.transport.task.DistributedMergeTask;
import io.crate.executor.transport.task.DropTableTask;
import io.crate.executor.transport.task.RemoteCollectTask;
import io.crate.executor.transport.task.elasticsearch.*;
import io.crate.metadata.Functions;
import io.crate.metadata.ReferenceResolver;
import io.crate.operation.ImplementationSymbolVisitor;
import io.crate.operation.collect.HandlerSideDataCollectOperation;
import io.crate.operation.collect.StatsTables;
import io.crate.operation.projectors.ProjectionToProjectorVisitor;
import io.crate.planner.Plan;
import io.crate.planner.RowGranularity;
import io.crate.planner.node.PlanNode;
import io.crate.planner.node.PlanVisitor;
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
import org.elasticsearch.search.controller.SearchPhaseController;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Collection;
import java.util.List;
import java.util.UUID;

public class TransportExecutor implements Executor, TaskExecutor {

    private final Functions functions;
    private final Provider<SearchPhaseController> searchPhaseControllerProvider;
    private Provider<DDLStatementDispatcher> ddlAnalysisDispatcherProvider;
    private final StatsTables statsTables;
    private final Visitor visitor;
    private final ThreadPool threadPool;

    private final Settings settings;
    private final ClusterService clusterService;
    private final TransportActionProvider transportActionProvider;

    private final ImplementationSymbolVisitor globalImplementationSymbolVisitor;
    private final ProjectionToProjectorVisitor globalProjectionToProjectionVisitor;

    // operation for handler side collecting
    private final HandlerSideDataCollectOperation handlerSideDataCollectOperation;
    private final CircuitBreaker circuitBreaker;

    @Inject
    public TransportExecutor(Settings settings,
                             TransportActionProvider transportActionProvider,
                             ThreadPool threadPool,
                             Functions functions,
                             ReferenceResolver referenceResolver,
                             HandlerSideDataCollectOperation handlerSideDataCollectOperation,
                             Provider<SearchPhaseController> searchPhaseControllerProvider,
                             Provider<DDLStatementDispatcher> ddlAnalysisDispatcherProvider,
                             StatsTables statsTables,
                             ClusterService clusterService,
                             CrateCircuitBreakerService breakerService) {
        this.settings = settings;
        this.transportActionProvider = transportActionProvider;
        this.handlerSideDataCollectOperation = handlerSideDataCollectOperation;
        this.threadPool = threadPool;
        this.functions = functions;
        this.searchPhaseControllerProvider = searchPhaseControllerProvider;
        this.ddlAnalysisDispatcherProvider = ddlAnalysisDispatcherProvider;
        this.statsTables = statsTables;
        this.clusterService = clusterService;
        this.visitor = new Visitor();
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
        for (PlanNode planNode : plan) {
            job.addTasks(planNode.accept(visitor, job.id()));
        }
        return job;
    }

    @Override
    public List<ListenableFuture<TaskResult>> execute(Job job) {
        assert job.tasks().size() > 0;
        return execute(job.tasks(), null);

    }

    @Override
    public List<Task> newTasks(PlanNode planNode, UUID jobId) {
        return planNode.accept(visitor, jobId);
    }

    @Override
    public List<ListenableFuture<TaskResult>> execute(Collection<Task> tasks, @Nullable Task upstreamTask) {
        Task lastTask = upstreamTask;
        for (Task task : tasks) {
            // chaining tasks
            if (lastTask != null) {
                task.upstreamResult(lastTask.result());
            }
            task.start();
            lastTask = task;
        }
        assert lastTask != upstreamTask;
        return lastTask.result();
    }

    class Visitor extends PlanVisitor<UUID, ImmutableList<Task>> {

        private ImmutableList<Task> singleTask(Task task) {
            return ImmutableList.of(task);
        }

        @Override
        public ImmutableList<Task> visitCollectNode(CollectNode node, UUID jobId) {
            node.jobId(jobId); // add jobId to collectNode
            if (node.isRouted()) {
                return singleTask(new RemoteCollectTask(
                        jobId,
                        node,
                        transportActionProvider.transportCollectNodeAction(),
                        handlerSideDataCollectOperation,
                        statsTables,
                        circuitBreaker));
            } else {
                return singleTask(new LocalCollectTask(
                        jobId,
                        handlerSideDataCollectOperation,
                        node,
                        circuitBreaker));
            }

        }

        @Override
        public ImmutableList<Task> visitGenericDDLNode(GenericDDLNode node, UUID jobId) {
            return singleTask(new DDLTask(jobId, ddlAnalysisDispatcherProvider.get(), node));
        }

        @Override
        public ImmutableList<Task> visitMergeNode(MergeNode node, UUID jobId) {
            node.contextId(jobId);
            if (node.executionNodes().isEmpty()) {
                return singleTask(new LocalMergeTask(
                        jobId,
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
                        jobId,
                        transportActionProvider.transportMergeNodeAction(), node));
            }
        }

        @Override
        public ImmutableList<Task> visitGlobalAggregateNode(GlobalAggregateNode node, UUID jobId) {
            return ImmutableList.<Task>builder()
                    .addAll(
                            visitCollectNode(node.collectNode(), jobId))
                    .addAll(
                            visitMergeNode(node.mergeNode(), jobId))
                    .build();
        }

        @Override
        public ImmutableList<Task> visitDistributedGroupByNode(DistributedGroupByNode node, UUID jobId) {
            return ImmutableList.<Task>builder()
                    .addAll(
                            visitCollectNode(node.collectNode(), jobId)
                    ).addAll(
                            visitMergeNode(node.reducerMergeNode(), jobId)
                    ).addAll(
                            visitMergeNode(node.localMergeNode(), jobId)
                    ).build();
        }

        @Override
        public ImmutableList<Task> visitNonDistributedGroupByNode(NonDistributedGroupByNode node, UUID jobId){
            return ImmutableList.<Task>builder()
                    .addAll(
                            visitCollectNode(node.collectNode(), jobId)
                    ).addAll(
                            visitMergeNode(node.localMergeNode(), jobId)
                    ).build();
        }

        @Override
        public ImmutableList<Task> visitQueryAndFetchNode(QueryAndFetchNode node, UUID jobId){
            return ImmutableList.<Task>builder()
                    .addAll(
                            visitCollectNode(node.collectNode(), jobId)
                    ).addAll(
                            visitMergeNode(node.localMergeNode(), jobId)
                    ).build();
        }

        @Override
        public ImmutableList<Task> visitQueryThenFetchNode(QueryThenFetchNode node, UUID jobId) {
            return singleTask(new QueryThenFetchTask(
                    jobId,
                    functions,
                    node,
                    clusterService,
                    transportActionProvider.transportQueryShardAction(),
                    transportActionProvider.searchServiceTransportAction(),
                    searchPhaseControllerProvider.get(),
                    threadPool));
        }

        @Override
        public ImmutableList<Task> visitNestedLoopNode(NestedLoopNode node, UUID jobId) {
            return singleTask(new NestedLoopTask(
                        jobId,
                        clusterService.localNode().id(),
                        node,
                        TransportExecutor.this,
                        globalProjectionToProjectionVisitor,
                        circuitBreaker));
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
        public ImmutableList<Task> visitESIndexNode(ESIndexNode node, UUID jobId) {
            if (node.sourceMaps().size() > 1) {
                return singleTask(new ESBulkIndexTask(jobId, clusterService, settings,
                        transportActionProvider.transportShardBulkAction(),
                        transportActionProvider.transportCreateIndexAction(),
                        node));
            } else {
                return singleTask(new ESIndexTask(
                        jobId,
                        transportActionProvider.transportIndexAction(),
                        node));
            }
        }

        @Override
        public ImmutableList<Task> visitESUpdateNode(ESUpdateNode node, UUID jobId) {
            // update with _version currently only possible in update by query
            if (node.ids().size() == 1 && node.routingValues().size() == 1) {
                return singleTask(new ESUpdateByIdTask(
                        jobId,
                        transportActionProvider.transportUpdateAction(),
                        node));
            } else {
                return singleTask(new ESUpdateByQueryTask(
                        jobId,
                        transportActionProvider.transportSearchAction(),
                        node));
            }
        }

        @Override
        public ImmutableList<Task> visitUpdateNode(UpdateNode node, UUID jobId) {
            ImmutableList.Builder<Task> taskBuilder = ImmutableList.builder();
            for (CollectNode collectNode : node.collectNodes()) {
                taskBuilder.addAll(visitCollectNode(collectNode, jobId));
            }
            taskBuilder.addAll(visitMergeNode(node.mergeNode(), jobId));
            return taskBuilder.build();
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
