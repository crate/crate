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

import com.google.common.util.concurrent.ListenableFuture;
import io.crate.action.sql.DDLStatementDispatcher;
import io.crate.breaker.CrateCircuitBreakerService;
import io.crate.executor.Executor;
import io.crate.executor.Job;
import io.crate.executor.Task;
import io.crate.executor.TaskResult;
import io.crate.executor.task.DDLTask;
import io.crate.executor.task.LocalCollectTask;
import io.crate.executor.task.LocalMergeTask;
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
import io.crate.planner.node.dml.ESDeleteByQueryNode;
import io.crate.planner.node.dml.ESDeleteNode;
import io.crate.planner.node.dml.ESIndexNode;
import io.crate.planner.node.dml.ESUpdateNode;
import io.crate.planner.node.dql.*;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.controller.SearchPhaseController;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.List;

public class TransportExecutor implements Executor {

    private final Functions functions;
    private final ReferenceResolver referenceResolver;
    private final Provider<SearchPhaseController> searchPhaseControllerProvider;
    private Provider<DDLStatementDispatcher> ddlAnalysisDispatcherProvider;
    private final StatsTables statsTables;
    private final Visitor visitor;
    private final ThreadPool threadPool;

    private final Settings settings;
    private final ClusterService clusterService;
    private final TransportActionProvider transportActionProvider;

    // operation for handler side collecting
    private final HandlerSideDataCollectOperation handlerSideDataCollectOperation;
    private final ProjectionToProjectorVisitor projectorVisitor;
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
        this.referenceResolver = referenceResolver;
        this.searchPhaseControllerProvider = searchPhaseControllerProvider;
        this.ddlAnalysisDispatcherProvider = ddlAnalysisDispatcherProvider;
        this.statsTables = statsTables;
        this.clusterService = clusterService;
        this.visitor = new Visitor();
        this.circuitBreaker = breakerService.getBreaker(CrateCircuitBreakerService.QUERY_BREAKER);
        ImplementationSymbolVisitor clusterImplementationSymbolVisitor =
                new ImplementationSymbolVisitor(referenceResolver, functions, RowGranularity.CLUSTER);
        projectorVisitor = new ProjectionToProjectorVisitor(
                clusterService, settings, transportActionProvider,
                clusterImplementationSymbolVisitor);
    }

    @Override
    public Job newJob(Plan node) {
        final Job job = new Job();
        for (PlanNode planNode : node) {
            planNode.accept(visitor, job);
        }
        return job;
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<ListenableFuture<TaskResult>> execute(Job job) {
        assert job.tasks().size() > 0;

        Task lastTask = null;
        for (Task task : job.tasks()) {
            // chaining tasks
            if (lastTask != null) {
                task.upstreamResult(lastTask.result());
            }
            task.start();
            lastTask = task;
        }

        assert lastTask != null;
        return (List<ListenableFuture<TaskResult>>)lastTask.result();
    }

    class Visitor extends PlanVisitor<Job, Void> {

        @Override
        public Void visitCollectNode(CollectNode node, Job context) {
            node.jobId(context.id()); // add jobId to collectNode
            if (node.isRouted()) {
                context.addTask(new RemoteCollectTask(
                    node,
                    transportActionProvider.transportCollectNodeAction(),
                    handlerSideDataCollectOperation, statsTables, circuitBreaker));
            } else {
                context.addTask(new LocalCollectTask(
                        handlerSideDataCollectOperation, node, circuitBreaker));
            }
            return null;
        }

        @Override
        public Void visitGenericDDLPlanNode(GenericDDLPlanNode genericDDLPlanNode, Job context) {
            context.addTask(new DDLTask(ddlAnalysisDispatcherProvider.get(), genericDDLPlanNode));
            return null;
        }

        @Override
        public Void visitMergeNode(MergeNode node, Job context) {
            node.contextId(context.id());
            if (node.executionNodes().isEmpty()) {
                context.addTask(new LocalMergeTask(
                        threadPool,
                        clusterService,
                        settings,
                        transportActionProvider,
                        new ImplementationSymbolVisitor(referenceResolver, functions, RowGranularity.CLUSTER),
                        node,
                        statsTables,
                        circuitBreaker));
            } else {
                context.addTask(new DistributedMergeTask(
                        transportActionProvider.transportMergeNodeAction(), node));
            }

            return null;
        }

        @Override
        public Void visitGlobalAggregateNode(GlobalAggregateNode globalAggregateNode, Job context) {
            visitCollectNode(globalAggregateNode.collectNode(), context);
            visitMergeNode(globalAggregateNode.mergeNode(), context);
            return null;
        }

        @Override
        public Void visitDistributedGroupByPlanNode(DistributedGroupByPlanNode distributedGroupByPlanNode, Job context) {
            visitCollectNode(distributedGroupByPlanNode.collectNode(), context);
            visitMergeNode(distributedGroupByPlanNode.reducerMergeNode(), context);
            visitMergeNode(distributedGroupByPlanNode.localMergeNode(), context);
            return null;
        }

        @Override
        public Void visitQueryThenFetchNode(QueryThenFetchNode node, Job context) {
            context.addTask(new QueryThenFetchTask(
                    functions,
                    node,
                    clusterService,
                    transportActionProvider.transportQueryShardAction(),
                    transportActionProvider.searchServiceTransportAction(),
                    searchPhaseControllerProvider.get(),
                    threadPool));
            return null;
        }

        @Override
        public Void visitESGetNode(ESGetNode node, Job context) {
            context.addTask(new ESGetTask(
                    functions,
                    projectorVisitor,
                    transportActionProvider.transportMultiGetAction(),
                    transportActionProvider.transportGetAction(),
                    node));
            return null;
        }

        @Override
        public Void visitESDeleteByQueryNode(ESDeleteByQueryNode node, Job context) {
            context.addTask(new ESDeleteByQueryTask(node,
                    transportActionProvider.transportDeleteByQueryAction()));
            return null;
        }

        @Override
        public Void visitESDeleteNode(ESDeleteNode node, Job context) {
            context.addTask(new ESDeleteTask(
                    transportActionProvider.transportDeleteAction(),
                    node));
            return null;
        }

        @Override
        public Void visitCreateTableNode(CreateTableNode node, Job context) {
            context.addTask(new CreateTableTask(clusterService,
                transportActionProvider.transportCreateIndexAction(),
                transportActionProvider.transportDeleteIndexAction(),
                transportActionProvider.transportPutIndexTemplateAction(),
                node)
            );
            return null;
        }

        @Override
        public Void visitESCreateTemplateNode(ESCreateTemplateNode node, Job context) {
            context.addTask(new ESCreateTemplateTask(node,
                    transportActionProvider.transportPutIndexTemplateAction()));
            return null;
        }

        @Override
        public Void visitESCountNode(ESCountNode node, Job context) {
            context.addTask(new ESCountTask(node,
                    transportActionProvider.transportCountAction()));
            return null;
        }

        @Override
        public Void visitESIndexNode(ESIndexNode node, Job context) {
            if (node.sourceMaps().size() > 1) {
                context.addTask(new ESBulkIndexTask(clusterService, settings,
                        transportActionProvider.transportShardBulkAction(),
                        transportActionProvider.transportCreateIndexAction(),
                        node));
            } else {
                context.addTask(new ESIndexTask(
                        transportActionProvider.transportIndexAction(),
                        node));
            }
            return null;
        }

        @Override
        public Void visitESUpdateNode(ESUpdateNode node, Job context) {
            // update with _version currently only possible in update by query
            if (node.ids().size() == 1 && node.routingValues().size() == 1) {
                context.addTask(new ESUpdateByIdTask(
                        transportActionProvider.transportUpdateAction(),
                        node));
            } else {
                context.addTask(new ESUpdateByQueryTask(
                        transportActionProvider.transportSearchAction(),
                        node));
            }
            return null;
        }

        @Override
        public Void visitDropTableNode(DropTableNode node, Job context) {
            context.addTask(new DropTableTask(
                    transportActionProvider.transportDeleteIndexTemplateAction(),
                    transportActionProvider.transportDeleteIndexAction(),
                    node));
            return null;
        }

        @Override
        public Void visitESDeleteIndexNode(ESDeleteIndexNode node, Job context) {
            context.addTask(new ESDeleteIndexTask(
                    transportActionProvider.transportDeleteIndexAction(),
                    node));
            return null;
        }

        @Override
        public Void visitESClusterUpdateSettingsNode(ESClusterUpdateSettingsNode node, Job context) {
            context.addTask(new ESClusterUpdateSettingsTask(
                    transportActionProvider.transportClusterUpdateSettingsAction(),
                    node));
            return null;
        }

        @Override
        protected Void visitPlanNode(PlanNode node, Job context) {
            throw new UnsupportedOperationException(
                    String.format("Can't generate job/task for planNode %s", node));
        }
    }
}
