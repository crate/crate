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
import io.crate.executor.Executor;
import io.crate.executor.Job;
import io.crate.executor.Task;
import io.crate.executor.TaskResult;
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
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.controller.SearchPhaseController;
import org.elasticsearch.threadpool.ThreadPool;

import javax.annotation.Nullable;
import java.util.List;
import java.util.UUID;

public class TransportExecutor implements Executor {

    private final Functions functions;
    private final ReferenceResolver referenceResolver;
    private final SearchPhaseController searchPhaseController;
    private final StatsTables statsTables;
    private final Visitor visitor;
    private final ThreadPool threadPool;

    private final Settings settings;
    private final ClusterService clusterService;
    private final TransportActionProvider transportActionProvider;

    // operation for handler side collecting
    private final HandlerSideDataCollectOperation handlerSideDataCollectOperation;

    @Inject
    public TransportExecutor(Settings settings,
                             TransportActionProvider transportActionProvider,
                             ThreadPool threadPool,
                             Functions functions,
                             ReferenceResolver referenceResolver,
                             HandlerSideDataCollectOperation handlerSideDataCollectOperation,
                             SearchPhaseController searchPhaseController,
                             StatsTables statsTables,
                             ClusterService clusterService) {
        this.settings = settings;
        this.transportActionProvider = transportActionProvider;
        this.handlerSideDataCollectOperation = handlerSideDataCollectOperation;
        this.threadPool = threadPool;
        this.functions = functions;
        this.referenceResolver = referenceResolver;
        this.searchPhaseController = searchPhaseController;
        this.statsTables = statsTables;
        this.clusterService = clusterService;
        this.visitor = new Visitor();
    }

    @Override
    public Job newJob(Plan node) {
        final Job job = new Job();
        for (PlanNode planNode : node) {
            job.addTask(
                    planNode.accept(visitor, job.id())
            );
        }
        return job;
    }

    @Override
    public Task newTask(PlanNode planNode, UUID jobId) {
        return planNode.accept(visitor, jobId);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends TaskResult> List<ListenableFuture<T>> execute(Job job) {
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
        return (List<ListenableFuture<T>>)lastTask.result();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends TaskResult> List<ListenableFuture<T>> execute(Task<T> task, @Nullable Task upstreamTask) {
        if (upstreamTask != null) {
            task.upstreamResult(upstreamTask.result());
        }
        task.start();
        return task.result();
    }

    class Visitor extends PlanVisitor<UUID, Task> {

        @Override
        public Task visitCollectNode(CollectNode node, UUID jobId) {
            node.jobId(jobId); // add jobId to collectNode
            if (node.isRouted()) {
               return new RemoteCollectTask(
                    jobId,
                    node,
                    transportActionProvider.transportCollectNodeAction(),
                    handlerSideDataCollectOperation);
            } else {
                return new LocalCollectTask(jobId, handlerSideDataCollectOperation, node);
            }
        }

        @Override
        public Task visitMergeNode(MergeNode node, UUID jobId) {
            node.contextId(jobId);
            if (node.executionNodes().isEmpty()) {
                return new LocalMergeTask(
                        jobId,
                        threadPool,
                        clusterService,
                        settings,
                        transportActionProvider,
                        new ImplementationSymbolVisitor(referenceResolver, functions, RowGranularity.CLUSTER),
                        node,
                        statsTables);
            } else {
                return new DistributedMergeTask(
                        jobId,
                        transportActionProvider.transportMergeNodeAction(),
                        node);
            }

            
        }

        @Override
        public Task visitESSearchNode(QueryThenFetchNode node, UUID jobId) {
            return new QueryThenFetchTask(
                    jobId,
                    node,
                    clusterService,
                    transportActionProvider.transportQueryShardAction(),
                    transportActionProvider.searchServiceTransportAction(),
                    searchPhaseController,
                    threadPool);
            
        }

        @Override
        public Task visitESGetNode(ESGetNode node, UUID jobId) {
            return new ESGetTask(
                    jobId,
                    transportActionProvider.transportMultiGetAction(),
                    transportActionProvider.transportGetAction(),
                    node);
            
        }

        @Override
        public Task visitESDeleteByQueryNode(ESDeleteByQueryNode node, UUID jobId) {
            return new ESDeleteByQueryTask(
                    jobId,
                    node,
                    transportActionProvider.transportDeleteByQueryAction());
            
        }

        @Override
        public Task visitESDeleteNode(ESDeleteNode node, UUID jobId) {
            return new ESDeleteTask(
                    jobId,
                    transportActionProvider.transportDeleteAction(),
                    node);
            
        }

        @Override
        public Task visitCreateTableNode(CreateTableNode node, UUID jobId) {
            return new CreateTableTask(
                jobId,
                clusterService,
                transportActionProvider.transportCreateIndexAction(),
                transportActionProvider.transportDeleteIndexAction(),
                transportActionProvider.transportPutIndexTemplateAction(),
                node);
        }

        @Override
        public Task visitESCreateTemplateNode(ESCreateTemplateNode node, UUID jobId) {
            return new ESCreateTemplateTask(
                    jobId,
                    node,
                    transportActionProvider.transportPutIndexTemplateAction());
            
        }

        @Override
        public Task visitESCountNode(ESCountNode node, UUID jobId) {
            return new ESCountTask(
                    jobId,
                    node,
                    transportActionProvider.transportCountAction());
            
        }

        @Override
        public Task visitESIndexNode(ESIndexNode node, UUID jobId) {
            if (node.sourceMaps().size() > 1) {
                return new ESBulkIndexTask(
                        jobId,
                        clusterService,
                        settings,
                        transportActionProvider.transportShardBulkAction(),
                        transportActionProvider.transportCreateIndexAction(),
                        node);
            } else {
                return new ESIndexTask(
                        jobId,
                        transportActionProvider.transportIndexAction(),
                        node);
            }
            
        }

        @Override
        public Task visitESUpdateNode(ESUpdateNode node, UUID jobId) {
            // update with _version currently only possible in update by query
            if (node.ids().size() == 1 && node.routingValues().size() == 1) {
                return new ESUpdateByIdTask(
                        jobId,
                        transportActionProvider.transportUpdateAction(),
                        node);
            } else {
                return new ESUpdateByQueryTask(
                        jobId,
                        transportActionProvider.transportSearchAction(),
                        node);
            }
            
        }

        @Override
        public Task visitDropTableNode(DropTableNode node, UUID jobId) {
            return new DropTableTask(
                    jobId,
                    transportActionProvider.transportDeleteIndexTemplateAction(),
                    transportActionProvider.transportDeleteIndexAction(),
                    node);
            
        }

        @Override
        public Task visitESDeleteIndexNode(ESDeleteIndexNode node, UUID jobId) {
            return new ESDeleteIndexTask(
                    jobId,
                    transportActionProvider.transportDeleteIndexAction(),
                    node);
            
        }

        @Override
        public Task visitESClusterUpdateSettingsNode(ESClusterUpdateSettingsNode node, UUID jobId) {
            return new ESClusterUpdateSettingsTask(
                    jobId,
                    transportActionProvider.transportClusterUpdateSettingsAction(),
                    node);
            
        }

        @Override
        protected Task visitPlanNode(PlanNode node, UUID jobId) {
            throw new UnsupportedOperationException(
                    String.format("Can't generate job/task for planNode %s", node));
        }
    }
}
