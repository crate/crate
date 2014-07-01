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
import io.crate.executor.task.LocalCollectTask;
import io.crate.executor.task.LocalMergeTask;
import io.crate.executor.transport.merge.TransportMergeNodeAction;
import io.crate.executor.transport.task.DistributedMergeTask;
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
import org.elasticsearch.action.admin.cluster.settings.TransportClusterUpdateSettingsAction;
import org.elasticsearch.action.admin.indices.alias.TransportIndicesAliasesAction;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.admin.indices.delete.TransportDeleteIndexAction;
import org.elasticsearch.action.admin.indices.template.delete.TransportDeleteIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.TransportPutIndexTemplateAction;
import org.elasticsearch.action.bulk.TransportBulkAction;
import org.elasticsearch.action.bulk.TransportShardBulkAction;
import org.elasticsearch.action.count.TransportCountAction;
import org.elasticsearch.action.delete.TransportDeleteAction;
import org.elasticsearch.action.deletebyquery.TransportDeleteByQueryAction;
import org.elasticsearch.action.get.TransportGetAction;
import org.elasticsearch.action.get.TransportMultiGetAction;
import org.elasticsearch.action.index.TransportIndexAction;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.update.TransportUpdateAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.List;

public class TransportExecutor implements Executor {

    private final Functions functions;
    private final ReferenceResolver referenceResolver;
    private final StatsTables statsTables;
    private final Visitor visitor;
    private final ThreadPool threadPool;

    private final TransportSearchAction transportSearchAction;
    private final TransportCollectNodeAction transportCollectNodeAction;
    private final TransportMergeNodeAction transportMergeNodeAction;
    private final ClusterService clusterService;
    private final Settings settings;
    private final TransportShardBulkAction transportShardBulkAction;
    private final TransportGetAction transportGetAction;
    private final TransportMultiGetAction transportMultiGetAction;
    private final TransportDeleteByQueryAction transportDeleteByQueryAction;
    private final TransportDeleteAction transportDeleteAction;
    private final TransportCreateIndexAction transportCreateIndexAction;
    private final TransportCountAction transportCountAction;
    private final TransportIndexAction transportIndexAction;
    private final TransportBulkAction transportBulkAction;
    private final TransportUpdateAction transportUpdateAction;
    private final TransportDeleteIndexAction transportDeleteIndexAction;
    private final TransportClusterUpdateSettingsAction transportClusterUpdateSettingsAction;
    private final TransportPutIndexTemplateAction transportPutIndexTemplateAction;
    private final TransportDeleteIndexTemplateAction transportDeleteIndexTemplateAction;
    private final TransportIndicesAliasesAction transportCreateAliasAction;
    // operation for handler side collecting
    private final HandlerSideDataCollectOperation handlerSideDataCollectOperation;

    @Inject
    public TransportExecutor(ClusterService clusterService,
                             Settings settings,
                             TransportShardBulkAction transportShardBulkAction,
                             TransportSearchAction transportSearchAction,
                             TransportCollectNodeAction transportCollectNodeAction,
                             TransportMergeNodeAction transportMergeNodeAction,
                             TransportGetAction transportGetAction,
                             TransportMultiGetAction transportMultiGetAction,
                             TransportDeleteByQueryAction transportDeleteByQueryAction,
                             TransportDeleteAction transportDeleteAction,
                             TransportCreateIndexAction transportCreateIndexAction,
                             TransportCountAction transportCountAction,
                             ThreadPool threadPool,
                             Functions functions,
                             ReferenceResolver referenceResolver,
                             TransportIndexAction transportIndexAction,
                             TransportBulkAction transportBulkAction,
                             TransportUpdateAction transportUpdateAction,
                             TransportDeleteIndexAction transportDeleteIndexAction,
                             TransportClusterUpdateSettingsAction transportClusterUpdateSettingsAction,
                             TransportPutIndexTemplateAction transportPutIndexTemplateAction,
                             TransportDeleteIndexTemplateAction transportDeleteIndexTemplateAction,
                             TransportIndicesAliasesAction transportCreateAliasAction,
                             HandlerSideDataCollectOperation handlerSideDataCollectOperation,
                             StatsTables statsTables) {
        this.clusterService = clusterService;
        this.settings = settings;
        this.transportShardBulkAction = transportShardBulkAction;
        this.transportGetAction = transportGetAction;
        this.transportMultiGetAction = transportMultiGetAction;
        this.transportCollectNodeAction = transportCollectNodeAction;
        this.transportMergeNodeAction = transportMergeNodeAction;
        this.transportSearchAction = transportSearchAction;
        this.transportDeleteByQueryAction = transportDeleteByQueryAction;
        this.transportDeleteAction = transportDeleteAction;
        this.transportCreateIndexAction = transportCreateIndexAction;
        this.transportCountAction = transportCountAction;
        this.transportIndexAction = transportIndexAction;
        this.transportBulkAction = transportBulkAction;
        this.transportUpdateAction = transportUpdateAction;
        this.transportDeleteIndexAction = transportDeleteIndexAction;
        this.transportClusterUpdateSettingsAction = transportClusterUpdateSettingsAction;
        this.transportPutIndexTemplateAction = transportPutIndexTemplateAction;
        this.transportDeleteIndexTemplateAction = transportDeleteIndexTemplateAction;
        this.transportCreateAliasAction = transportCreateAliasAction;

        this.handlerSideDataCollectOperation = handlerSideDataCollectOperation;
        this.threadPool = threadPool;
        this.functions = functions;
        this.referenceResolver = referenceResolver;
        this.statsTables = statsTables;
        this.visitor = new Visitor();
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
    public List<ListenableFuture<Object[][]>> execute(Job job) {
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
        return lastTask.result();
    }

    class Visitor extends PlanVisitor<Job, Void> {

        @Override
        public Void visitCollectNode(CollectNode node, Job context) {
            node.jobId(context.id()); // add jobId to collectNode
            if (node.isRouted()) {
                context.addTask(new RemoteCollectTask(
                    node,
                    transportCollectNodeAction,
                    handlerSideDataCollectOperation));
            } else {
                context.addTask(new LocalCollectTask(handlerSideDataCollectOperation, node));
            }
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
                        transportShardBulkAction,
                        transportCreateIndexAction,
                        new ImplementationSymbolVisitor(referenceResolver, functions, RowGranularity.CLUSTER),
                        node,
                        statsTables));
            } else {
                context.addTask(new DistributedMergeTask(transportMergeNodeAction, node));
            }

            return null;
        }

        @Override
        public Void visitESSearchNode(ESSearchNode node, Job context) {
            context.addTask(new ESSearchTask(node, transportSearchAction));
            return null;
        }

        @Override
        public Void visitESGetNode(ESGetNode node, Job context) {
            context.addTask(new ESGetTask(transportMultiGetAction, transportGetAction, node));
            return null;
        }

        @Override
        public Void visitESDeleteByQueryNode(ESDeleteByQueryNode node, Job context) {
            context.addTask(new ESDeleteByQueryTask(node, transportDeleteByQueryAction));
            return null;
        }

        @Override
        public Void visitESDeleteNode(ESDeleteNode node, Job context) {
            context.addTask(new ESDeleteTask(transportDeleteAction, node));
            return null;
        }

        @Override
        public Void visitESCreateIndexNode(ESCreateIndexNode node, Job context) {
            context.addTask(new ESCreateIndexTask(node, transportCreateIndexAction));
            return null;
        }

        @Override
        public Void visitESCreateAliasNode(ESCreateAliasNode node, Job context) {
            context.addTask(new ESCreateAliasTask(node, transportCreateAliasAction));
            return null;
        }

        @Override
        public Void visitESCreateTemplateNode(ESCreateTemplateNode node, Job context) {
            context.addTask(new ESCreateTemplateTask(node, transportPutIndexTemplateAction));
            return null;
        }

        @Override
        public Void visitESDeleteTemplateNode(ESDeleteTemplateNode node, Job context) {
            context.addTask(new ESDeleteTemplateTask(node, transportDeleteIndexTemplateAction));
            return null;
        }

        @Override
        public Void visitESCountNode(ESCountNode node, Job context) {
            context.addTask(new ESCountTask(node, transportCountAction));
            return null;
        }

        @Override
        public Void visitESIndexNode(ESIndexNode node, Job context) {
            if (node.sourceMaps().size() > 1) {
                context.addTask(new ESBulkIndexTask(threadPool, transportBulkAction, node));
            } else {
                context.addTask(new ESIndexTask(transportIndexAction, node));
            }
            return null;
        }

        @Override
        public Void visitESUpdateNode(ESUpdateNode node, Job context) {
            // update with _version currently only possible in update by query
            if (node.ids().size() == 1 && node.routingValues().size() == 1) {
                context.addTask(new ESUpdateByIdTask(transportUpdateAction, node));
            } else {
                context.addTask(new ESUpdateByQueryTask(transportSearchAction, node));
            }
            return null;
        }

        @Override
        public Void visitESDeleteIndexNode(ESDeleteIndexNode node, Job context) {
            context.addTask(new ESDeleteIndexTask(transportDeleteIndexAction, node));
            return null;
        }

        @Override
        public Void visitESClusterUpdateSettingsNode(ESClusterUpdateSettingsNode node, Job context) {
            context.addTask(new ESClusterUpdateSettingsTask(transportClusterUpdateSettingsAction, node));
            return null;
        }

        @Override
        protected Void visitPlanNode(PlanNode node, Job context) {
            throw new UnsupportedOperationException(
                    String.format("Can't generate job/task for planNode %s", node));
        }
    }
}
