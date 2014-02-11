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
import io.crate.executor.task.LocalMergeTask;
import io.crate.executor.transport.merge.TransportMergeNodeAction;
import io.crate.executor.transport.task.DistributedMergeTask;
import io.crate.executor.transport.task.RemoteCollectTask;
import io.crate.executor.transport.task.elasticsearch.ESGetTask;
import io.crate.executor.transport.task.elasticsearch.ESSearchTask;
import io.crate.metadata.Functions;
import io.crate.metadata.ReferenceResolver;
import io.crate.operator.operations.ImplementationSymbolVisitor;
import io.crate.planner.Plan;
import io.crate.planner.RowGranularity;
import io.crate.planner.node.*;
import org.elasticsearch.action.get.TransportGetAction;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.List;

public class TransportExecutor implements Executor {

    private final Functions functions;
    private final ReferenceResolver referenceResolver;
    private final Visitor visitor;
    private final ThreadPool threadPool;

    private final TransportSearchAction transportSearchAction;
    private final TransportCollectNodeAction transportCollectNodeAction;
    private final TransportMergeNodeAction transportMergeNodeAction;
    private final TransportGetAction transportGetAction;

    @Inject
    public TransportExecutor(TransportSearchAction transportSearchAction,
                             TransportCollectNodeAction transportCollectNodeAction,
                             TransportMergeNodeAction transportMergeNodeAction,
                             TransportGetAction transportGetAction,
                             ThreadPool threadPool,
                             Functions functions,
                             ReferenceResolver referenceResolver) {
        this.transportGetAction = transportGetAction;
        this.transportCollectNodeAction = transportCollectNodeAction;
        this.transportMergeNodeAction = transportMergeNodeAction;
        this.transportSearchAction = transportSearchAction;

        this.threadPool = threadPool;
        this.functions = functions;
        this.referenceResolver = referenceResolver;
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
            if (node.isRouted()) {
                // TODO: add contextId to task/node
                context.addTask(new RemoteCollectTask(node, transportCollectNodeAction));
            } else {
                throw new UnsupportedOperationException("LocalCollectTask is not implemented yet");
            }

            return null;
        }

        @Override
        public Void visitMergeNode(MergeNode node, Job context) {
            node.contextId(context.id());
            if (node.executionNodes().isEmpty()) {
                context.addTask(new LocalMergeTask(
                        threadPool,
                        new ImplementationSymbolVisitor(referenceResolver, functions, RowGranularity.CLUSTER),
                        node));
            } else {
                context.addTask(new DistributedMergeTask(transportMergeNodeAction, node));
            }

            return null;
        }

        @Override
        public Void visitESSearchNode(ESSearchNode node, Job context) {
            context.addTask(new ESSearchTask(node, transportSearchAction, functions, referenceResolver));
            return null;
        }

        @Override
        public Void visitESGetNode(ESGetNode node, Job context) {
            context.addTask(new ESGetTask(transportGetAction, node));
            return null;
        }

        @Override
        protected Void visitPlanNode(PlanNode node, Job context) {
            throw new UnsupportedOperationException(
                    String.format("Can't generate job/task for planNode %s", node));
        }
    }
}
