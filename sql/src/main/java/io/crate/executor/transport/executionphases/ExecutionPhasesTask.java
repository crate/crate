/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.executor.transport.executionphases;

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.action.job.ContextPreparer;
import io.crate.action.job.JobRequest;
import io.crate.action.job.SharedShardContexts;
import io.crate.action.job.TransportJobAction;
import io.crate.core.collections.Bucket;
import io.crate.executor.JobTask;
import io.crate.executor.TaskResult;
import io.crate.executor.transport.kill.TransportKillJobsNodeAction;
import io.crate.jobs.*;
import io.crate.operation.NodeOperation;
import io.crate.operation.NodeOperationTree;
import io.crate.operation.RowCountResultRowDownstream;
import io.crate.operation.projectors.RowReceiver;
import io.crate.planner.node.ExecutionPhase;
import io.crate.planner.node.ExecutionPhases;
import io.crate.planner.node.NodeOperationGrouper;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.indices.IndicesService;

import javax.annotation.Nullable;
import java.util.*;


public class ExecutionPhasesTask extends JobTask {

    static final ESLogger LOGGER = Loggers.getLogger(ExecutionPhasesTask.class);

    private final TransportJobAction transportJobAction;
    private final TransportKillJobsNodeAction transportKillJobsNodeAction;
    private final List<NodeOperationTree> nodeOperationTrees;
    private final ClusterService clusterService;
    private ContextPreparer contextPreparer;
    private final JobContextService jobContextService;
    private final IndicesService indicesService;

    private boolean hasDirectResponse;

    public ExecutionPhasesTask(UUID jobId,
                               ClusterService clusterService,
                               ContextPreparer contextPreparer,
                               JobContextService jobContextService,
                               IndicesService indicesService,
                               TransportJobAction transportJobAction,
                               TransportKillJobsNodeAction transportKillJobsNodeAction,
                               List<NodeOperationTree> nodeOperationTrees) {
        super(jobId);
        this.clusterService = clusterService;
        this.contextPreparer = contextPreparer;
        this.jobContextService = jobContextService;
        this.indicesService = indicesService;
        this.transportJobAction = transportJobAction;
        this.transportKillJobsNodeAction = transportKillJobsNodeAction;
        this.nodeOperationTrees = nodeOperationTrees;

        for (NodeOperationTree nodeOperationTree : nodeOperationTrees) {
            for (NodeOperation nodeOperation : nodeOperationTree.nodeOperations()) {
                if (ExecutionPhases.hasDirectResponseDownstream(nodeOperation.downstreamNodes())) {
                    hasDirectResponse = true;
                    break;
                }
            }
        }
    }

    @Override
    public void execute(RowReceiver resultReceiver) {
        assert nodeOperationTrees.size() == 1 : "must only have 1 NodeOperationTree for non-bulk operations";
        NodeOperationTree nodeOperationTree = nodeOperationTrees.get(0);
        Map<String, Collection<NodeOperation>> operationByServer = NodeOperationGrouper.groupByServer(nodeOperationTree.nodeOperations());
        InitializationTracker initializationTracker = new InitializationTracker(operationByServer.size());

        RowReceiver receiver = new InterceptingRowReceiver(
            jobId(),
            resultReceiver,
            initializationTracker,
            transportKillJobsNodeAction);
        Tuple<ExecutionPhase, RowReceiver> handlerPhase = new Tuple<>(nodeOperationTree.leaf(), receiver);
        try {
            setupContext(operationByServer, Collections.singletonList(handlerPhase), initializationTracker);
        } catch (Throwable throwable) {
            resultReceiver.fail(throwable);
        }
    }

    @Override
    public List<? extends ListenableFuture<TaskResult>> executeBulk() {
        FluentIterable<NodeOperation> nodeOperations = FluentIterable.from(nodeOperationTrees)
            .transformAndConcat(new Function<NodeOperationTree, Iterable<? extends NodeOperation>>() {
                @Nullable
                @Override
                public Iterable<? extends NodeOperation> apply(NodeOperationTree input) {
                    return input.nodeOperations();
                }
            });
        Map<String, Collection<NodeOperation>> operationByServer = NodeOperationGrouper.groupByServer(nodeOperations);
        InitializationTracker initializationTracker = new InitializationTracker(operationByServer.size());

        List<Tuple<ExecutionPhase, RowReceiver>> handlerPhases = new ArrayList<>(nodeOperationTrees.size());
        List<SettableFuture<TaskResult>> results = new ArrayList<>(nodeOperationTrees.size());
        for (NodeOperationTree nodeOperationTree : nodeOperationTrees) {
            SettableFuture<TaskResult> result = SettableFuture.create();
            results.add(result);
            RowReceiver receiver = new InterceptingRowReceiver(
                jobId(),
                new RowCountResultRowDownstream(result),
                initializationTracker,
                transportKillJobsNodeAction);
            handlerPhases.add(new Tuple<>(nodeOperationTree.leaf(), receiver));
        }

        try {
            setupContext(operationByServer, handlerPhases, initializationTracker);
        } catch (Throwable throwable) {
            for (SettableFuture<TaskResult> result : results) {
                result.setException(throwable);
            }
        }
        return results;
    }

    private void setupContext(Map<String, Collection<NodeOperation>> operationByServer,
                              List<Tuple<ExecutionPhase, RowReceiver>> handlerPhases,
                              InitializationTracker initializationTracker) throws Throwable {

        String localNodeId = clusterService.localNode().id();
        Collection<NodeOperation> localNodeOperations = operationByServer.remove(localNodeId);
        if (localNodeOperations == null) {
            localNodeOperations = Collections.emptyList();
        }

        JobExecutionContext.Builder builder = jobContextService.newBuilder(jobId(), localNodeId);
        List<ListenableFuture<Bucket>> directResponseFutures =
            contextPreparer.prepareOnHandler(localNodeOperations, builder, handlerPhases, new SharedShardContexts(indicesService));
        JobExecutionContext localJobContext = jobContextService.createContext(builder);

        List<PageBucketReceiver> pageBucketReceivers = getHandlerBucketReceivers(localJobContext, handlerPhases);
        int bucketIdx = 0;

        if (!localNodeOperations.isEmpty()) {
            if (directResponseFutures.isEmpty()) {
                initializationTracker.jobInitialized(null);
            } else {
                Futures.addCallback(Futures.allAsList(directResponseFutures),
                    new SetBucketAction(pageBucketReceivers, bucketIdx, initializationTracker));
                bucketIdx++;
            }
        }
        localJobContext.start();
        sendJobRequests(localNodeId, operationByServer, pageBucketReceivers, handlerPhases, bucketIdx, initializationTracker);
    }


    private void sendJobRequests(String localNodeId,
                                 Map<String, Collection<NodeOperation>> operationByServer,
                                 List<PageBucketReceiver> pageBucketReceivers,
                                 List<Tuple<ExecutionPhase, RowReceiver>> handlerPhases,
                                 int bucketIdx,
                                 InitializationTracker initializationTracker) {
        for (Map.Entry<String, Collection<NodeOperation>> entry : operationByServer.entrySet()) {
            String serverNodeId = entry.getKey();
            JobRequest request = new JobRequest(jobId(), localNodeId, entry.getValue());
            if (hasDirectResponse) {
                transportJobAction.execute(serverNodeId, request,
                    new SetBucketAction(pageBucketReceivers, bucketIdx, initializationTracker));
            } else {
                transportJobAction.execute(serverNodeId, request, new FailureOnlyResponseListener(handlerPhases, initializationTracker));
            }
            bucketIdx++;
        }
    }

    private List<PageBucketReceiver> getHandlerBucketReceivers(JobExecutionContext jobExecutionContext,
                                                               List<Tuple<ExecutionPhase, RowReceiver>> handlerPhases) {
        final List<PageBucketReceiver> pageBucketReceivers = new ArrayList<>(handlerPhases.size());
        for (Tuple<ExecutionPhase, RowReceiver> handlerPhase : handlerPhases) {
            ExecutionSubContext ctx = jobExecutionContext.getSubContextOrNull(handlerPhase.v1().executionPhaseId());
            if (ctx instanceof DownstreamExecutionSubContext) {
                PageBucketReceiver pageBucketReceiver = ((DownstreamExecutionSubContext) ctx).getBucketReceiver((byte) 0);
                pageBucketReceivers.add(pageBucketReceiver);
            }
        }
        return pageBucketReceivers;
    }
}
