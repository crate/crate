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
import io.crate.core.collections.Row;
import io.crate.executor.JobTask;
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
    public void execute(RowReceiver rowReceiver, Row parameters) {
        assert nodeOperationTrees.size() == 1 : "must only have 1 NodeOperationTree for non-bulk operations";
        NodeOperationTree nodeOperationTree = nodeOperationTrees.get(0);
        Map<String, Collection<NodeOperation>> operationByServer = NodeOperationGrouper.groupByServer(nodeOperationTree.nodeOperations());

        List<ExecutionPhase> handlerPhases = Collections.singletonList(nodeOperationTree.leaf());
        List<RowReceiver> handlerReceivers = Collections.singletonList(rowReceiver);
        try {
            setupContext(operationByServer, handlerPhases, handlerReceivers);
        } catch (Throwable throwable) {
            rowReceiver.fail(throwable);
        }
    }

    @Override
    public ListenableFuture<List<Long>> executeBulk() {
        FluentIterable<NodeOperation> nodeOperations = FluentIterable.from(nodeOperationTrees)
            .transformAndConcat(new Function<NodeOperationTree, Iterable<? extends NodeOperation>>() {
                @Nullable
                @Override
                public Iterable<? extends NodeOperation> apply(NodeOperationTree input) {
                    return input.nodeOperations();
                }
            });
        Map<String, Collection<NodeOperation>> operationByServer = NodeOperationGrouper.groupByServer(nodeOperations);

        List<ExecutionPhase> handlerPhases = new ArrayList<>(nodeOperationTrees.size());
        List<RowReceiver> handlerReceivers = new ArrayList<>(nodeOperationTrees.size());
        List<SettableFuture<Long>> results = new ArrayList<>(nodeOperationTrees.size());
        for (NodeOperationTree nodeOperationTree : nodeOperationTrees) {
            SettableFuture<Long> result = SettableFuture.create();
            results.add(result);
            handlerPhases.add(nodeOperationTree.leaf());
            handlerReceivers.add(new RowCountResultRowDownstream(result));
        }
        try {
            setupContext(operationByServer, handlerPhases, handlerReceivers);
        } catch (Throwable throwable) {
            return Futures.immediateFailedFuture(throwable);
        }
        return Futures.successfulAsList(results);
    }

    private void setupContext(Map<String, Collection<NodeOperation>> operationByServer,
                              List<ExecutionPhase> handlerPhases,
                              List<RowReceiver> handlerReceivers) throws Throwable {
        assert handlerPhases.size() == handlerReceivers.size() : "handlerPhases size must match handlerReceivers size";

        String localNodeId = clusterService.localNode().id();
        Collection<NodeOperation> localNodeOperations = operationByServer.remove(localNodeId);
        if (localNodeOperations == null) {
            localNodeOperations = Collections.emptyList();
        }
        // + 1 for localJobContext which is always created
        InitializationTracker initializationTracker = new InitializationTracker(operationByServer.size() + 1);

        List<Tuple<ExecutionPhase, RowReceiver>> handlerPhaseAndReceiver = createHandlerPhaseAndReceivers(
            handlerPhases, handlerReceivers, initializationTracker);

        JobExecutionContext.Builder builder = jobContextService.newBuilder(jobId(), localNodeId);
        List<ListenableFuture<Bucket>> directResponseFutures = contextPreparer.prepareOnHandler(
            localNodeOperations, builder, handlerPhaseAndReceiver, new SharedShardContexts(indicesService));
        JobExecutionContext localJobContext = jobContextService.createContext(builder);
        initializationTracker.jobInitialized();

        List<PageBucketReceiver> pageBucketReceivers = getHandlerBucketReceivers(localJobContext, handlerPhaseAndReceiver);
        int bucketIdx = 0;

        if (!localNodeOperations.isEmpty()) {
            if (!directResponseFutures.isEmpty()) {
                Futures.addCallback(Futures.allAsList(directResponseFutures),
                    new SetBucketCallback(pageBucketReceivers, bucketIdx, initializationTracker));
                bucketIdx++;
            }
        }
        try {
            localJobContext.start();
        } catch (Throwable t) {
            for (Tuple<ExecutionPhase, RowReceiver> executionPhaseRowReceiverTuple : handlerPhaseAndReceiver) {
                executionPhaseRowReceiverTuple.v2().fail(t);
            }
            for (int i = 0; i < operationByServer.size(); i++) {
                // not going to initialize remote jobs, but need to account for them
                initializationTracker.jobInitializationFailed(t);
            }
            return;
        }
        sendJobRequests(
            localNodeId, operationByServer, pageBucketReceivers, handlerPhaseAndReceiver, bucketIdx, initializationTracker);
    }

    private List<Tuple<ExecutionPhase, RowReceiver>> createHandlerPhaseAndReceivers(List<ExecutionPhase> handlerPhases,
                                                                                    List<RowReceiver> handlerReceivers,
                                                                                    InitializationTracker initializationTracker) {
        List<Tuple<ExecutionPhase, RowReceiver>> handlerPhaseAndReceiver = new ArrayList<>();
        ListIterator<RowReceiver> receiverIt = handlerReceivers.listIterator();

        for (ExecutionPhase handlerPhase : handlerPhases) {
            RowReceiver interceptingRowReceiver =
                new InterceptingRowReceiver(jobId(), receiverIt.next(), initializationTracker, transportKillJobsNodeAction);
            handlerPhaseAndReceiver.add(new Tuple<>(handlerPhase, interceptingRowReceiver));
        }
        return handlerPhaseAndReceiver;
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
                    new SetBucketActionListener(pageBucketReceivers, bucketIdx, initializationTracker));
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
