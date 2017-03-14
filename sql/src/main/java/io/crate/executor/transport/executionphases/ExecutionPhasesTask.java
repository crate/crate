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
import io.crate.action.job.ContextPreparer;
import io.crate.action.job.JobRequest;
import io.crate.action.job.SharedShardContexts;
import io.crate.action.job.TransportJobAction;
import io.crate.concurrent.CompletableFutures;
import io.crate.data.BatchConsumer;
import io.crate.data.Bucket;
import io.crate.data.Row;
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
import java.util.concurrent.CompletableFuture;


/**
 * Creates and starts local and remote execution jobs using the provided
 * NodeOperationTrees
 * <p>
 * <pre>
 * Direct Result:
 *
 *       N1   N2    N3  // <-- job context created via jobRequests using TransportJobAction
 *        ^    ^    ^
 *        |    |    |
 *        +----+----+
 *             |
 *             |        // result is received via DirectResponseFutures
 *             v
 *            N1        // <-- job context created via ContextPreparer.prepareOnHandler
 *             |
 *          RowReceiver
 *
 *
 * Push Result:
 *                  // result is sent via DistributingDownstream
 *       +------------------->---+
 *       ^     ^     ^           |
 *       |     |     |           |
 *       N1   N2    N3           |
 *        ^    ^    ^            |
 *        |    |    |            |
 *        +----+----+            |
 *             |                 |  result is received via
 *             |                 v  TransportDistributedResultAction
 *            N1<----------------+  and passed into a PageDownstreamContext
 *             |
 *          RowReceiver
 * </pre>
 **/
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
    public void execute(BatchConsumer consumer, Row parameters) {
        throw new UnsupportedOperationException("NYI");
    }

    @Override
    public List<CompletableFuture<Long>> executeBulk() {
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
        List<CompletableFuture<Long>> results = new ArrayList<>(nodeOperationTrees.size());
        for (NodeOperationTree nodeOperationTree : nodeOperationTrees) {
            CompletableFuture<Long> result = new CompletableFuture<>();
            results.add(result);
            handlerPhases.add(nodeOperationTree.leaf());
            handlerReceivers.add(new RowCountResultRowDownstream(result));
        }
        try {
            setupContext(operationByServer, handlerPhases, handlerReceivers);
        } catch (Throwable throwable) {
            return Collections.singletonList(CompletableFutures.failedFuture(throwable));
        }
        return results;
    }

    private void setupContext(Map<String, Collection<NodeOperation>> operationByServer,
                              List<ExecutionPhase> handlerPhases,
                              List<RowReceiver> handlerReceivers) throws Throwable {
        assert handlerPhases.size() == handlerReceivers.size() : "handlerPhases size must match handlerReceivers size";

        String localNodeId = clusterService.localNode().getId();
        Collection<NodeOperation> localNodeOperations = operationByServer.remove(localNodeId);
        if (localNodeOperations == null) {
            localNodeOperations = Collections.emptyList();
        }
        // + 1 for localJobContext which is always created
        InitializationTracker initializationTracker = new InitializationTracker(operationByServer.size() + 1);

        List<Tuple<ExecutionPhase, RowReceiver>> handlerPhaseAndReceiver = createHandlerPhaseAndReceivers(
            handlerPhases, handlerReceivers, initializationTracker);

        JobExecutionContext.Builder builder = jobContextService.newBuilder(jobId(), localNodeId, operationByServer.keySet());
        List<CompletableFuture<Bucket>> directResponseFutures = contextPreparer.prepareOnHandler(
            localNodeOperations, builder, handlerPhaseAndReceiver, new SharedShardContexts(indicesService));
        JobExecutionContext localJobContext = jobContextService.createContext(builder);

        List<PageBucketReceiver> pageBucketReceivers = getHandlerBucketReceivers(localJobContext, handlerPhaseAndReceiver);
        int bucketIdx = 0;

        /*
         * If you touch anything here make sure the following tests pass with > 1k iterations:
         *
         * Seed: 112E1807417E925A - testInvalidPatternSyntax
         * Seed: Any              - testRegularSelectWithFewAvailableThreadsShouldNeverGetStuck
         * Seed: CC456FF5004F35D3 - testFailureOfJoinDownstream
         */
        if (!localNodeOperations.isEmpty() && !directResponseFutures.isEmpty()) {
            CompletableFutures.allAsList(directResponseFutures)
                .whenComplete(new SetBucketCallback(pageBucketReceivers, bucketIdx, initializationTracker));
            bucketIdx++;
            try {
                // initializationTracker for localNodeOperations is triggered via SetBucketCallback

                localJobContext.start();
            } catch (Throwable t) {
                accountFailureForRemoteOperations(operationByServer, initializationTracker, handlerPhaseAndReceiver, t);
                return;
            }
        } else {
            try {
                localJobContext.start();
                initializationTracker.jobInitialized();
            } catch (Throwable t) {
                initializationTracker.jobInitializationFailed(t);
                accountFailureForRemoteOperations(operationByServer, initializationTracker, handlerPhaseAndReceiver, t);
                return;
            }
        }
        sendJobRequests(
            localNodeId, operationByServer, pageBucketReceivers, handlerPhaseAndReceiver, bucketIdx, initializationTracker);
    }

    private void accountFailureForRemoteOperations(Map<String, Collection<NodeOperation>> operationByServer,
                                                   InitializationTracker initializationTracker,
                                                   List<Tuple<ExecutionPhase, RowReceiver>> handlerPhaseAndReceiver,
                                                   Throwable t) {
        for (Tuple<ExecutionPhase, RowReceiver> executionPhaseRowReceiverTuple : handlerPhaseAndReceiver) {
            executionPhaseRowReceiverTuple.v2().fail(t);
        }
        for (int i = 0; i < operationByServer.size() + 1; i++) {
            initializationTracker.jobInitializationFailed(t);
        }
    }

    private List<Tuple<ExecutionPhase, RowReceiver>> createHandlerPhaseAndReceivers(List<ExecutionPhase> handlerPhases,
                                                                                    List<RowReceiver> handlerReceivers,
                                                                                    InitializationTracker initializationTracker) {
        List<Tuple<ExecutionPhase, RowReceiver>> handlerPhaseAndReceiver = new ArrayList<>();
        ListIterator<RowReceiver> receiverIt = handlerReceivers.listIterator();

        for (ExecutionPhase handlerPhase : handlerPhases) {
            InterceptingRowReceiver interceptingRowReceiver =
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
            ExecutionSubContext ctx = jobExecutionContext.getSubContextOrNull(handlerPhase.v1().phaseId());
            if (ctx instanceof DownstreamExecutionSubContext) {
                PageBucketReceiver pageBucketReceiver = ((DownstreamExecutionSubContext) ctx).getBucketReceiver((byte) 0);
                pageBucketReceivers.add(pageBucketReceiver);
            }
        }
        return pageBucketReceivers;
    }
}
