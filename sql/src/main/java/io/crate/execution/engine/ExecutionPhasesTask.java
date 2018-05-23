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

package io.crate.execution.engine;

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import io.crate.concurrent.CompletableFutures;
import io.crate.data.Bucket;
import io.crate.data.CollectingRowConsumer;
import io.crate.data.RowConsumer;
import io.crate.execution.dsl.phases.ExecutionPhase;
import io.crate.execution.dsl.phases.ExecutionPhases;
import io.crate.execution.dsl.phases.NodeOperation;
import io.crate.execution.dsl.phases.NodeOperationGrouper;
import io.crate.execution.dsl.phases.NodeOperationTree;
import io.crate.execution.jobs.ContextPreparer;
import io.crate.execution.jobs.DownstreamExecutionSubContext;
import io.crate.execution.jobs.ExecutionSubContext;
import io.crate.execution.jobs.JobContextService;
import io.crate.execution.jobs.JobExecutionContext;
import io.crate.execution.jobs.PageBucketReceiver;
import io.crate.execution.jobs.SharedShardContexts;
import io.crate.execution.jobs.kill.TransportKillJobsNodeAction;
import io.crate.execution.jobs.transport.JobRequest;
import io.crate.execution.jobs.transport.TransportJobAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.indices.IndicesService;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;


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
 *          BatchConsumer
 *
 *
 * Push Result:
 *                  // result is sent via DistributingConsumer
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
 *          BatchConsumer
 * </pre>
 **/
public class ExecutionPhasesTask {

    private final TransportJobAction transportJobAction;
    private final TransportKillJobsNodeAction transportKillJobsNodeAction;
    private final List<NodeOperationTree> nodeOperationTrees;
    private final UUID jobId;
    private final ClusterService clusterService;
    private ContextPreparer contextPreparer;
    private final JobContextService jobContextService;
    private final IndicesService indicesService;
    private final boolean enableProfiling;

    private boolean hasDirectResponse;

    ExecutionPhasesTask(UUID jobId,
                        ClusterService clusterService,
                        ContextPreparer contextPreparer,
                        JobContextService jobContextService,
                        IndicesService indicesService,
                        TransportJobAction transportJobAction,
                        TransportKillJobsNodeAction transportKillJobsNodeAction,
                        List<NodeOperationTree> nodeOperationTrees,
                        boolean enableProfiling) {
        this.jobId = jobId;
        this.clusterService = clusterService;
        this.contextPreparer = contextPreparer;
        this.jobContextService = jobContextService;
        this.indicesService = indicesService;
        this.transportJobAction = transportJobAction;
        this.transportKillJobsNodeAction = transportKillJobsNodeAction;
        this.nodeOperationTrees = nodeOperationTrees;
        this.enableProfiling = enableProfiling;

        for (NodeOperationTree nodeOperationTree : nodeOperationTrees) {
            for (NodeOperation nodeOperation : nodeOperationTree.nodeOperations()) {
                if (ExecutionPhases.hasDirectResponseDownstream(nodeOperation.downstreamNodes())) {
                    hasDirectResponse = true;
                    break;
                }
            }
        }
    }

    public void execute(RowConsumer consumer) {
        assert nodeOperationTrees.size() == 1 : "must only have 1 NodeOperationTree for non-bulk operations";
        NodeOperationTree nodeOperationTree = nodeOperationTrees.get(0);
        Map<String, Collection<NodeOperation>> operationByServer = NodeOperationGrouper.groupByServer(nodeOperationTree.nodeOperations());

        List<ExecutionPhase> handlerPhases = Collections.singletonList(nodeOperationTree.leaf());
        List<RowConsumer> handlerConsumers = Collections.singletonList(consumer);
        try {
            setupContext(operationByServer, handlerPhases, handlerConsumers);
        } catch (Throwable throwable) {
            consumer.accept(null, throwable);
        }
    }

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
        List<RowConsumer> handlerConsumers = new ArrayList<>(nodeOperationTrees.size());
        List<CompletableFuture<Long>> results = new ArrayList<>(nodeOperationTrees.size());
        for (NodeOperationTree nodeOperationTree : nodeOperationTrees) {
            CollectingRowConsumer<?, Long> consumer = new CollectingRowConsumer<>(
                Collectors.collectingAndThen(Collectors.summingLong(r -> ((long) r.get(0))), sum -> sum));
            handlerConsumers.add(consumer);
            results.add(consumer.resultFuture());
            handlerPhases.add(nodeOperationTree.leaf());
        }
        try {
            setupContext(operationByServer, handlerPhases, handlerConsumers);
        } catch (Throwable throwable) {
            return Collections.singletonList(CompletableFutures.failedFuture(throwable));
        }
        return results;
    }

    private void setupContext(Map<String, Collection<NodeOperation>> operationByServer,
                              List<ExecutionPhase> handlerPhases,
                              List<RowConsumer> handlerConsumers) throws Throwable {
        assert handlerPhases.size() == handlerConsumers.size() : "handlerPhases size must match handlerConsumers size";

        String localNodeId = clusterService.localNode().getId();
        Collection<NodeOperation> localNodeOperations = operationByServer.remove(localNodeId);
        if (localNodeOperations == null) {
            localNodeOperations = Collections.emptyList();
        }
        // + 1 for localJobContext which is always created
        InitializationTracker initializationTracker = new InitializationTracker(operationByServer.size() + 1);

        List<Tuple<ExecutionPhase, RowConsumer>> handlerPhaseAndReceiver = createHandlerPhaseAndReceivers(
            handlerPhases, handlerConsumers, initializationTracker);

        JobExecutionContext.Builder builder = jobContextService.newBuilder(jobId, localNodeId, operationByServer.keySet())
            .enableProfiling(enableProfiling);
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
            assert directResponseFutures.size() == pageBucketReceivers.size() : "directResponses size must match pageBucketReceivers";
            CompletableFutures.allAsList(directResponseFutures)
                .whenComplete(BucketForwarder.asConsumer(pageBucketReceivers, bucketIdx, initializationTracker));
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
                                                   List<Tuple<ExecutionPhase, RowConsumer>> handlerPhaseAndReceiver,
                                                   Throwable t) {
        for (Tuple<ExecutionPhase, RowConsumer> executionPhaseRowReceiverTuple : handlerPhaseAndReceiver) {
            executionPhaseRowReceiverTuple.v2().accept(null, t);
        }
        for (int i = 0; i < operationByServer.size() + 1; i++) {
            initializationTracker.jobInitializationFailed(t);
        }
    }

    private List<Tuple<ExecutionPhase, RowConsumer>> createHandlerPhaseAndReceivers(List<ExecutionPhase> handlerPhases,
                                                                                         List<RowConsumer> handlerReceivers,
                                                                                         InitializationTracker initializationTracker) {
        List<Tuple<ExecutionPhase, RowConsumer>> handlerPhaseAndReceiver = new ArrayList<>();
        ListIterator<RowConsumer> consumerIt = handlerReceivers.listIterator();

        for (ExecutionPhase handlerPhase : handlerPhases) {
            InterceptingRowConsumer interceptingBatchConsumer =
                new InterceptingRowConsumer(jobId, consumerIt.next(), initializationTracker, transportKillJobsNodeAction);
            handlerPhaseAndReceiver.add(new Tuple<>(handlerPhase, interceptingBatchConsumer));
        }
        return handlerPhaseAndReceiver;
    }

    private void sendJobRequests(String localNodeId,
                                 Map<String, Collection<NodeOperation>> operationByServer,
                                 List<PageBucketReceiver> pageBucketReceivers,
                                 List<Tuple<ExecutionPhase, RowConsumer>> handlerPhases,
                                 int bucketIdx,
                                 InitializationTracker initializationTracker) {
        for (Map.Entry<String, Collection<NodeOperation>> entry : operationByServer.entrySet()) {
            String serverNodeId = entry.getKey();
            JobRequest request = new JobRequest(jobId, localNodeId, entry.getValue(), enableProfiling);
            if (hasDirectResponse) {
                transportJobAction.execute(serverNodeId, request,
                    BucketForwarder.asActionListener(pageBucketReceivers, bucketIdx, initializationTracker));
            } else {
                transportJobAction.execute(serverNodeId, request, new FailureOnlyResponseListener(handlerPhases, initializationTracker));
            }
            bucketIdx++;
        }
    }

    private List<PageBucketReceiver> getHandlerBucketReceivers(JobExecutionContext jobExecutionContext,
                                                               List<Tuple<ExecutionPhase, RowConsumer>> handlerPhases) {
        final List<PageBucketReceiver> pageBucketReceivers = new ArrayList<>(handlerPhases.size());
        for (Tuple<ExecutionPhase, ?> handlerPhase : handlerPhases) {
            ExecutionSubContext ctx = jobExecutionContext.getSubContextOrNull(handlerPhase.v1().phaseId());
            if (ctx instanceof DownstreamExecutionSubContext) {
                PageBucketReceiver pageBucketReceiver = ((DownstreamExecutionSubContext) ctx).getBucketReceiver((byte) 0);
                pageBucketReceivers.add(pageBucketReceiver);
            }
        }
        return pageBucketReceivers;
    }
}
