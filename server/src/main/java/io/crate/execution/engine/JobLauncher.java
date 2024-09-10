/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.execution.engine;

import static io.crate.data.SentinelRow.SENTINEL;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.search.profile.query.QueryProfiler;

import io.crate.common.concurrent.CompletableFutures;
import io.crate.data.CollectingRowConsumer;
import io.crate.data.InMemoryBatchIterator;
import io.crate.data.Row1;
import io.crate.data.RowConsumer;
import io.crate.execution.dsl.phases.ExecutionPhase;
import io.crate.execution.dsl.phases.ExecutionPhases;
import io.crate.execution.dsl.phases.NodeOperation;
import io.crate.execution.dsl.phases.NodeOperationGrouper;
import io.crate.execution.dsl.phases.NodeOperationTree;
import io.crate.execution.engine.distribution.StreamBucket;
import io.crate.execution.jobs.DownstreamRXTask;
import io.crate.execution.jobs.InstrumentedIndexSearcher;
import io.crate.execution.jobs.JobSetup;
import io.crate.execution.jobs.PageBucketReceiver;
import io.crate.execution.jobs.RootTask;
import io.crate.execution.jobs.SharedShardContexts;
import io.crate.execution.jobs.Task;
import io.crate.execution.jobs.TasksService;
import io.crate.execution.jobs.kill.KillJobsNodeRequest;
import io.crate.execution.jobs.kill.KillResponse;
import io.crate.execution.jobs.transport.JobRequest;
import io.crate.execution.jobs.transport.JobResponse;
import io.crate.execution.support.ActionExecutor;
import io.crate.execution.support.NodeRequest;
import io.crate.metadata.TransactionContext;
import io.crate.profile.ProfilingContext;


/**
 * Creates and starts local and remote execution jobs using the provided
 * NodeOperationTrees
 * <p>
 * <pre>
 * Direct Result:
 *
 *       N1   N2    N3  // <-- job created via jobRequests using TransportJobAction
 *        ^    ^    ^
 *        |    |    |
 *        +----+----+
 *             |
 *             |        // result is received via DirectResponseFutures
 *             v
 *            N1        // <-- job created via JobSetup.prepareOnHandler
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
 *            N1<----------------+  and passed into a DistResultRXTask
 *             |
 *          BatchConsumer
 * </pre>
 **/
public class JobLauncher {

    private final ActionExecutor<NodeRequest<JobRequest>, JobResponse> transportJobAction;
    private final ActionExecutor<KillJobsNodeRequest, KillResponse> killNodeAction;
    private final List<NodeOperationTree> nodeOperationTrees;
    private final UUID jobId;
    private final ClusterService clusterService;
    private final JobSetup jobSetup;
    private final TasksService tasksService;
    private final IndicesService indicesService;
    private final boolean enableProfiling;
    private final Executor executor;

    private boolean hasDirectResponse;

    JobLauncher(UUID jobId,
                ClusterService clusterService,
                JobSetup jobSetup,
                TasksService tasksService,
                IndicesService indicesService,
                ActionExecutor<NodeRequest<JobRequest>, JobResponse> transportJobAction,
                ActionExecutor<KillJobsNodeRequest, KillResponse> killNodeAction,
                List<NodeOperationTree> nodeOperationTrees,
                boolean enableProfiling,
                Executor executor) {
        this.jobId = jobId;
        this.clusterService = clusterService;
        this.jobSetup = jobSetup;
        this.tasksService = tasksService;
        this.indicesService = indicesService;
        this.transportJobAction = transportJobAction;
        this.killNodeAction = killNodeAction;
        this.nodeOperationTrees = nodeOperationTrees;
        this.enableProfiling = enableProfiling;
        this.executor = executor;

        for (NodeOperationTree nodeOperationTree : nodeOperationTrees) {
            for (NodeOperation nodeOperation : nodeOperationTree.nodeOperations()) {
                if (ExecutionPhases.hasDirectResponseDownstream(nodeOperation.downstreamNodes())) {
                    hasDirectResponse = true;
                    break;
                }
            }
        }
    }

    public record HandlerPhase(ExecutionPhase phase, RowConsumer consumer) {}

    public void execute(RowConsumer consumer,
                        TransactionContext txnCtx,
                        boolean waitForCompletion) {
        if (waitForCompletion) {
            execute(consumer, txnCtx);
        } else {
            execute(new CollectingRowConsumer<>(Collectors.counting()), txnCtx);
            consumer.accept(InMemoryBatchIterator.of(Row1.ROW_COUNT_UNKNOWN, SENTINEL), null);
        }
    }

    public void execute(RowConsumer consumer, TransactionContext txnCtx) {
        assert nodeOperationTrees.size() == 1 : "must only have 1 NodeOperationTree for non-bulk operations";
        NodeOperationTree nodeOperationTree = nodeOperationTrees.get(0);
        Map<String, Collection<NodeOperation>> operationByServer = NodeOperationGrouper.groupByServer(nodeOperationTree.nodeOperations());

        List<ExecutionPhase> handlerPhases = Collections.singletonList(nodeOperationTree.leaf());
        List<RowConsumer> handlerConsumers = Collections.singletonList(consumer);
        try {
            setupTasks(txnCtx, operationByServer, handlerPhases, handlerConsumers);
        } catch (Throwable throwable) {
            consumer.accept(null, throwable);
        }
    }

    public List<CompletableFuture<Long>> executeBulk(TransactionContext txnCtx) {
        Iterable<NodeOperation> nodeOperations = nodeOperationTrees.stream()
            .flatMap(opTree -> opTree.nodeOperations().stream())
            ::iterator;
        Map<String, Collection<NodeOperation>> operationByServer = NodeOperationGrouper.groupByServer(nodeOperations);

        List<ExecutionPhase> handlerPhases = new ArrayList<>(nodeOperationTrees.size());
        List<RowConsumer> handlerConsumers = new ArrayList<>(nodeOperationTrees.size());
        List<CompletableFuture<Long>> results = new ArrayList<>(nodeOperationTrees.size());
        for (NodeOperationTree nodeOperationTree : nodeOperationTrees) {
            CollectingRowConsumer<?, Long> consumer = new CollectingRowConsumer<>(
                Collectors.collectingAndThen(Collectors.summingLong(r -> ((long) r.get(0))), sum -> sum));
            handlerConsumers.add(consumer);
            results.add(consumer.completionFuture());
            handlerPhases.add(nodeOperationTree.leaf());
        }
        try {
            setupTasks(txnCtx, operationByServer, handlerPhases, handlerConsumers);
        } catch (Throwable throwable) {
            return Collections.singletonList(CompletableFuture.failedFuture(throwable));
        }
        return results;
    }

    private void setupTasks(TransactionContext txnCtx,
                            Map<String, Collection<NodeOperation>> operationByServer,
                            List<ExecutionPhase> handlerPhases,
                            List<RowConsumer> handlerConsumers) throws Throwable {
        assert handlerPhases.size() == handlerConsumers.size() : "handlerPhases size must match handlerConsumers size";

        String localNodeId = clusterService.localNode().getId();
        Collection<NodeOperation> localNodeOperations = operationByServer.remove(localNodeId);
        if (localNodeOperations == null) {
            localNodeOperations = Collections.emptyList();
        }
        // + 1 for localTask which is always created
        InitializationTracker initializationTracker = new InitializationTracker(operationByServer.size() + 1);

        List<HandlerPhase> handlerPhaseAndReceiver = createHandlerPhaseAndReceivers(
            handlerPhases, handlerConsumers, initializationTracker);

        RootTask.Builder builder = tasksService.newBuilder(
            jobId,
            txnCtx.sessionSettings().userName(),
            localNodeId,
            operationByServer.keySet()
        );
        SharedShardContexts sharedShardContexts = maybeInstrumentProfiler(builder);
        List<CompletableFuture<StreamBucket>> directResponseFutures = jobSetup.prepareOnHandler(
            txnCtx.sessionSettings(),
            localNodeOperations,
            builder,
            handlerPhaseAndReceiver,
            sharedShardContexts);
        RootTask localTask = tasksService.createTask(builder);

        List<PageBucketReceiver> pageBucketReceivers = getHandlerBucketReceivers(localTask, handlerPhaseAndReceiver);
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

            final int currentBucket = bucketIdx;
            // initializationTracker for localNodeOperations is triggered via SetBucketCallback
            localTask.start().whenComplete((result, err) -> {
                if (err == null) {
                    sendJobRequests(
                        txnCtx,
                        localNodeId,
                        operationByServer,
                        pageBucketReceivers,
                        handlerPhaseAndReceiver,
                        currentBucket,
                        initializationTracker
                    );
                } else {
                    accountFailureForRemoteOperations(operationByServer, initializationTracker, handlerPhaseAndReceiver, err);
                }
            });
        } else {
            localTask.start().whenComplete((result, err) -> {
                if (err == null) {
                    initializationTracker.jobInitialized();
                    sendJobRequests(
                        txnCtx,
                        localNodeId,
                        operationByServer,
                        pageBucketReceivers,
                        handlerPhaseAndReceiver,
                        0,
                        initializationTracker
                    );
                } else {
                    initializationTracker.jobInitializationFailed(err);
                    accountFailureForRemoteOperations(operationByServer, initializationTracker, handlerPhaseAndReceiver, err);
                }
            });
        }
    }

    private SharedShardContexts maybeInstrumentProfiler(RootTask.Builder builder) {
        if (enableProfiling) {
            var profilers = new HashMap<ShardId, QueryProfiler>();
            ProfilingContext profilingContext = new ProfilingContext(profilers);
            builder.profilingContext(profilingContext);
            return new SharedShardContexts(
                indicesService,
                (shardId, indexSearcher) -> {
                    var queryProfiler = new QueryProfiler();
                    profilers.put(shardId, queryProfiler);
                    return new InstrumentedIndexSearcher(indexSearcher, queryProfiler);
                }
            );
        } else {
            return new SharedShardContexts(indicesService, (ignored, indexSearcher) -> indexSearcher);
        }
    }

    private void accountFailureForRemoteOperations(Map<String, Collection<NodeOperation>> operationByServer,
                                                   InitializationTracker initializationTracker,
                                                   List<HandlerPhase> handlerPhaseAndReceiver,
                                                   Throwable t) {
        for (HandlerPhase handlerPhase : handlerPhaseAndReceiver) {
            handlerPhase.consumer().accept(null, t);
        }
        for (int i = 0; i < operationByServer.size() + 1; i++) {
            initializationTracker.jobInitializationFailed(t);
        }
    }

    private List<HandlerPhase> createHandlerPhaseAndReceivers(List<ExecutionPhase> handlerPhases,
                                                              List<RowConsumer> handlerReceivers,
                                                              InitializationTracker initializationTracker) {
        List<HandlerPhase> handlerPhaseAndReceiver = new ArrayList<>(handlerPhases.size());
        ListIterator<RowConsumer> consumerIt = handlerReceivers.listIterator();

        for (ExecutionPhase handlerPhase : handlerPhases) {
            InterceptingRowConsumer interceptingBatchConsumer = new InterceptingRowConsumer(
                jobId,
                consumerIt.next(),
                initializationTracker,
                executor,
                killNodeAction
            );
            handlerPhaseAndReceiver.add(new HandlerPhase(handlerPhase, interceptingBatchConsumer));
        }
        return handlerPhaseAndReceiver;
    }

    private void sendJobRequests(TransactionContext txnCtx,
                                 String localNodeId,
                                 Map<String, Collection<NodeOperation>> operationByServer,
                                 List<PageBucketReceiver> pageBucketReceivers,
                                 List<HandlerPhase> handlerPhases,
                                 int bucketIdx,
                                 InitializationTracker initializationTracker) {
        for (Map.Entry<String, Collection<NodeOperation>> entry : operationByServer.entrySet()) {
            String serverNodeId = entry.getKey();
            var request = JobRequest.of(
                serverNodeId,
                jobId,
                txnCtx.sessionSettings(),
                localNodeId,
                entry.getValue(),
                enableProfiling);
            if (hasDirectResponse) {
                transportJobAction
                    .execute(request)
                    .whenComplete(
                        BucketForwarder.asActionListener(pageBucketReceivers, bucketIdx, initializationTracker)
                    );
            } else {
                transportJobAction
                    .execute(request)
                    .whenComplete(
                        new FailureOnlyResponseListener(handlerPhases, initializationTracker)
                    );
            }
            bucketIdx++;
        }
    }

    private List<PageBucketReceiver> getHandlerBucketReceivers(RootTask rootTask,
                                                               List<HandlerPhase> handlerPhases) {
        final List<PageBucketReceiver> pageBucketReceivers = new ArrayList<>(handlerPhases.size());
        for (var handlerPhase : handlerPhases) {
            Task ctx = rootTask.getTaskOrNull(handlerPhase.phase().phaseId());
            if (ctx instanceof DownstreamRXTask) {
                PageBucketReceiver pageBucketReceiver = ((DownstreamRXTask) ctx).getBucketReceiver((byte) 0);
                pageBucketReceivers.add(pageBucketReceiver);
            }
        }
        return pageBucketReceivers;
    }
}
