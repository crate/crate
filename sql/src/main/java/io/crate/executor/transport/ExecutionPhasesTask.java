/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.action.job.*;
import io.crate.core.collections.Bucket;
import io.crate.exceptions.Exceptions;
import io.crate.executor.JobTask;
import io.crate.executor.TaskResult;
import io.crate.executor.transport.kill.KillJobsRequest;
import io.crate.executor.transport.kill.KillResponse;
import io.crate.executor.transport.kill.TransportKillJobsNodeAction;
import io.crate.jobs.*;
import io.crate.operation.*;
import io.crate.operation.projectors.ForwardingRowReceiver;
import io.crate.operation.projectors.RowReceiver;
import io.crate.planner.node.ExecutionPhase;
import io.crate.planner.node.ExecutionPhases;
import io.crate.planner.node.NodeOperationGrouper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.indices.IndicesService;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;


class ExecutionPhasesTask extends JobTask {

    private static final ESLogger LOGGER = Loggers.getLogger(ExecutionPhasesTask.class);

    private final TransportJobAction transportJobAction;
    private final TransportKillJobsNodeAction transportKillJobsNodeAction;
    private final List<NodeOperationTree> nodeOperationTrees;
    private final OperationType operationType;
    private final ClusterService clusterService;
    private ContextPreparer contextPreparer;
    private final JobContextService jobContextService;
    private final IndicesService indicesService;


    private final List<SettableFuture<TaskResult>> results = new ArrayList<>();
    private boolean hasDirectResponse;

    enum OperationType {
        BULK,
        /**
         * UNKNOWN means it will depend on the number of nodeOperationTrees, if > 1 it will use bulk, otherwise QueryResult
         */
        UNKNOWN
    }

    ExecutionPhasesTask(UUID jobId,
                        ClusterService clusterService,
                        ContextPreparer contextPreparer,
                        JobContextService jobContextService,
                        IndicesService indicesService,
                        TransportJobAction transportJobAction,
                        TransportKillJobsNodeAction transportKillJobsNodeAction,
                        List<NodeOperationTree> nodeOperationTrees,
                        OperationType operationType) {
        super(jobId);
        this.clusterService = clusterService;
        this.contextPreparer = contextPreparer;
        this.jobContextService = jobContextService;
        this.indicesService = indicesService;
        this.transportJobAction = transportJobAction;
        this.transportKillJobsNodeAction = transportKillJobsNodeAction;
        this.nodeOperationTrees = nodeOperationTrees;
        this.operationType = operationType;

        for (NodeOperationTree nodeOperationTree : nodeOperationTrees) {
            results.add(SettableFuture.<TaskResult>create());
            for (NodeOperation nodeOperation : nodeOperationTree.nodeOperations()) {
                if (ExecutionPhases.hasDirectResponseDownstream(nodeOperation.downstreamNodes())) {
                    hasDirectResponse = true;
                    break;
                }
            }
        }
    }

    @Override
    public void start() {
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
        List<Tuple<ExecutionPhase, RowReceiver>> handlerPhases = createHandlerPhases(initializationTracker);
        try {
            setupContext(operationByServer, handlerPhases, initializationTracker);
        } catch (Throwable throwable) {
            for (SettableFuture<TaskResult> result : results) {
                result.setException(throwable);
            }
        }
    }

    private static class InitializationTracker {

        private final SettableFuture<Void> future;
        private final AtomicInteger serverToInitialize;
        private Throwable failure;

        InitializationTracker(int numServer) {
            future = SettableFuture.create();
            if (numServer == 0) {
                future.set(null);
            }
            serverToInitialize = new AtomicInteger(numServer);
        }

        void jobInitialized(@Nullable Throwable t) {
            if (failure == null || failure instanceof InterruptedException) {
                failure = t;
            }
            if (serverToInitialize.decrementAndGet() <= 0) {
                if (failure == null) {
                    future.set(null);
                } else {
                    future.setException(failure);
                }
            }
        }
    }

    private static class InterceptingRowReceiver extends ForwardingRowReceiver implements FutureCallback<Void> {

        private final AtomicInteger upstreams = new AtomicInteger(2);
        private final UUID jobId;
        private final TransportKillJobsNodeAction transportKillJobsNodeAction;
        private final AtomicBoolean rowReceiverDone = new AtomicBoolean(false);
        private Throwable failure;

        InterceptingRowReceiver(UUID jobId,
                                RowReceiver rowReceiver,
                                InitializationTracker jobsInitialized,
                                TransportKillJobsNodeAction transportKillJobsNodeAction) {
            super(rowReceiver);
            this.jobId = jobId;
            this.transportKillJobsNodeAction = transportKillJobsNodeAction;
            Futures.addCallback(jobsInitialized.future, this);
        }

        @Override
        public void fail(Throwable throwable) {
            if (rowReceiverDone.compareAndSet(false, true)) {
                tryForwardResult(throwable);
            }
        }

        @Override
        public void kill(Throwable throwable) {
            fail(throwable);
        }

        @Override
        public void finish() {
            fail(null);
        }

        @Override
        public void onSuccess(@Nullable Void result) {
            tryForwardResult(null);
        }

        @Override
        public void onFailure(@Nonnull Throwable t) {
            tryForwardResult(t);
        }

        private void tryForwardResult(Throwable throwable) {
            if (throwable != null && (failure == null || failure instanceof InterruptedException)) {
                failure = Exceptions.unwrap(throwable);
            }
            if (upstreams.decrementAndGet() > 0) {
                return;
            }
            if (failure == null) {
                super.finish();
            } else {
                transportKillJobsNodeAction.executeKillOnAllNodes(
                    new KillJobsRequest(Collections.singletonList(jobId)), new ActionListener<KillResponse>() {
                        @Override
                        public void onResponse(KillResponse killResponse) {
                            InterceptingRowReceiver.super.fail(failure);
                        }

                        @Override
                        public void onFailure(Throwable e) {
                            InterceptingRowReceiver.super.fail(failure);
                        }
                    });
            }
        }
    }

    private List<Tuple<ExecutionPhase, RowReceiver>> createHandlerPhases(InitializationTracker initializationTracker) {
        List<Tuple<ExecutionPhase, RowReceiver>> handlerPhases = new ArrayList<>(nodeOperationTrees.size());

        if (operationType == OperationType.BULK || nodeOperationTrees.size() > 1) {
            // bulk Operation with rowCountResult
            for (int i = 0; i < nodeOperationTrees.size(); i++) {
                SettableFuture<TaskResult> result = results.get(i);
                RowReceiver receiver = new InterceptingRowReceiver(
                    jobId(),
                    new RowCountResultRowDownstream(result),
                    initializationTracker,
                    transportKillJobsNodeAction);
                handlerPhases.add(new Tuple<>(nodeOperationTrees.get(i).leaf(), receiver));
            }
        } else {
            SettableFuture<TaskResult> result = Iterables.getOnlyElement(results);
            RowReceiver receiver = new InterceptingRowReceiver(
                jobId(),
                new QueryResultRowDownstream(result),
                initializationTracker,
                transportKillJobsNodeAction);
            handlerPhases.add(new Tuple<>(Iterables.getOnlyElement(nodeOperationTrees).leaf(), receiver));
        }
        return handlerPhases;
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
        Tuple<List<ExecutionSubContext>, List<ListenableFuture<Bucket>>> onHandler =
            contextPreparer.prepareOnHandler(localNodeOperations, builder, handlerPhases, new SharedShardContexts(indicesService));
        JobExecutionContext localJobContext = jobContextService.createContext(builder);
        localJobContext.start();

        List<PageDownstreamContext> pageDownstreamContexts = getHandlerPageDownstreamContexts(onHandler);
        int bucketIdx = 0;
        List<ListenableFuture<Bucket>> directResponseFutures = onHandler.v2();

        if (!localNodeOperations.isEmpty()) {
            if (directResponseFutures.isEmpty()) {
                initializationTracker.jobInitialized(null);
            } else {
                Futures.addCallback(Futures.allAsList(directResponseFutures),
                    new SetBucketAction(pageDownstreamContexts, bucketIdx, initializationTracker));
                bucketIdx++;
            }
        }

        sendJobRequests(localNodeId, operationByServer, pageDownstreamContexts, handlerPhases, bucketIdx, initializationTracker);
    }

    private void sendJobRequests(String localNodeId,
                                 Map<String, Collection<NodeOperation>> operationByServer,
                                 List<PageDownstreamContext> pageDownstreamContexts,
                                 List<Tuple<ExecutionPhase, RowReceiver>> handlerPhases,
                                 int bucketIdx,
                                 InitializationTracker initializationTracker) {
        for (Map.Entry<String, Collection<NodeOperation>> entry : operationByServer.entrySet()) {
            String serverNodeId = entry.getKey();
            JobRequest request = new JobRequest(jobId(), localNodeId, entry.getValue());
            if (hasDirectResponse) {
                transportJobAction.execute(serverNodeId, request,
                    new SetBucketAction(pageDownstreamContexts, bucketIdx, initializationTracker));
            } else {
                transportJobAction.execute(serverNodeId, request, new FailureOnlyResponseListener(handlerPhases, initializationTracker));
            }
            bucketIdx++;
        }
    }

    private List<PageDownstreamContext> getHandlerPageDownstreamContexts(Tuple<List<ExecutionSubContext>, List<ListenableFuture<Bucket>>> onHandler) {
        final List<PageDownstreamContext> pageDownstreamContexts = new ArrayList<>(onHandler.v1().size());
        for (ExecutionSubContext handlerExecutionSubContext : onHandler.v1()) {
            if (handlerExecutionSubContext instanceof DownstreamExecutionSubContext) {
                PageDownstreamContext pageDownstreamContext = ((DownstreamExecutionSubContext) handlerExecutionSubContext).pageDownstreamContext((byte) 0);
                pageDownstreamContexts.add(pageDownstreamContext);
            }
        }
        return pageDownstreamContexts;
    }

    @Override
    public List<? extends ListenableFuture<TaskResult>> result() {
        return results;
    }

    @Override
    public void upstreamResult(List<? extends ListenableFuture<TaskResult>> result) {
        throw new UnsupportedOperationException("ExecutionNodesTask doesn't support upstreamResult");
    }

    private static class FailureOnlyResponseListener implements ActionListener<JobResponse> {

        private final List<Tuple<ExecutionPhase, RowReceiver>> rowReceivers;
        private final InitializationTracker initializationTracker;

        FailureOnlyResponseListener(List<Tuple<ExecutionPhase, RowReceiver>> rowReceivers, InitializationTracker initializationTracker) {
            this.rowReceivers = rowReceivers;
            this.initializationTracker = initializationTracker;
        }

        @Override
        public void onResponse(JobResponse jobResponse) {
            initializationTracker.jobInitialized(null);
            if (jobResponse.directResponse().size() > 0) {
                for (Tuple<ExecutionPhase, RowReceiver> rowReceiver : rowReceivers) {
                    rowReceiver.v2().fail(new IllegalStateException("Got a directResponse but didn't expect one"));
                }
            }
        }

        @Override
        public void onFailure(Throwable e) {
            initializationTracker.jobInitialized(null);
            // could be a preparation failure - in that case the regular error propagation doesn't work as it hasn't been set up yet
            // so fail rowReceivers directly
            for (Tuple<ExecutionPhase, RowReceiver> rowReceiver : rowReceivers) {
                rowReceiver.v2().fail(e);
            }
        }
    }

    private static class SetBucketAction implements FutureCallback<List<Bucket>>, ActionListener<JobResponse> {
        private final List<PageDownstreamContext> pageDownstreamContexts;
        private final int bucketIdx;
        private final InitializationTracker initializationTracker;
        private final BucketResultListener bucketResultListener;

        SetBucketAction(List<PageDownstreamContext> pageDownstreamContexts, int bucketIdx, InitializationTracker initializationTracker) {
            this.pageDownstreamContexts = pageDownstreamContexts;
            this.bucketIdx = bucketIdx;
            this.initializationTracker = initializationTracker;
            bucketResultListener = new BucketResultListener(bucketIdx);
        }

        @Override
        public void onSuccess(@Nullable List<Bucket> result) {
            initializationTracker.jobInitialized(null);
            if (result == null) {
                onFailure(new NullPointerException("result is null"));
                return;
            }

            for (int i = 0; i < pageDownstreamContexts.size(); i++) {
                PageDownstreamContext pageDownstreamContext = pageDownstreamContexts.get(i);
                setBucket(pageDownstreamContext, result.get(i));
            }
        }

        @Override
        public void onResponse(JobResponse jobResponse) {
            initializationTracker.jobInitialized(null);
            for (int i = 0; i < pageDownstreamContexts.size(); i++) {
                PageDownstreamContext pageDownstreamContext = pageDownstreamContexts.get(i);
                jobResponse.streamers(pageDownstreamContext.streamer());
                setBucket(pageDownstreamContext, jobResponse.directResponse().get(i));
            }
        }

        @Override
        public void onFailure(@Nonnull Throwable t) {
            initializationTracker.jobInitialized(t);
            for (PageDownstreamContext pageDownstreamContext : pageDownstreamContexts) {
                pageDownstreamContext.failure(bucketIdx, t);
            }
        }

        private void setBucket(PageDownstreamContext pageDownstreamContext, Bucket bucket) {
            if (bucket == null) {
                pageDownstreamContext.failure(bucketIdx, new IllegalStateException("expected directResponse but didn't get one"));
                return;
            }
            pageDownstreamContext.setBucket(bucketIdx, bucket, true, bucketResultListener);
        }
    }

    private static class BucketResultListener implements PageResultListener {

        private final int bucketIdx;

        BucketResultListener(int bucketIdx) {
            this.bucketIdx = bucketIdx;
        }

        @Override
        public void needMore(boolean needMore) {
            if (needMore) {
                LOGGER.warn("requested more data but directResponse doesn't support paging");
            }
        }

        @Override
        public int buckedIdx() {
            return bucketIdx;
        }
    }
}
