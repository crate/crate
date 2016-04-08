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
import io.crate.executor.JobTask;
import io.crate.executor.TaskResult;
import io.crate.jobs.*;
import io.crate.operation.*;
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


public class ExecutionPhasesTask extends JobTask {

    private static final ESLogger LOGGER = Loggers.getLogger(ExecutionPhasesTask.class);

    private final TransportJobAction transportJobAction;
    private final List<NodeOperationTree> nodeOperationTrees;
    private final OperationType operationType;
    private final ClusterService clusterService;
    private ContextPreparer contextPreparer;
    private final JobContextService jobContextService;
    private final IndicesService indicesService;


    private final List<SettableFuture<TaskResult>> results = new ArrayList<>();
    private boolean hasDirectResponse;

    public enum OperationType {
        BULK,
        /**
         * UNKNOWN means it will depend on the number of nodeOperationTrees, if > 1 it will use bulk, otherwise QueryResult
         */
        UNKNOWN
    }

    protected ExecutionPhasesTask(UUID jobId,
                                  ClusterService clusterService,
                                  ContextPreparer contextPreparer,
                                  JobContextService jobContextService,
                                  IndicesService indicesService,
                                  TransportJobAction transportJobAction,
                                  List<NodeOperationTree> nodeOperationTrees,
                                  OperationType operationType) {
        super(jobId);
        this.clusterService = clusterService;
        this.contextPreparer = contextPreparer;
        this.jobContextService = jobContextService;
        this.indicesService = indicesService;
        this.transportJobAction = transportJobAction;
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
        List<Tuple<ExecutionPhase, RowReceiver>> handlerPhases = createHandlerPhases();

        try {
            setupContext(operationByServer, handlerPhases);
        } catch (Throwable throwable) {
            for (SettableFuture<TaskResult> result : results) {
                result.setException(throwable);
            }
        }
    }

    private List<Tuple<ExecutionPhase, RowReceiver>> createHandlerPhases() {
        List<Tuple<ExecutionPhase, RowReceiver>> handlerPhases = new ArrayList<>(nodeOperationTrees.size());

        if (operationType == OperationType.BULK || nodeOperationTrees.size() > 1) {
            // bulk Operation with rowCountResult
            for (int i = 0; i < nodeOperationTrees.size(); i++) {
                SettableFuture<TaskResult> result = results.get(i);
                RowCountResultRowDownstream rowDownstream = new RowCountResultRowDownstream(result);
                handlerPhases.add(new Tuple<ExecutionPhase, RowReceiver>(nodeOperationTrees.get(i).leaf(), rowDownstream));
            }
        } else {
            SettableFuture<TaskResult> result = Iterables.getOnlyElement(results);
            QueryResultRowDownstream downstream = new QueryResultRowDownstream(result);
            handlerPhases.add(new Tuple<ExecutionPhase, RowReceiver>(Iterables.getOnlyElement(nodeOperationTrees).leaf(), downstream));
        }
        return handlerPhases;
    }

    private void setupContext(Map<String, Collection<NodeOperation>> operationByServer,
                              List<Tuple<ExecutionPhase, RowReceiver>> handlerPhases) throws Throwable {

        String localNodeId = clusterService.localNode().id();
        Collection<NodeOperation> localNodeOperations = operationByServer.remove(localNodeId);
        if (localNodeOperations == null) {
            localNodeOperations = Collections.emptyList();
        }

        JobExecutionContext.Builder builder = jobContextService.newBuilder(jobId());
        Tuple<List<ExecutionSubContext>, List<ListenableFuture<Bucket>>> onHandler =
                contextPreparer.prepareOnHandler(localNodeOperations, builder, handlerPhases, new SharedShardContexts(indicesService));
        JobExecutionContext localJobContext = jobContextService.createContext(builder);
        localJobContext.start();


        List<PageDownstreamContext> pageDownstreamContexts = getHandlerPageDownstreamContexts(onHandler);
        int bucketIdx = 0;
        List<ListenableFuture<Bucket>> directResponseFutures = onHandler.v2();

        if (directResponseFutures.size() > 0) {
            Futures.addCallback(Futures.allAsList(directResponseFutures),
                    new SetBucketAction(pageDownstreamContexts, bucketIdx));
            bucketIdx++;
        }

        sendJobRequests(operationByServer, pageDownstreamContexts, bucketIdx);
        if (localNodeOperations.isEmpty() && operationByServer.isEmpty()) {
            // queries on partitioned tables without active partitions can lead to a plan without executionNodes
            // (no shards -> no routing)
            // just close context to trigger a response
            localJobContext.close();
        }
    }

    private void sendJobRequests(Map<String, Collection<NodeOperation>> operationByServer, List<PageDownstreamContext> pageDownstreamContexts, int bucketIdx) {
        for (Map.Entry<String, Collection<NodeOperation>> entry : operationByServer.entrySet()) {
            String serverNodeId = entry.getKey();
            Collection<NodeOperation> nodeOperations = entry.getValue();

            JobRequest request = new JobRequest(jobId(), nodeOperations);
            if (hasDirectResponse) {
                transportJobAction.execute(serverNodeId, request, new SetBucketAction(pageDownstreamContexts, bucketIdx));
            } else {
                transportJobAction.execute(serverNodeId, request, new FailureOnlyResponseListener(results));
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

        private final List<SettableFuture<TaskResult>> results;

        FailureOnlyResponseListener(List<SettableFuture<TaskResult>> results) {
            this.results = results;
        }

        @Override
        public void onResponse(JobResponse jobResponse) {
            if (jobResponse.directResponse().size() > 0) {
                for (SettableFuture<TaskResult> result : results) {
                    result.setException(new IllegalStateException("Got a directResponse but didn't expect one"));
                }
            }
        }

        @Override
        public void onFailure(Throwable e) {
            // in the non-direct-response case the failure is pushed to downStreams
            LOGGER.warn(e.getMessage(), e);
        }
    }

    private static class SetBucketAction implements FutureCallback<List<Bucket>>, ActionListener<JobResponse> {
        private final List<PageDownstreamContext> pageDownstreamContexts;
        private final int bucketIdx;

        SetBucketAction(List<PageDownstreamContext> pageDownstreamContexts, int bucketIdx) {
            this.pageDownstreamContexts = pageDownstreamContexts;
            this.bucketIdx = bucketIdx;
        }

        @Override
        public void onSuccess(@Nullable List<Bucket> result) {
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
            for (int i = 0; i < pageDownstreamContexts.size(); i++) {
                PageDownstreamContext pageDownstreamContext = pageDownstreamContexts.get(i);
                jobResponse.streamers(pageDownstreamContext.streamer());
                setBucket(pageDownstreamContext, jobResponse.directResponse().get(i));
            }
        }

        private void setBucket(PageDownstreamContext pageDownstreamContext, Bucket bucket) {
            if (bucket == null) {
                pageDownstreamContext.failure(bucketIdx, new IllegalStateException("expected directResponse but didn't get one"));
                return;
            }
            pageDownstreamContext.setBucket(bucketIdx, bucket, true, new PageResultListener() {
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
            });
        }

        @Override
        public void onFailure(@Nonnull Throwable t) {
            for (PageDownstreamContext pageDownstreamContext : pageDownstreamContexts) {
                pageDownstreamContext.failure(bucketIdx, t);
            }
        }
    }
}
