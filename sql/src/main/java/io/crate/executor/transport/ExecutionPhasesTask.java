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
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.action.job.ContextPreparer;
import io.crate.action.job.JobRequest;
import io.crate.action.job.JobResponse;
import io.crate.action.job.TransportJobAction;
import io.crate.core.collections.Bucket;
import io.crate.executor.JobTask;
import io.crate.executor.TaskResult;
import io.crate.jobs.*;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.*;
import io.crate.operation.projectors.RowReceiver;
import io.crate.planner.node.ExecutionPhase;
import io.crate.planner.node.ExecutionPhases;
import io.crate.planner.node.NodeOperationGrouper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import javax.annotation.Nullable;
import java.util.*;


public class ExecutionPhasesTask extends JobTask {

    private static final ESLogger LOGGER = Loggers.getLogger(ExecutionPhasesTask.class);

    private final TransportJobAction transportJobAction;
    private final List<NodeOperationTree> nodeOperationTrees;
    private final ClusterService clusterService;
    private ContextPreparer contextPreparer;
    private final JobContextService jobContextService;

    private final List<SettableFuture<TaskResult>> results = new ArrayList<>();
    private boolean hasDirectResponse;

    protected ExecutionPhasesTask(UUID jobId,
                                  ClusterService clusterService,
                                  ContextPreparer contextPreparer,
                                  JobContextService jobContextService,
                                  TransportJobAction transportJobAction,
                                  List<NodeOperationTree> nodeOperationTrees) {
        super(jobId);
        this.clusterService = clusterService;
        this.contextPreparer = contextPreparer;
        this.jobContextService = jobContextService;
        this.transportJobAction = transportJobAction;
        this.nodeOperationTrees = nodeOperationTrees;

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

        Map<String, Collection<NodeOperation>> operationByServer = NodeOperationGrouper.groupByServer(
                clusterService.state().nodes().localNodeId(), nodeOperations);


        List<PageDownstreamContext> pageDownstreamContexts = new ArrayList<>(nodeOperationTrees.size());

        if (nodeOperationTrees.size() > 1) {
            // bulk Operation with rowCountResult
            for (int i = 0; i < nodeOperationTrees.size(); i++) {
                RowCountResultRowDownstream rowDownstream = new RowCountResultRowDownstream(results.get(i));
                setupContext(operationByServer, pageDownstreamContexts, rowDownstream, nodeOperationTrees.get(i));
            }
        } else {
            QueryResultRowDownstream downstream = new QueryResultRowDownstream(Iterables.getOnlyElement(results));
            setupContext(operationByServer, pageDownstreamContexts, downstream, Iterables.getOnlyElement(nodeOperationTrees));
        }

        if (operationByServer.isEmpty()) {
            return;
        }
        sendJobRequests(pageDownstreamContexts, operationByServer);
    }

    private void setupContext(Map<String, Collection<NodeOperation>> operationByServer,
                              List<PageDownstreamContext> pageDownstreamContexts,
                              RowReceiver rowReceiver,
                              NodeOperationTree nodeOperationTree) {
        boolean noNodeOperations = false;
        if (operationByServer.isEmpty()) {
            // this should never happen, instead we should use a NOOP plan instead
            // which immediately results in an empty result
            noNodeOperations = true;
        }
        ExecutionSubContext executionSubContext = createLocalContextAndStartOperation(
                operationByServer,
                nodeOperationTree.leaf(),
                rowReceiver);
        if (noNodeOperations && executionSubContext != null) {
            executionSubContext.close();
            return;
        }
        if (hasDirectResponse) {
            assert executionSubContext != null && executionSubContext instanceof DownstreamExecutionSubContext
                    : "Need DownstreamExecutionSubContext for DIRECT_RESPONSE remote jobs";
            pageDownstreamContexts.add(((DownstreamExecutionSubContext) executionSubContext).pageDownstreamContext((byte) 0));
        }
    }

    private void sendJobRequests(List<PageDownstreamContext> pageDownstreamContexts,
                                 Map<String, Collection<NodeOperation>> operationsByServer) {
        int idx = 0;
        for (Map.Entry<String, Collection<NodeOperation>> entry : operationsByServer.entrySet()) {
            String serverNodeId = entry.getKey();
            if (TableInfo.NULL_NODE_ID.equals(serverNodeId)) {
                continue; // handled by local node
            }
            Collection<NodeOperation> nodeOperations = entry.getValue();

            JobRequest request = new JobRequest(jobId(), nodeOperations);
            if (hasDirectResponse) {
                transportJobAction.execute(serverNodeId, request, new DirectResponseListener(idx, pageDownstreamContexts));
            } else {
                transportJobAction.execute(serverNodeId, request,
                        new FailureOnlyResponseListener(results));
            }
            idx++;
        }
    }

    /**
     * removes the localNodeId entry from the nodesByServer map and initializes the context and starts the operation.
     *
     * This is done in order to be able to create the JobExecutionContext with the localMerge PageDownstreamContext
     */
    @Nullable
    private ExecutionSubContext createLocalContextAndStartOperation(Map<String, Collection<NodeOperation>> operationsByServer,
                                                                    ExecutionPhase handlerFinalMergePhase,
                                                                    RowReceiver handlerPhaseRowReceiver) {
        String localNodeId = clusterService.localNode().id();
        JobExecutionContext.Builder builder = jobContextService.newBuilder(jobId());
        Collection<NodeOperation> localNodeOperations = null;
        if (!hasDirectResponse) {
            localNodeOperations = operationsByServer.remove(localNodeId);
        }
        if (localNodeOperations == null) {
            localNodeOperations = ImmutableList.of();
        }

        ExecutionSubContext finalLocalMergeContext = contextPreparer.prepareOnHandler(
                jobId(),
                localNodeOperations,
                builder,
                handlerFinalMergePhase,
                handlerPhaseRowReceiver);

        if (!hasDirectResponse) {
            JobExecutionContext context = jobContextService.createContext(builder);
            context.start();
        } else if (finalLocalMergeContext != null) {
            finalLocalMergeContext.prepare();
            finalLocalMergeContext.start();
        }
        return finalLocalMergeContext;
    }

    @Override
    public List<? extends ListenableFuture<TaskResult>> result() {
        return results;
    }

    @Override
    public void upstreamResult(List<? extends ListenableFuture<TaskResult>> result) {
        throw new UnsupportedOperationException("ExecutionNodesTask doesn't support upstreamResult");
    }

    private static class DirectResponseListener implements ActionListener<JobResponse> {

        private final int bucketIdx;
        private final List<PageDownstreamContext> pageDownstreamContexts;

        public DirectResponseListener(int bucketIdx, List<PageDownstreamContext> pageDownstreamContexts) {
            this.bucketIdx = bucketIdx;
            this.pageDownstreamContexts = pageDownstreamContexts;
        }

        @Override
        public void onResponse(JobResponse jobResponse) {
            for (int i = 0; i < pageDownstreamContexts.size(); i++) {
                PageDownstreamContext pageDownstreamContext = pageDownstreamContexts.get(i);
                jobResponse.streamers(pageDownstreamContext.streamer());
                Bucket bucket = jobResponse.directResponse().get(i);
                if (bucket == null) {
                    pageDownstreamContext.failure(bucketIdx, new IllegalStateException("expected directResponse but didn't get one"));
                }
                pageDownstreamContext.setBucket(bucketIdx, bucket, true, new PageResultListener() {
                    @Override
                    public void needMore(boolean needMore) {
                        // can't page with directResult
                    }

                    @Override
                    public int buckedIdx() {
                        return bucketIdx;
                    }
                });
            }
        }

        @Override
        public void onFailure(Throwable e) {
            for (PageDownstreamContext pageDownstreamContext : pageDownstreamContexts) {
                pageDownstreamContext.failure(bucketIdx, e);
            }
        }
    }

    private static class FailureOnlyResponseListener implements ActionListener<JobResponse> {

        private final List<SettableFuture<TaskResult>> results;

        public FailureOnlyResponseListener(List<SettableFuture<TaskResult>> results) {
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
}
