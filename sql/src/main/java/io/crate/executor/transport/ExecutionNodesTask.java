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

import com.google.common.base.Optional;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.Streamer;
import io.crate.action.job.JobRequest;
import io.crate.action.job.JobResponse;
import io.crate.action.job.TransportJobAction;
import io.crate.breaker.RamAccountingContext;
import io.crate.core.collections.Bucket;
import io.crate.executor.JobTask;
import io.crate.executor.TaskResult;
import io.crate.executor.callbacks.OperationFinishedStatsTablesCallback;
import io.crate.jobs.JobContextService;
import io.crate.jobs.PageDownstreamContext;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.*;
import io.crate.operation.collect.HandlerSideDataCollectOperation;
import io.crate.operation.collect.StatsTables;
import io.crate.operation.projectors.CollectingProjector;
import io.crate.operation.projectors.FlatProjectorChain;
import io.crate.operation.projectors.ProjectionToProjectorVisitor;
import io.crate.planner.node.ExecutionNode;
import io.crate.planner.node.ExecutionNodeVisitor;
import io.crate.planner.node.ExecutionNodes;
import io.crate.planner.node.StreamerVisitor;
import io.crate.planner.node.dql.CollectNode;
import io.crate.planner.node.dql.MergeNode;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.threadpool.ThreadPool;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;


public class ExecutionNodesTask extends JobTask {

    private static final ESLogger LOGGER = Loggers.getLogger(ExecutionNodesTask.class);

    private final TransportJobAction transportJobAction;
    private final ExecutionNode[] executionNodes;
    private final List<ListenableFuture<TaskResult>> results;
    private final boolean hasDirectResponse;
    private final SettableFuture<TaskResult> result;
    private final ProjectionToProjectorVisitor projectionToProjectorVisitor;
    private final JobContextService jobContextService;
    private final PageDownstreamFactory pageDownstreamFactory;
    private final StatsTables statsTables;
    private final ThreadPool threadPool;
    private final TransportCloseContextNodeAction transportCloseContextNodeAction;
    private final HandlerSideDataCollectOperation handlerSideDataCollectOperation;
    private final StreamerVisitor streamerVisitor;
    private final CircuitBreaker circuitBreaker;
    private MergeNode mergeNode;

    /**
     * @param mergeNode the mergeNode for the final merge operation on the handler.
     *                  This may be null in the CTOR but then it must be set using the
     *                  {@link #mergeNode(MergeNode)} setter before {@link #start()} is called.
     */
    protected ExecutionNodesTask(UUID jobId,
                                 ProjectionToProjectorVisitor projectionToProjectorVisitor,
                                 JobContextService jobContextService,
                                 PageDownstreamFactory pageDownstreamFactory,
                                 StatsTables statsTables,
                                 ThreadPool threadPool,
                                 TransportJobAction transportJobAction,
                                 TransportCloseContextNodeAction transportCloseContextNodeAction,
                                 HandlerSideDataCollectOperation handlerSideDataCollectOperation,
                                 StreamerVisitor streamerVisitor,
                                 CircuitBreaker circuitBreaker,
                                 @Nullable MergeNode mergeNode,
                                 ExecutionNode... executionNodes) {
        super(jobId);
        this.projectionToProjectorVisitor = projectionToProjectorVisitor;
        this.jobContextService = jobContextService;
        this.pageDownstreamFactory = pageDownstreamFactory;
        this.statsTables = statsTables;
        this.threadPool = threadPool;
        this.transportCloseContextNodeAction = transportCloseContextNodeAction;
        this.handlerSideDataCollectOperation = handlerSideDataCollectOperation;
        this.streamerVisitor = streamerVisitor;
        this.circuitBreaker = circuitBreaker;
        this.mergeNode = mergeNode;
        this.transportJobAction = transportJobAction;
        this.executionNodes = executionNodes;
        result = SettableFuture.create();
        results = ImmutableList.<ListenableFuture<TaskResult>>of(result);
        hasDirectResponse = hasDirectResponse(executionNodes);
    }

    public void mergeNode(MergeNode mergeNode) {
        assert this.mergeNode == null : "can only overwrite mergeNode if it was null";
        this.mergeNode = mergeNode;
    }

    @Override
    public void start() {
        assert mergeNode != null : "mergeNode must not be null";
        RamAccountingContext ramAccountingContext = trackOperation(mergeNode, "localMerge");

        RowDownstream rowDownstream = new QueryResultRowDownstream(result, jobId(), mergeNode.executionNodeId(), jobContextService);
        PageDownstream finalMergePageDownstream = pageDownstreamFactory.createMergeNodePageDownstream(
                mergeNode,
                rowDownstream,
                ramAccountingContext,
                Optional.of(threadPool.executor(ThreadPool.Names.SEARCH))
        );

        Streamer<?>[] streamers = streamerVisitor.processExecutionNode(mergeNode, ramAccountingContext).inputStreamers();
        PageDownstreamContext pageDownstreamContext = new PageDownstreamContext(
                finalMergePageDownstream,
                streamers,
                executionNodes[executionNodes.length - 1].executionNodes().size()
        );
        if (!hasDirectResponse) {
            jobContextService.initializeFinalMerge(jobId(), mergeNode.executionNodeId(), pageDownstreamContext);
        }

        Map<String, Collection<ExecutionNode>> nodesByServer = groupExecutionNodesByServer(executionNodes);
        if (nodesByServer.size() == 0) {
            pageDownstreamContext.finish();
            return;
        }

        addCloseContextCallback(transportCloseContextNodeAction, executionNodes, nodesByServer.keySet());
        int idx = 0;
        for (Map.Entry<String, Collection<ExecutionNode>> entry : nodesByServer.entrySet()) {
            String serverNodeId = entry.getKey();
            Collection<ExecutionNode> executionNodes = entry.getValue();

            if (serverNodeId.equals(TableInfo.NULL_NODE_ID)) {
                handlerSideCollect(executionNodes, pageDownstreamContext);
            } else {
                JobRequest request = new JobRequest(jobId(), executionNodes);
                if (hasDirectResponse) {
                    transportJobAction.execute(serverNodeId, request, new DirectResponseListener(idx, streamers, pageDownstreamContext));
                } else {
                    transportJobAction.execute(serverNodeId, request, new FailureOnlyResponseListener(result));
                }
            }
            idx++;
        }
    }

    private void handlerSideCollect(Collection<ExecutionNode> executionNodes,
                                    final PageDownstreamContext pageDownstreamContext) {
        assert executionNodes.size() == 1 : "handlerSideCollect is only possible with 1 collectNode";
        ExecutionNode onlyElement = Iterables.getOnlyElement(executionNodes);
        assert onlyElement instanceof CollectNode : "handlerSideCollect is only possible with 1 collectNode";

        CollectNode collectNode = ((CollectNode) onlyElement);
        RamAccountingContext ramAccountingContext = trackOperation(collectNode, "handlerSide collect");
        CollectingProjector collectingProjector = new CollectingProjector();
        Futures.addCallback(collectingProjector.result(), new FutureCallback<Bucket>() {
            @Override
            public void onSuccess(Bucket result) {
                // TODO: change bucketIdx once the logic for bucketIdxs has been changed
                pageDownstreamContext.setBucket(0, result, true, new PageConsumeListener() {
                    @Override
                    public void needMore() {
                        finish();
                    }

                    @Override
                    public void finish() {
                        pageDownstreamContext.finish();
                    }
                });
            }

            @Override
            public void onFailure(@Nonnull Throwable t) {
                pageDownstreamContext.failure(t);
            }
        });

        RowDownstream rowDownstream = collectingProjector;
        if (!collectNode.projections().isEmpty()) {
            FlatProjectorChain flatProjectorChain = FlatProjectorChain.withAttachedDownstream(
                    projectionToProjectorVisitor,
                    ramAccountingContext,
                    collectNode.projections(),
                    collectingProjector);
            flatProjectorChain.startProjections();
            rowDownstream = flatProjectorChain.firstProjector();
        }
        if (hasDirectResponse) {
            handlerSideDataCollectOperation.collect(collectNode, rowDownstream, ramAccountingContext);
        } else {
            throw new UnsupportedOperationException("handler side collect doesn't support push based collect");
        }
    }

    private RamAccountingContext trackOperation(ExecutionNode executionNode, String operationName) {
        UUID operationId = UUID.randomUUID();
        RamAccountingContext ramAccountingContext = RamAccountingContext.forExecutionNode(circuitBreaker, executionNode, operationId);
        statsTables.operationStarted(operationId, jobId(), operationName);
        Futures.addCallback(result, new OperationFinishedStatsTablesCallback<>(operationId, statsTables, ramAccountingContext));
        return ramAccountingContext;
    }

    @Override
    public List<ListenableFuture<TaskResult>> result() {
        return results;
    }

    @Override
    public void upstreamResult(List<ListenableFuture<TaskResult>> result) {
        throw new UnsupportedOperationException("ExecutionNodesTask doesn't support upstreamResult");
    }

    private void addCloseContextCallback(TransportCloseContextNodeAction transportCloseContextNodeAction,
                                         final ExecutionNode[] executionNodes,
                                         final Set<String> server) {
        final ContextCloser contextCloser = new ContextCloser(transportCloseContextNodeAction);
        Futures.addCallback(result, new FutureCallback<TaskResult>() {
            @Override
            public void onSuccess(TaskResult result) {
                closeContext();
            }

            @Override
            public void onFailure(@Nonnull Throwable t) {
                closeContext();
            }

            private void closeContext() {
                if (server.isEmpty()) {
                    return;
                }
                for (ExecutionNode executionNode : executionNodes) {
                    contextCloser.process(executionNode, server);
                }
            }
        });
    }

    static boolean hasDirectResponse(ExecutionNode[] executionNodes) {
        for (ExecutionNode executionNode : executionNodes) {
            if (ExecutionNodes.hasDirectResponseDownstream(executionNode.downstreamNodes())) {
                return true;
            }
        }
        return false;
    }

    static Map<String, Collection<ExecutionNode>> groupExecutionNodesByServer(ExecutionNode[] executionNodes) {
        ArrayListMultimap<String, ExecutionNode> executionNodesGroupedByServer = ArrayListMultimap.create();
        for (ExecutionNode executionNode : executionNodes) {
            for (String server : executionNode.executionNodes()) {
                executionNodesGroupedByServer.put(server, executionNode);
            }
        }
        return executionNodesGroupedByServer.asMap();
    }

    private static class DirectResponseListener implements ActionListener<JobResponse> {

        private final int bucketIdx;
        private final Streamer<?>[] streamer;
        private final PageDownstreamContext pageDownstreamContext;

        public DirectResponseListener(int bucketIdx, Streamer<?>[] streamer, PageDownstreamContext pageDownstreamContext) {
            this.bucketIdx = bucketIdx;
            this.streamer = streamer;
            this.pageDownstreamContext = pageDownstreamContext;
        }

        @Override
        public void onResponse(JobResponse jobResponse) {
            jobResponse.streamers(streamer);
            if (jobResponse.directResponse().isPresent()) {
                pageDownstreamContext.setBucket(bucketIdx, jobResponse.directResponse().get(), true, new PageConsumeListener() {
                    @Override
                    public void needMore() {
                        // can't page with directResult
                        finish();
                    }

                    @Override
                    public void finish() {
                        pageDownstreamContext.finish();
                    }
                });
            } else {
                pageDownstreamContext.failure(new IllegalStateException("expected directResponse but didn't get one"));
            }
        }

        @Override
        public void onFailure(Throwable e) {
            pageDownstreamContext.failure(e);
        }
    }

    private static class FailureOnlyResponseListener implements ActionListener<JobResponse> {

        private final SettableFuture<TaskResult> result;

        public FailureOnlyResponseListener(SettableFuture<TaskResult> result) {
            this.result = result;
        }

        @Override
        public void onResponse(JobResponse jobResponse) {
            if (jobResponse.directResponse().isPresent()) {
                result.setException(new IllegalStateException("Got a directResponse but didn't expect one"));
            }
        }

        @Override
        public void onFailure(Throwable e) {
            result.setException(e);
        }
    }

    private static class ContextCloser extends ExecutionNodeVisitor<Set<String>, Void> {

        private final TransportCloseContextNodeAction transportCloseContextNodeAction;

        public ContextCloser(TransportCloseContextNodeAction transportCloseContextNodeAction) {
            this.transportCloseContextNodeAction = transportCloseContextNodeAction;
        }

        @Override
        public Void visitCollectNode(final CollectNode node, Set<String> nodeIds) {
            if (node.keepContextForFetcher()) {
                LOGGER.trace("closing job context {} on {} nodes", node.jobId().get(), nodeIds.size());
                for (final String nodeId : nodeIds) {
                    transportCloseContextNodeAction.execute(nodeId, new NodeCloseContextRequest(node.jobId().get()), new ActionListener<NodeCloseContextResponse>() {
                        @Override
                        public void onResponse(NodeCloseContextResponse nodeCloseContextResponse) {
                        }

                        @Override
                        public void onFailure(Throwable e) {
                            LOGGER.warn("Closing job context {} failed on node {} with: {}", node.jobId().get(), nodeId, e.getMessage());
                        }
                    });
                }
            }
            return null;
        }
    }
}
