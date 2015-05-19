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
import com.google.common.util.concurrent.*;
import io.crate.Streamer;
import io.crate.action.job.*;
import io.crate.breaker.RamAccountingContext;
import io.crate.core.collections.Bucket;
import io.crate.executor.JobTask;
import io.crate.executor.TaskResult;
import io.crate.executor.callbacks.OperationFinishedStatsTablesCallback;
import io.crate.jobs.JobContextService;
import io.crate.jobs.JobExecutionContext;
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
import org.elasticsearch.cluster.ClusterService;
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
    private final ClusterService clusterService;
    private ContextPreparer contextPreparer;
    private final ExecutionNodeOperationStarter executionNodeOperationStarter;
    private final HandlerSideDataCollectOperation handlerSideDataCollectOperation;
    private final ProjectionToProjectorVisitor projectionToProjectorVisitor;
    private final JobContextService jobContextService;
    private final PageDownstreamFactory pageDownstreamFactory;
    private final StatsTables statsTables;
    private final ThreadPool threadPool;
    private TransportCloseContextNodeAction transportCloseContextNodeAction;
    private final StreamerVisitor streamerVisitor;
    private final CircuitBreaker circuitBreaker;
    private MergeNode mergeNode;

    /**
     * @param mergeNode the mergeNode for the final merge operation on the handler.
     *                  This may be null in the constructor but then it must be set using the
     *                  {@link #mergeNode(MergeNode)} setter before {@link #start()} is called.
     */
    protected ExecutionNodesTask(UUID jobId,
                                 ClusterService clusterService,
                                 ContextPreparer contextPreparer,
                                 ExecutionNodeOperationStarter executionNodeOperationStarter,
                                 HandlerSideDataCollectOperation handlerSideDataCollectOperation,
                                 ProjectionToProjectorVisitor projectionToProjectorVisitor,
                                 JobContextService jobContextService,
                                 PageDownstreamFactory pageDownstreamFactory,
                                 StatsTables statsTables,
                                 ThreadPool threadPool,
                                 TransportJobAction transportJobAction,
                                 TransportCloseContextNodeAction transportCloseContextNodeAction,
                                 StreamerVisitor streamerVisitor,
                                 CircuitBreaker circuitBreaker,
                                 @Nullable MergeNode mergeNode,
                                 ExecutionNode... executionNodes) {
        super(jobId);
        this.clusterService = clusterService;
        this.contextPreparer = contextPreparer;
        this.executionNodeOperationStarter = executionNodeOperationStarter;
        this.handlerSideDataCollectOperation = handlerSideDataCollectOperation;
        this.projectionToProjectorVisitor = projectionToProjectorVisitor;
        this.jobContextService = jobContextService;
        this.pageDownstreamFactory = pageDownstreamFactory;
        this.statsTables = statsTables;
        this.threadPool = threadPool;
        this.transportCloseContextNodeAction = transportCloseContextNodeAction;
        this.streamerVisitor = streamerVisitor;
        this.circuitBreaker = circuitBreaker;
        this.mergeNode = mergeNode;
        this.transportJobAction = transportJobAction;
        this.executionNodes = executionNodes;
        hasDirectResponse = hasDirectResponse(executionNodes);

        result = SettableFuture.create();
        results = ImmutableList.<ListenableFuture<TaskResult>>of(result);
    }

    public void mergeNode(MergeNode mergeNode) {
        assert this.mergeNode == null : "can only overwrite mergeNode if it was null";
        this.mergeNode = mergeNode;
    }

    @Override
    public void start() {
        assert mergeNode != null : "mergeNode must not be null";
        RamAccountingContext ramAccountingContext = trackOperation(mergeNode, "localMerge");

        Streamer<?>[] streamers = streamerVisitor.processExecutionNode(mergeNode).inputStreamers();
        final PageDownstreamContext pageDownstreamContext = createPageDownstreamContext(ramAccountingContext, streamers);
        Map<String, Collection<ExecutionNode>> nodesByServer = groupExecutionNodesByServer(executionNodes);
        if (nodesByServer.size() == 0) {
            pageDownstreamContext.finish();
            return;
        }
        if (!hasDirectResponse) {
            createLocalContextAndStartOperation(pageDownstreamContext, nodesByServer);
        }
        addCloseContextCallback(transportCloseContextNodeAction, executionNodes, nodesByServer.keySet());
        sendJobRequests(streamers, pageDownstreamContext, nodesByServer);
    }

    private PageDownstreamContext createPageDownstreamContext(RamAccountingContext ramAccountingContext, Streamer<?>[] streamers) {
        RowDownstream rowDownstream = new QueryResultRowDownstream(result);
        PageDownstream finalMergePageDownstream = pageDownstreamFactory.createMergeNodePageDownstream(
                mergeNode,
                rowDownstream,
                ramAccountingContext,
                Optional.of(threadPool.executor(ThreadPool.Names.SEARCH))
        );
        return new PageDownstreamContext(
                finalMergePageDownstream,
                streamers,
                executionNodes[executionNodes.length - 1].executionNodes().size()
        );
    }

    private void sendJobRequests(Streamer<?>[] streamers,
                                 PageDownstreamContext pageDownstreamContext,
                                 Map<String, Collection<ExecutionNode>> nodesByServer) {
        int idx = 0;
        for (Map.Entry<String, Collection<ExecutionNode>> entry : nodesByServer.entrySet()) {
            String serverNodeId = entry.getKey();
            Collection<ExecutionNode> executionNodes = entry.getValue();

            if (serverNodeId.equals(TableInfo.NULL_NODE_ID)) {
                handlerSideCollect(idx, executionNodes, pageDownstreamContext);
            } else {
                JobRequest request = new JobRequest(jobId(), executionNodes);
                if (hasDirectResponse) {
                    transportJobAction.execute(serverNodeId, request,
                            new DirectResponseListener(idx, streamers, pageDownstreamContext));
                } else {
                    transportJobAction.execute(serverNodeId, request,
                            new FailureOnlyResponseListener(result));
                }
            }
            idx++;
        }
    }

    /**
     * removes the localNodeId entry from the nodesByServer map and initializes the context and starts the operation.
     *
     * This is done in order to be able to create the JobExecutionContext with the localMerge PageDownstreamContext
     */
    private void createLocalContextAndStartOperation(PageDownstreamContext finalLocalMerge,
                                                     Map<String, Collection<ExecutionNode>> nodesByServer) {
        String localNodeId = clusterService.localNode().id();
        Collection<ExecutionNode> localExecutionNodes = nodesByServer.remove(localNodeId);

        JobExecutionContext.Builder builder = jobContextService.newBuilder(jobId());
        builder.addSubContext(mergeNode.executionNodeId(), finalLocalMerge);

        if (localExecutionNodes == null || localExecutionNodes.isEmpty()) {
            // only the local merge happens locally so it is enough to just create that context.
            jobContextService.createOrMergeContext(builder);
        } else {
            for (ExecutionNode executionNode : localExecutionNodes) {
                contextPreparer.prepare(jobId(), executionNode, builder);
            }
            JobExecutionContext context = jobContextService.createOrMergeContext(builder);
            for (ExecutionNode executionNode : localExecutionNodes) {
                executionNodeOperationStarter.startOperation(executionNode, context);
            }
        }
    }

    private void handlerSideCollect(final int bucketIdx,
                                    Collection<ExecutionNode> executionNodes,
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
                pageDownstreamContext.setBucket(bucketIdx, result, true, new PageResultListener() {
                    @Override
                    public void needMore(boolean needMore) {
                        // can't page
                    }

                    @Override
                    public int buckedIdx() {
                        return bucketIdx;
                    }
                });
            }

            @Override
            public void onFailure(@Nonnull Throwable t) {
                pageDownstreamContext.failure(bucketIdx, t);
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

    private void addCloseContextCallback(TransportCloseContextNodeAction transportCloseContextNodeAction,
                                         final ExecutionNode[] executionNodes,
                                         final Set<String> server) {
        if (server.isEmpty()) {
            return;
        }
        final ContextCloser contextCloser = new ContextCloser(transportCloseContextNodeAction);
        Futures.addCallback(Futures.allAsList(results), new FutureCallback<List<TaskResult>>() {
            @Override
            public void onSuccess(@Nullable List<TaskResult> result) {
                // do nothing, contexts will be closed through fetch projection
            }

            @Override
            public void onFailure(@Nonnull Throwable t) {
                // if a failure happens, no fetch projection will be called, clean up contexts
                for (ExecutionNode executionNode : executionNodes) {
                    contextCloser.process(executionNode, server);
                }
            }
        });
    }

    private RamAccountingContext trackOperation(ExecutionNode executionNode, String operationName) {
        RamAccountingContext ramAccountingContext = RamAccountingContext.forExecutionNode(circuitBreaker, executionNode);
        statsTables.operationStarted(executionNode.executionNodeId(), jobId(), operationName);
        Futures.addCallback(result, new OperationFinishedStatsTablesCallback<>(
                executionNode.executionNodeId(), statsTables, ramAccountingContext));
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
                pageDownstreamContext.setBucket(bucketIdx, jobResponse.directResponse().get(), true, new PageResultListener() {
                    @Override
                    public void needMore(boolean needMore) {
                        // can't page with directResult
                    }

                    @Override
                    public int buckedIdx() {
                        return bucketIdx;
                    }
                });
            } else {
                pageDownstreamContext.failure(bucketIdx, new IllegalStateException("expected directResponse but didn't get one"));
            }
        }

        @Override
        public void onFailure(Throwable e) {
            pageDownstreamContext.failure(bucketIdx, e);
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
            // in the non-direct-response case the failure is pushed to downStreams
            LOGGER.warn(e.getMessage(), e);
        }
    }

    private static class ContextCloser extends ExecutionNodeVisitor<Set<String>, Void> {

        private final TransportCloseContextNodeAction transportCloseContextNodeAction;

        public ContextCloser(TransportCloseContextNodeAction transportCloseContextNodeAction) {
            this.transportCloseContextNodeAction = transportCloseContextNodeAction;
        }

        @Override
        public Void visitCollectNode(final CollectNode node, Set<String> nodeIds) {
            if (!node.keepContextForFetcher()) {
                return null;
            }

            LOGGER.trace("closing job context {} on {} nodes", node.jobId().get(), nodeIds.size());
            for (final String nodeId : nodeIds) {
                transportCloseContextNodeAction.execute(
                        nodeId,
                        new NodeCloseContextRequest(node.jobId().get(), node.executionNodeId()),
                        new ActionListener<NodeCloseContextResponse>() {

                    @Override
                    public void onResponse(NodeCloseContextResponse nodeCloseContextResponse) {
                    }

                    @Override
                    public void onFailure(Throwable e) {
                        LOGGER.warn("Closing job context {} failed on node {} with: {}", node.jobId().get(), nodeId, e.getMessage());
                    }
                });
            }
            return null;
        }
    }
}
