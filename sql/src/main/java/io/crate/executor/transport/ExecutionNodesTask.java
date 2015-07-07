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
import io.crate.planner.node.ExecutionPhase;
import io.crate.planner.node.ExecutionNodeVisitor;
import io.crate.planner.node.ExecutionNodes;
import io.crate.planner.node.StreamerVisitor;
import io.crate.planner.node.dql.CollectPhase;
import io.crate.planner.node.dql.MergePhase;
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
    private final ExecutionPhase[] executionPhases;
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
    private MergePhase mergeNode;

    /**
     * @param mergeNode the mergeNode for the final merge operation on the handler.
     *                  This may be null in the constructor but then it must be set using the
     *                  {@link #mergeNode(MergePhase)} setter before {@link #start()} is called.
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
                                 @Nullable MergePhase mergeNode,
                                 ExecutionPhase... executionPhases) {
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
        this.executionPhases = executionPhases;
        hasDirectResponse = hasDirectResponse(executionPhases);

        result = SettableFuture.create();
        results = ImmutableList.<ListenableFuture<TaskResult>>of(result);
    }

    public void mergeNode(MergePhase mergeNode) {
        assert this.mergeNode == null : "can only overwrite mergeNode if it was null";
        this.mergeNode = mergeNode;
    }

    @Override
    public void start() {
        assert mergeNode != null : "mergeNode must not be null";
        RamAccountingContext ramAccountingContext = trackOperation(mergeNode, "localMerge");

        Streamer<?>[] streamers = streamerVisitor.processExecutionNode(mergeNode).inputStreamers();
        final PageDownstreamContext pageDownstreamContext = createPageDownstreamContext(ramAccountingContext, streamers);
        Map<String, Collection<ExecutionPhase>> nodesByServer = groupExecutionNodesByServer(executionPhases);
        if (nodesByServer.size() == 0) {
            pageDownstreamContext.finish();
            return;
        }
        if (!hasDirectResponse) {
            createLocalContextAndStartOperation(pageDownstreamContext, nodesByServer);
        }
        addCloseContextCallback(transportCloseContextNodeAction, executionPhases, nodesByServer.keySet());
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
                ramAccountingContext,
                executionPhases[executionPhases.length - 1].executionNodes().size()
        );
    }

    private void sendJobRequests(Streamer<?>[] streamers,
                                 PageDownstreamContext pageDownstreamContext,
                                 Map<String, Collection<ExecutionPhase>> nodesByServer) {
        int idx = 0;
        for (Map.Entry<String, Collection<ExecutionPhase>> entry : nodesByServer.entrySet()) {
            String serverNodeId = entry.getKey();
            Collection<ExecutionPhase> executionPhases = entry.getValue();

            if (serverNodeId.equals(TableInfo.NULL_NODE_ID)) {
                handlerSideCollect(idx, executionPhases, pageDownstreamContext);
            } else {
                JobRequest request = new JobRequest(jobId(), executionPhases);
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
                                                     Map<String, Collection<ExecutionPhase>> nodesByServer) {
        String localNodeId = clusterService.localNode().id();
        Collection<ExecutionPhase> localExecutionPhases = nodesByServer.remove(localNodeId);

        JobExecutionContext.Builder builder = jobContextService.newBuilder(jobId());

        if (localExecutionPhases == null || localExecutionPhases.isEmpty()) {
            // only the local merge happens locally so it is enough to just create that context.
            builder.addSubContext(mergeNode.executionPhaseId(), finalLocalMerge);
            jobContextService.createOrMergeContext(builder);
        } else {
            for (ExecutionPhase executionPhase : localExecutionPhases) {
                contextPreparer.prepare(jobId(), executionPhase, builder);
            }
            builder.addSubContext(mergeNode.executionPhaseId(), finalLocalMerge);
            JobExecutionContext context = jobContextService.createOrMergeContext(builder);
            for (ExecutionPhase executionPhase : localExecutionPhases) {
                executionNodeOperationStarter.startOperation(executionPhase, context);
            }
        }
    }

    private void handlerSideCollect(final int bucketIdx,
                                    Collection<ExecutionPhase> executionPhases,
                                    final PageDownstreamContext pageDownstreamContext) {
        assert executionPhases.size() == 1 : "handlerSideCollect is only possible with 1 collectNode";
        ExecutionPhase onlyElement = Iterables.getOnlyElement(executionPhases);
        assert onlyElement instanceof CollectPhase : "handlerSideCollect is only possible with 1 collectNode";

        CollectPhase collectNode = ((CollectPhase) onlyElement);
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
                                         final ExecutionPhase[] executionPhases,
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
                for (ExecutionPhase executionPhase : executionPhases) {
                    contextCloser.process(executionPhase, server);
                }
            }
        });
    }

    private RamAccountingContext trackOperation(ExecutionPhase executionPhase, String operationName) {
        RamAccountingContext ramAccountingContext = RamAccountingContext.forExecutionNode(circuitBreaker, executionPhase);
        statsTables.operationStarted(executionPhase.executionPhaseId(), jobId(), operationName);
        Futures.addCallback(result, new OperationFinishedStatsTablesCallback<>(
                executionPhase.executionPhaseId(), statsTables, ramAccountingContext));
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

    static boolean hasDirectResponse(ExecutionPhase[] executionPhases) {
        for (ExecutionPhase executionPhase : executionPhases) {
            if (ExecutionNodes.hasDirectResponseDownstream(executionPhase.downstreamNodes())) {
                return true;
            }
        }
        return false;
    }

    static Map<String, Collection<ExecutionPhase>> groupExecutionNodesByServer(ExecutionPhase[] executionPhases) {
        ArrayListMultimap<String, ExecutionPhase> executionNodesGroupedByServer = ArrayListMultimap.create();
        for (ExecutionPhase executionPhase : executionPhases) {
            for (String server : executionPhase.executionNodes()) {
                executionNodesGroupedByServer.put(server, executionPhase);
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
        public Void visitCollectNode(final CollectPhase node, Set<String> nodeIds) {
            if (!node.keepContextForFetcher()) {
                return null;
            }

            LOGGER.trace("closing job context {} on {} nodes", node.jobId(), nodeIds.size());
            for (final String nodeId : nodeIds) {
                transportCloseContextNodeAction.execute(
                        nodeId,
                        new NodeCloseContextRequest(node.jobId(), node.executionPhaseId()),
                        new ActionListener<NodeCloseContextResponse>() {

                    @Override
                    public void onResponse(NodeCloseContextResponse nodeCloseContextResponse) {
                    }

                    @Override
                    public void onFailure(Throwable e) {
                        LOGGER.warn("Closing job context {} failed on node {} with: {}", node.jobId(), nodeId, e.getMessage());
                    }
                });
            }
            return null;
        }
    }
}
