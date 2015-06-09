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
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.Streamer;
import io.crate.action.job.ContextPreparer;
import io.crate.action.job.JobRequest;
import io.crate.action.job.JobResponse;
import io.crate.action.job.TransportJobAction;
import io.crate.breaker.RamAccountingContext;
import io.crate.core.collections.Bucket;
import io.crate.executor.JobTask;
import io.crate.executor.TaskResult;
import io.crate.jobs.JobContextService;
import io.crate.jobs.JobExecutionContext;
import io.crate.jobs.PageDownstreamContext;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.*;
import io.crate.operation.projectors.FlatProjectorChain;
import io.crate.planner.node.*;
import io.crate.planner.node.dql.CollectNode;
import io.crate.planner.node.dql.MergeNode;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.threadpool.ThreadPool;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;


public class ExecutionNodesTask extends JobTask {

    private static final ESLogger LOGGER = Loggers.getLogger(ExecutionNodesTask.class);

    private final TransportJobAction transportJobAction;
    private final ClusterService clusterService;
    private ContextPreparer contextPreparer;
    private final JobContextService jobContextService;
    private final PageDownstreamFactory pageDownstreamFactory;
    private final ThreadPool threadPool;
    private TransportCloseContextNodeAction transportCloseContextNodeAction;
    private final StreamerVisitor streamerVisitor;
    private final CircuitBreaker circuitBreaker;

    private final List<List<ExecutionNode>> groupedExecutionNodes = new ArrayList<>();
    private final List<MergeNode> finalMergeNodes = new ArrayList<>();
    private final List<SettableFuture<TaskResult>> results = new ArrayList<>();
    private boolean hasDirectResponse;
    private boolean rowCountResult = false;

    protected ExecutionNodesTask(UUID jobId,
                                 ClusterService clusterService,
                                 ContextPreparer contextPreparer,
                                 JobContextService jobContextService,
                                 PageDownstreamFactory pageDownstreamFactory,
                                 ThreadPool threadPool,
                                 TransportJobAction transportJobAction,
                                 TransportCloseContextNodeAction transportCloseContextNodeAction,
                                 StreamerVisitor streamerVisitor,
                                 CircuitBreaker circuitBreaker) {
        super(jobId);
        this.clusterService = clusterService;
        this.contextPreparer = contextPreparer;
        this.jobContextService = jobContextService;
        this.pageDownstreamFactory = pageDownstreamFactory;
        this.threadPool = threadPool;
        this.transportCloseContextNodeAction = transportCloseContextNodeAction;
        this.streamerVisitor = streamerVisitor;
        this.circuitBreaker = circuitBreaker;
        this.transportJobAction = transportJobAction;
    }


    /**
     * @param finalMergeNode a mergeNode for the final merge operation on the handler.
     *                   Multiple merge nodes are only occurring on bulk operations.
     */
    public void addFinalMergeNode(MergeNode finalMergeNode) {
        finalMergeNode.jobId(jobId());
        finalMergeNodes.add(finalMergeNode);
    }

    public void addExecutionNode(int group, ExecutionNode executionNode) {
        executionNode.jobId(jobId());
        while (group >= groupedExecutionNodes.size()) {
            results.add(SettableFuture.<TaskResult>create());
            groupedExecutionNodes.add(new ArrayList<ExecutionNode>());
        }
        List<ExecutionNode> executionNodes = groupedExecutionNodes.get(group);

        if (ExecutionNodes.hasDirectResponseDownstream(executionNode.downstreamNodes())) {
            hasDirectResponse = true;
        }
        executionNodes.add(executionNode);
    }

    public void rowCountResult(boolean rowCountResult) {
        this.rowCountResult = rowCountResult;
    }

    @Override
    public void start() {
        assert finalMergeNodes.size() == groupedExecutionNodes.size() : "groupedExecutionNodes and finalMergeNodes sizes must match";

        Map<String, Collection<ExecutionNode>> nodesByServer = ExecutionNodeGrouper.groupByServer(clusterService.state().nodes().localNodeId(), groupedExecutionNodes);
        RowDownstream rowDownstream;
        if (rowCountResult) {
            rowDownstream = new RowCountResultRowDownstream(results);
        } else {
            rowDownstream = new QueryResultRowDownstream(results);
        }
        Streamer<?>[] streamers = streamerVisitor.processExecutionNode(finalMergeNodes.get(0)).inputStreamers();
        List<PageDownstreamContext> pageDownstreamContexts = new ArrayList<>(groupedExecutionNodes.size());

        for (int i = 0; i < groupedExecutionNodes.size(); i++) {
            RamAccountingContext ramAccountingContext = RamAccountingContext.forExecutionNode(
                    circuitBreaker, finalMergeNodes.get(i));

            PageDownstreamContext pageDownstreamContext = createPageDownstreamContext(ramAccountingContext, streamers,
                    finalMergeNodes.get(i), groupedExecutionNodes.get(i), rowDownstream);
            if (nodesByServer.size() == 0) {
                pageDownstreamContext.finish();
                continue;
            }
            if (!hasDirectResponse) {
                createLocalContextAndStartOperation(pageDownstreamContext, nodesByServer, finalMergeNodes.get(i).executionNodeId());
            }
            pageDownstreamContexts.add(pageDownstreamContext);
        }
        if (nodesByServer.size() == 0) {
            return;
        }
        addCloseContextCallback(transportCloseContextNodeAction, groupedExecutionNodes, nodesByServer.keySet());
        sendJobRequests(streamers, pageDownstreamContexts, nodesByServer);
    }

    private PageDownstreamContext createPageDownstreamContext(
            RamAccountingContext ramAccountingContext,
            Streamer<?>[] streamers,
            MergeNode mergeNode,
            List<ExecutionNode> executionNodes,
            RowDownstream rowDownstream) {
        Tuple<PageDownstream, FlatProjectorChain> pageDownstreamProjectorChain = pageDownstreamFactory.createMergeNodePageDownstream(
                mergeNode,
                rowDownstream,
                ramAccountingContext,
                Optional.of(threadPool.executor(ThreadPool.Names.SEARCH))
        );
        PageDownstreamContext pageDownstreamContext = new PageDownstreamContext(
                mergeNode.name(),
                pageDownstreamProjectorChain.v1(),
                streamers,
                ramAccountingContext,
                executionNodes.get(executionNodes.size() - 1).executionNodes().size()
        );
        FlatProjectorChain flatProjectorChain = pageDownstreamProjectorChain.v2();
        if (flatProjectorChain != null) {
            flatProjectorChain.startProjections(pageDownstreamContext);
        }
        return pageDownstreamContext;
    }

    private void sendJobRequests(Streamer<?>[] streamers,
                                 List<PageDownstreamContext> pageDownstreamContexts,
                                 Map<String, Collection<ExecutionNode>> nodesByServer) {
        int idx = 0;
        for (Map.Entry<String, Collection<ExecutionNode>> entry : nodesByServer.entrySet()) {
            String serverNodeId = entry.getKey();
            if (TableInfo.NULL_NODE_ID.equals(serverNodeId)) {
                continue; // handled by local node
            }
            Collection<ExecutionNode> executionNodes = entry.getValue();

            JobRequest request = new JobRequest(jobId(), executionNodes);
            if (hasDirectResponse) {
                transportJobAction.execute(serverNodeId, request,
                        new DirectResponseListener(idx, streamers, pageDownstreamContexts));
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
    private void createLocalContextAndStartOperation(PageDownstreamContext finalLocalMerge,
                                                     Map<String, Collection<ExecutionNode>> nodesByServer,
                                                     int localMergeExecutionNodeId) {
        String localNodeId = clusterService.localNode().id();
        Collection<ExecutionNode> localExecutionNodes = nodesByServer.remove(localNodeId);

        JobExecutionContext.Builder builder = jobContextService.newBuilder(jobId());
        builder.addSubContext(localMergeExecutionNodeId, finalLocalMerge);

        if (localExecutionNodes == null || localExecutionNodes.isEmpty()) {
            // only the local merge happens locally so it is enough to just create that context.
            jobContextService.createContext(builder);
        } else {
            for (ExecutionNode executionNode : localExecutionNodes) {
                contextPreparer.prepare(jobId(), executionNode, builder);
            }
            JobExecutionContext context = jobContextService.createContext(builder);
            context.start();
        }
    }

    private void addCloseContextCallback(TransportCloseContextNodeAction transportCloseContextNodeAction,
                                         final List<List<ExecutionNode>> groupedExecutionNodes,
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
                for (List<ExecutionNode> executionNodeGroup : groupedExecutionNodes) {
                    for (ExecutionNode executionNode : executionNodeGroup) {
                        contextCloser.process(executionNode, server);
                    }
                }
            }
        });
    }

    @Override
    public List<? extends ListenableFuture<TaskResult>> result() {
        if (results.size() != groupedExecutionNodes.size()) {
            for (int i = 0; i < groupedExecutionNodes.size(); i++) {
                results.add(SettableFuture.<TaskResult>create());
            }
        }
        return results;
    }

    @Override
    public void upstreamResult(List<? extends ListenableFuture<TaskResult>> result) {
        throw new UnsupportedOperationException("ExecutionNodesTask doesn't support upstreamResult");
    }

    static boolean hasDirectResponse(List<List<ExecutionNode>> groupedExecutionNodes) {
        for (List<ExecutionNode> executionNodeGroup : groupedExecutionNodes) {
            for (ExecutionNode executionNode : executionNodeGroup) {
                if (ExecutionNodes.hasDirectResponseDownstream(executionNode.downstreamNodes())) {
                    return true;
                }
            }
        }
        return false;
    }

    private static class DirectResponseListener implements ActionListener<JobResponse> {

        private final int bucketIdx;
        private final Streamer<?>[] streamer;
        private final List<PageDownstreamContext> pageDownstreamContexts;

        public DirectResponseListener(int bucketIdx, Streamer<?>[] streamer, List<PageDownstreamContext> pageDownstreamContexts) {
            this.bucketIdx = bucketIdx;
            this.streamer = streamer;
            this.pageDownstreamContexts = pageDownstreamContexts;
        }

        @Override
        public void onResponse(JobResponse jobResponse) {
            jobResponse.streamers(streamer);
            for (int i = 0; i < pageDownstreamContexts.size(); i++) {
                PageDownstreamContext pageDownstreamContext = pageDownstreamContexts.get(i);
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

            LOGGER.trace("closing job context {} on {} nodes", node.jobId(), nodeIds.size());
            for (final String nodeId : nodeIds) {
                transportCloseContextNodeAction.execute(
                        nodeId,
                        new NodeCloseContextRequest(node.jobId(), node.executionNodeId()),
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
