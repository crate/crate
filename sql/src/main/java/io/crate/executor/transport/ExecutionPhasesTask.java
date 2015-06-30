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
import io.crate.planner.node.ExecutionPhase;
import io.crate.planner.node.ExecutionPhaseGrouper;
import io.crate.planner.node.ExecutionPhases;
import io.crate.planner.node.dql.MergePhase;
import io.crate.types.DataTypes;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.*;


public class ExecutionPhasesTask extends JobTask {

    private static final ESLogger LOGGER = Loggers.getLogger(ExecutionPhasesTask.class);

    private final TransportJobAction transportJobAction;
    private final ClusterService clusterService;
    private ContextPreparer contextPreparer;
    private final JobContextService jobContextService;
    private final PageDownstreamFactory pageDownstreamFactory;
    private final ThreadPool threadPool;
    private final CircuitBreaker circuitBreaker;

    private final List<List<ExecutionPhase>> groupedExecutionPhases = new ArrayList<>();
    private final List<MergePhase> finalMergeNodes = new ArrayList<>();
    private final List<SettableFuture<TaskResult>> results = new ArrayList<>();
    private boolean hasDirectResponse;
    private boolean rowCountResult = false;

    protected ExecutionPhasesTask(UUID jobId,
                                  ClusterService clusterService,
                                  ContextPreparer contextPreparer,
                                  JobContextService jobContextService,
                                  PageDownstreamFactory pageDownstreamFactory,
                                  ThreadPool threadPool,
                                  TransportJobAction transportJobAction,
                                  CircuitBreaker circuitBreaker) {
        super(jobId);
        this.clusterService = clusterService;
        this.contextPreparer = contextPreparer;
        this.jobContextService = jobContextService;
        this.pageDownstreamFactory = pageDownstreamFactory;
        this.threadPool = threadPool;
        this.circuitBreaker = circuitBreaker;
        this.transportJobAction = transportJobAction;
    }


    /**
     * @param finalMergeNode a mergeNode for the final merge operation on the handler.
     *                   Multiple merge nodes are only occurring on bulk operations.
     */
    public void addFinalMergeNode(MergePhase finalMergeNode) {
        finalMergeNodes.add(finalMergeNode);
    }

    public void addExecutionPhase(int group, ExecutionPhase executionPhase) {
        while (group >= groupedExecutionPhases.size()) {
            results.add(SettableFuture.<TaskResult>create());
            groupedExecutionPhases.add(new ArrayList<ExecutionPhase>());
        }
        List<ExecutionPhase> executionPhases = groupedExecutionPhases.get(group);

        if (ExecutionPhases.hasDirectResponseDownstream(executionPhase.downstreamNodes())) {
            hasDirectResponse = true;
        }
        executionPhases.add(executionPhase);
    }

    public void rowCountResult(boolean rowCountResult) {
        this.rowCountResult = rowCountResult;
    }

    @Override
    public void start() {
        assert finalMergeNodes.size() == groupedExecutionPhases.size() : "groupedExecutionPhases and finalMergeNodes sizes must match";

        Map<String, Collection<ExecutionPhase>> nodesByServer = ExecutionPhaseGrouper.groupByServer(clusterService.state().nodes().localNodeId(), groupedExecutionPhases);
        RowDownstream rowDownstream;
        if (rowCountResult) {
            rowDownstream = new RowCountResultRowDownstream(results);
        } else {
            rowDownstream = new QueryResultRowDownstream(results);
        }
        Streamer<?>[] streamers = DataTypes.getStreamer(finalMergeNodes.get(0).inputTypes());
        List<PageDownstreamContext> pageDownstreamContexts = new ArrayList<>(groupedExecutionPhases.size());

        for (int i = 0; i < groupedExecutionPhases.size(); i++) {
            RamAccountingContext ramAccountingContext = RamAccountingContext.forExecutionPhase(
                    circuitBreaker, finalMergeNodes.get(i));

            PageDownstreamContext pageDownstreamContext = createPageDownstreamContext(ramAccountingContext, streamers,
                    finalMergeNodes.get(i), groupedExecutionPhases.get(i), rowDownstream);
            if (nodesByServer.size() == 0) {
                pageDownstreamContext.finish();
                continue;
            }
            if (!hasDirectResponse) {
                createLocalContextAndStartOperation(pageDownstreamContext, nodesByServer, finalMergeNodes.get(i).executionPhaseId());
            } else {
                pageDownstreamContext.start();
            }
            pageDownstreamContexts.add(pageDownstreamContext);
        }
        if (nodesByServer.size() == 0) {
            return;
        }
        sendJobRequests(streamers, pageDownstreamContexts, nodesByServer);
    }

    private PageDownstreamContext createPageDownstreamContext(
            RamAccountingContext ramAccountingContext,
            Streamer<?>[] streamers,
            MergePhase mergeNode,
            List<ExecutionPhase> executionPhases,
            RowDownstream rowDownstream) {
        Tuple<PageDownstream, FlatProjectorChain> pageDownstreamProjectorChain = pageDownstreamFactory.createMergeNodePageDownstream(
                mergeNode,
                rowDownstream,
                ramAccountingContext,
                Optional.of(threadPool.executor(ThreadPool.Names.SEARCH))
        );
        return new PageDownstreamContext(
                mergeNode.name(),
                pageDownstreamProjectorChain.v1(),
                streamers,
                ramAccountingContext,
                executionPhases.get(executionPhases.size() - 1).executionNodes().size(),
                pageDownstreamProjectorChain.v2()
        );
    }

    private void sendJobRequests(Streamer<?>[] streamers,
                                 List<PageDownstreamContext> pageDownstreamContexts,
                                 Map<String, Collection<ExecutionPhase>> nodesByServer) {
        int idx = 0;
        for (Map.Entry<String, Collection<ExecutionPhase>> entry : nodesByServer.entrySet()) {
            String serverNodeId = entry.getKey();
            if (TableInfo.NULL_NODE_ID.equals(serverNodeId)) {
                continue; // handled by local node
            }
            Collection<ExecutionPhase> executionPhases = entry.getValue();

            JobRequest request = new JobRequest(jobId(), executionPhases);
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
                                                     Map<String, Collection<ExecutionPhase>> nodesByServer,
                                                     int localMergeExecutionNodeId) {
        String localNodeId = clusterService.localNode().id();
        Collection<ExecutionPhase> localExecutionPhases = nodesByServer.remove(localNodeId);

        JobExecutionContext.Builder builder = jobContextService.newBuilder(jobId());
        builder.addSubContext(localMergeExecutionNodeId, finalLocalMerge);

        if (localExecutionPhases != null) {
            for (ExecutionPhase executionPhase : localExecutionPhases) {
                contextPreparer.prepare(jobId(), executionPhase, builder);
            }
        }
        JobExecutionContext context = jobContextService.createContext(builder);
        context.start();
    }

    @Override
    public List<? extends ListenableFuture<TaskResult>> result() {
        if (results.size() != groupedExecutionPhases.size()) {
            for (int i = 0; i < groupedExecutionPhases.size(); i++) {
                results.add(SettableFuture.<TaskResult>create());
            }
        }
        return results;
    }

    @Override
    public void upstreamResult(List<? extends ListenableFuture<TaskResult>> result) {
        throw new UnsupportedOperationException("ExecutionNodesTask doesn't support upstreamResult");
    }

    static boolean hasDirectResponse(List<List<ExecutionPhase>> groupedExecutionNodes) {
        for (List<ExecutionPhase> executionPhaseGroup : groupedExecutionNodes) {
            for (ExecutionPhase executionPhase : executionPhaseGroup) {
                if (ExecutionPhases.hasDirectResponseDownstream(executionPhase.downstreamNodes())) {
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
}
