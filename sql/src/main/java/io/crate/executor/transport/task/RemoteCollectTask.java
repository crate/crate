/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.executor.transport.task;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
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
import io.crate.exceptions.Exceptions;
import io.crate.executor.JobTask;
import io.crate.executor.QueryResult;
import io.crate.executor.TaskResult;
import io.crate.executor.transport.NodeCloseContextRequest;
import io.crate.executor.transport.NodeCloseContextResponse;
import io.crate.executor.transport.TransportCloseContextNodeAction;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.RowDownstream;
import io.crate.operation.collect.HandlerSideDataCollectOperation;
import io.crate.operation.collect.JobCollectContext;
import io.crate.operation.collect.StatsTables;
import io.crate.operation.projectors.FlatProjectorChain;
import io.crate.operation.projectors.ProjectionToProjectorVisitor;
import io.crate.operation.projectors.ResultProvider;
import io.crate.planner.node.ExecutionNode;
import io.crate.planner.node.StreamerVisitor;
import io.crate.planner.node.dql.CollectNode;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.collect.Lists.newArrayList;

public class RemoteCollectTask extends JobTask {

    private final CollectNode collectNode;
    private final List<ListenableFuture<TaskResult>> result;
    private final List<String> nodeIds;
    private final TransportJobAction transportJobAction;
    private final TransportCloseContextNodeAction transportCloseContextNodeAction;
    private final HandlerSideDataCollectOperation handlerSideDataCollectOperation;
    private final StatsTables statsTables;
    private final CircuitBreaker circuitBreaker;
    private final AtomicInteger nodeResponses = new AtomicInteger(0);
    private final AtomicBoolean someNodeHasFailures = new AtomicBoolean(false);
    private final ProjectionToProjectorVisitor clusterProjectorVisitor;
    private final Streamer<?>[] streamers;

    private static final ESLogger LOGGER = Loggers.getLogger(RemoteCollectTask.class);

    public RemoteCollectTask(UUID jobId,
                             CollectNode collectNode,
                             TransportJobAction transportJobAction,
                             TransportCloseContextNodeAction transportCloseContextNodeAction,
                             StreamerVisitor streamerVisitor,
                             HandlerSideDataCollectOperation handlerSideDataCollectOperation,
                             ProjectionToProjectorVisitor clusterProjectionToProjectorVisitor,
                             StatsTables statsTables,
                             CircuitBreaker circuitBreaker) {
        super(jobId);
        this.collectNode = collectNode;
        this.transportJobAction = transportJobAction;
        this.transportCloseContextNodeAction = transportCloseContextNodeAction;
        this.streamers = streamerVisitor.processExecutionNode(collectNode, null).outputStreamers();
        this.handlerSideDataCollectOperation = handlerSideDataCollectOperation;
        this.statsTables = statsTables;
        this.circuitBreaker = circuitBreaker;
        this.clusterProjectorVisitor = clusterProjectionToProjectorVisitor;

        Preconditions.checkArgument(collectNode.isRouted(),
                "RemoteCollectTask currently only works for plans with routing"
        );

        Preconditions.checkArgument(collectNode.routing().hasLocations(), "RemoteCollectTask does not need to be executed.");


        int resultSize = collectNode.routing().nodes().size();
        nodeIds = newArrayList(collectNode.routing().nodes());
        result = new ArrayList<>(resultSize);
        for (int i = 0; i < resultSize; i++) {
            result.add(SettableFuture.<TaskResult>create());
        }
    }

    @Override
    public void start() {
        final JobRequest jobRequest = new JobRequest(
                jobId(),
                ImmutableList.<ExecutionNode>of(collectNode)
        );
        for (int i = 0; i < nodeIds.size(); i++) {
            final int resultIdx = i;
            String nodeId = nodeIds.get(i);
            if (nodeId.equals(TableInfo.NULL_NODE_ID)) {
                handlerSideCollect(resultIdx);
                continue;
            }
            nodeResponses.incrementAndGet();
            transportJobAction.execute(nodeId, jobRequest, new ActionListener<JobResponse>() {
                @Override
                public void onResponse(JobResponse jobResponse) {
                    SettableFuture<TaskResult> future = ((SettableFuture<TaskResult>) result.get(resultIdx));
                    jobResponse.streamers(streamers);
                    if (jobResponse.directResponse().isPresent()) {
                        future.set(new QueryResult(jobResponse.directResponse().get()));
                    } else {
                        future.set(TaskResult.EMPTY_RESULT);
                    }
                    if (nodeResponses.decrementAndGet() == 0 && someNodeHasFailures.get()) {
                        freeAllContexts();
                    }
                }

                @Override
                public void onFailure(Throwable e) {
                    ((SettableFuture<TaskResult>) result.get(resultIdx)).setException(e);
                    someNodeHasFailures.set(true);
                    if (nodeResponses.decrementAndGet() == 0) {
                        freeAllContexts();
                    }
                }
            });
        }
    }

    private void handlerSideCollect(final int resultIdx) {
        statsTables.operationStarted(collectNode.executionNodeId(), jobId(), collectNode.name());
        final RamAccountingContext ramAccountingContext =
                RamAccountingContext.forExecutionNode(circuitBreaker, collectNode);

        FlatProjectorChain projectorChain = FlatProjectorChain.withResultProvider(
                clusterProjectorVisitor,
                ramAccountingContext,
                collectNode.projections()
        );
        RowDownstream rowDownstream = projectorChain.firstProjector();
        ResultProvider resultProvider = projectorChain.resultProvider();
        assert resultProvider != null;
        projectorChain.startProjections();


        JobCollectContext jobCollectContext = new JobCollectContext(
                handlerSideDataCollectOperation,
                jobId(),
                collectNode,
                rowDownstream,
                ramAccountingContext
        );
        jobCollectContext.start();

        Futures.addCallback(resultProvider.result(), new FutureCallback<Bucket>() {
            @Override
            public void onSuccess(Bucket rows) {
                ramAccountingContext.close();
                ((SettableFuture<TaskResult>) result.get(resultIdx)).set(new QueryResult(rows));
                statsTables.operationFinished(collectNode.executionNodeId(), null, ramAccountingContext.totalBytes());
            }

            @Override
            public void onFailure(@Nonnull Throwable t) {
                ramAccountingContext.close();
                ((SettableFuture<TaskResult>) result.get(resultIdx)).setException(t);
                statsTables.operationFinished(collectNode.executionNodeId(), Exceptions.messageOf(t),
                        ramAccountingContext.totalBytes());
            }
        });
    }

    @Override
    public List<ListenableFuture<TaskResult>> result() {
        return result;
    }

    @Override
    public void upstreamResult(List<ListenableFuture<TaskResult>> result) {
        throw new UnsupportedOperationException("RemoteCollectTask does not support upstream results");
    }

    public void freeAllContexts() {
        if (collectNode.keepContextForFetcher()) {
            LOGGER.trace("closing job context {} on {} nodes", collectNode.jobId().get(), nodeIds.size());
            for (final String nodeId : nodeIds) {
                transportCloseContextNodeAction.execute(nodeId,
                        new NodeCloseContextRequest(collectNode.jobId().get(), collectNode.executionNodeId()), new ActionListener<NodeCloseContextResponse>() {
                    @Override
                    public void onResponse(NodeCloseContextResponse nodeCloseContextResponse) {
                    }

                    @Override
                    public void onFailure(Throwable e) {
                        LOGGER.warn("Closing job context {} failed on node {} with: {}", collectNode.jobId().get(), nodeId, e.getMessage());
                    }
                });
            }

        }
    }
}
