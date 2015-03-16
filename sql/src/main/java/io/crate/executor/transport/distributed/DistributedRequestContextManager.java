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

package io.crate.executor.transport.distributed;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.Streamer;
import io.crate.breaker.RamAccountingContext;
import io.crate.core.collections.Bucket;
import io.crate.executor.transport.merge.NodeMergeResponse;
import io.crate.metadata.Functions;
import io.crate.operation.DownstreamOperationFactory;
import io.crate.operation.collect.StatsTables;
import io.crate.planner.node.PlanNodeStreamerVisitor;
import io.crate.planner.node.dql.MergeNode;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.*;


/**
 * this class handles the contexts for each {@link io.crate.executor.transport.task.DistributedMergeTask}
 *
 * there is a possible race condition that the
 * {@link io.crate.executor.transport.merge.TransportMergeNodeAction} receives results from a collector
 * before the MergeTasks.start() method initialized the context.
 *
 * in case of this race condition the rows that are sent from the collector cannot be de-serialized immediately
 * so they are placed into a memoryStream and later read.
 *
 * this class is also responsible for this lazy-de-serialization.
 *
 * the merge itself is done inside {@link DownstreamOperationContext}
 */
public class DistributedRequestContextManager {

    private final ESLogger logger = Loggers.getLogger(getClass());

    private final Map<UUID, DownstreamOperationContext> activeMergeOperations = new HashMap<>();
    private final Map<UUID, List<DistributedResultRequest>> unprocessedRequests = new HashMap<>();
    private final Set<UUID> unprocessedFailureIds = new HashSet<>();
    private final Object lock = new Object();
    private final DownstreamOperationFactory downstreamOperationFactory;
    private final PlanNodeStreamerVisitor planNodeStreamerVisitor;
    private final StatsTables statsTables;
    private final CircuitBreaker circuitBreaker;

    public DistributedRequestContextManager(DownstreamOperationFactory downstreamOperationFactory,
                                            Functions functions,
                                            StatsTables statsTables,
                                            CircuitBreaker circuitBreaker) {
        this.downstreamOperationFactory = downstreamOperationFactory;
        this.statsTables = statsTables;
        this.circuitBreaker = circuitBreaker;
        this.planNodeStreamerVisitor = new PlanNodeStreamerVisitor(functions);
    }

    /**
     * called to create a new DownstreamOperationContext
     */
    // TODO: add outputTypes to the downstreamOperation and remove the mergeNode dependency here.
    // the downstreamOperationFactory can then be removed and the downstreamOperation can be passed into
    // createContext directly.
    public void createContext(final MergeNode mergeNode,
                              final ActionListener<NodeMergeResponse> listener) throws IOException {
        logger.trace("createContext: {}", mergeNode);
        final UUID operationId = UUID.randomUUID();
        String ramAccountingContextId = String.format("%s: %s", mergeNode.id(), operationId);
        final RamAccountingContext ramAccountingContext =
                new RamAccountingContext(ramAccountingContextId, circuitBreaker);
        statsTables.operationStarted(operationId, mergeNode.jobId(), mergeNode.id());
        PlanNodeStreamerVisitor.Context streamerContext = planNodeStreamerVisitor.process(mergeNode, ramAccountingContext);
        SettableFuture<Bucket> settableFuture = wrapActionListener(streamerContext.outputStreamers(), listener);
        DownstreamOperationContext downstreamOperationContext = new DownstreamOperationContext(
                downstreamOperationFactory.create(mergeNode, ramAccountingContext),
                settableFuture,
                streamerContext.inputStreamers(),
                new DoneCallback() {
                    @Override
                    public void finished() {
                        logger.trace("DoneCallback.finished: {} {}", mergeNode.jobId());
                        activeMergeOperations.remove(mergeNode.jobId());
                        statsTables.operationFinished(operationId, null, ramAccountingContext.totalBytes());
                        ramAccountingContext.close();
                    }
                }
        );
        logger.trace("createContext.put: {} {}", this, mergeNode.jobId(), downstreamOperationContext);
        put(mergeNode.jobId(), downstreamOperationContext);
    }


    /**
     * use to retrieve the streamers to read the incoming rows
     */
    public Optional<Streamer<?>[]> getStreamer(UUID contextId) {
        DownstreamOperationContext downstreamOperationContext = activeMergeOperations.get(contextId);
        if (downstreamOperationContext != null) {
            return Optional.of(downstreamOperationContext.streamers());
        }

        return Optional.absent();
    }

    /**
     * put the request either into the context of store into unprocessedRequests
     */
    public void addToContext(DistributedResultRequest request) throws Exception {
        synchronized (lock) {
            DownstreamOperationContext operationContext = activeMergeOperations.get(request.contextId());
            if (operationContext != null) {
                if (request.failure()) {
                    operationContext.addFailure(null);
                } else {
                    request.streamers(operationContext.streamers());
                    operationContext.add(request.rows());
                }
            } else {
                List<DistributedResultRequest> requests = unprocessedRequests.get(request.contextId());
                if (requests == null) {
                    requests = new ArrayList<>();
                    unprocessedRequests.put(request.contextId(), requests);
                }
                requests.add(request);
            }
        }
    }

    private SettableFuture<Bucket> wrapActionListener(final Streamer<?>[] streamers,
                                                      final ActionListener<NodeMergeResponse> listener) {
        SettableFuture<Bucket> settableFuture = SettableFuture.create();
        Futures.addCallback(settableFuture, new FutureCallback<Bucket>() {
            @Override
            public void onSuccess(Bucket result) {
                listener.onResponse(new NodeMergeResponse(streamers, result));
            }

            @Override
            public void onFailure(@Nonnull Throwable t) {
                listener.onFailure(t);
            }
        });
        return settableFuture;
    }

    private void put(UUID contextId, DownstreamOperationContext downstreamOperationContext) {
        List<DistributedResultRequest> requests;

        synchronized (lock) {
            logger.trace("put: {} {}", contextId, downstreamOperationContext);
            activeMergeOperations.put(contextId, downstreamOperationContext);
            requests = unprocessedRequests.remove(contextId);
            if (unprocessedFailureIds.remove(contextId)){
                downstreamOperationContext.addFailure(null);
            }
        }
        if (requests != null) {
            for (DistributedResultRequest request : requests) {
                request.streamers(downstreamOperationContext.streamers());
                downstreamOperationContext.add(request.rows());
            }
        }
    }

    public void setFailure(UUID contextId) {
        synchronized (lock) {
            DownstreamOperationContext downstreamOperationContext = activeMergeOperations.get(contextId);
            if (downstreamOperationContext == null) {
                unprocessedFailureIds.add(contextId);
            } else {
                downstreamOperationContext.addFailure(null);
            }
        }
    }

    public interface DoneCallback {
        public void finished();
    }
}
