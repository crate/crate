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
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.Streamer;
import io.crate.breaker.RamAccountingContext;
import io.crate.executor.transport.merge.NodeMergeResponse;
import io.crate.metadata.Functions;
import io.crate.operation.DownstreamOperation;
import io.crate.operation.DownstreamOperationFactory;
import io.crate.operation.collect.StatsTables;
import io.crate.operation.projectors.Projector;
import io.crate.planner.node.PlanNodeStreamerVisitor;
import io.crate.planner.node.dql.MergeNode;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.HandlesStreamInput;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
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
    private final Map<UUID, List<BytesReference>> unreadStreams = new HashMap<>();
    private final Set<UUID> unreadFailures = new HashSet<>();
    private final Set<UUID> contextFailures = new HashSet<>();
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
        logger.setLevel("trace");
    }

    /**
     * called to create a new DownstreamOperationContext
     *
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
        PlanNodeStreamerVisitor.Context streamerContext = planNodeStreamerVisitor.process(mergeNode, ramAccountingContext);
        SettableFuture<Object[][]> settableFuture = wrapActionListener(streamerContext.outputStreamers(), listener);

        DownstreamOperation downstreamOperation;
        DoneCallback doneCallback;
        try {
            downstreamOperation = downstreamOperationFactory.create(mergeNode, ramAccountingContext);
            doneCallback = new DoneCallback() {
                @Override
                public void finished() {
                    logger.trace("DoneCallback.finished: {} {}", mergeNode.contextId());
                    activeMergeOperations.remove(mergeNode.contextId());
                    statsTables.operationFinished(operationId, null, ramAccountingContext.totalBytes());
                    ramAccountingContext.close();
                }
            };
        } catch (final Exception e) {
            logger.error("Error while creating merge context: {}", e.getMessage());
            //contextFailures.add(mergeNode.contextId());
            //ramAccountingContext.close();
            //listener.onFailure(e);
            //return;
            downstreamOperation = new DownstreamOperation() {
                private SettableFuture<Object[][]> result = SettableFuture.create();
                private Projector downstream;

                @Override
                public boolean addRows(Object[][] rows) throws Exception {
                    logger.trace("addRows on dummy operation called");
                    return false;
                }

                @Override
                public int numUpstreams() {
                    return mergeNode.numUpstreams();
                }

                @Override
                public void finished() {
                    logger.trace("Finish on dummy operation called");
                    downstream.upstreamFinished();
                }

                @Override
                public ListenableFuture<Object[][]> result() {
                    logger.trace("result on dummy operation called");
                    return result;
                }

                @Override
                public void downstream(Projector downstream) {
                    downstream.registerUpstream(this);
                    this.downstream = downstream;
                }

                @Override
                public Projector downstream() {
                    logger.trace("downstream on dummy operation called");
                    return downstream;
                }
            };
            doneCallback = new DoneCallback() {
                @Override
                public void finished() {
                    logger.trace("DoneCallback.finished: {} {}", mergeNode.contextId());
                    activeMergeOperations.remove(mergeNode.contextId());
                    statsTables.operationFinished(operationId, e.getMessage(), ramAccountingContext.totalBytes());
                    ramAccountingContext.close();
                }
            };
        }

        DownstreamOperationContext downstreamOperationContext = new DownstreamOperationContext(
                downstreamOperation,
                settableFuture,
                streamerContext.inputStreamers(),
                doneCallback
        );

        statsTables.operationStarted(operationId, mergeNode.contextId(), mergeNode.id());
        logger.trace("createContext.put: {} {}", this, mergeNode.contextId(), downstreamOperationContext);
        put(mergeNode.contextId(), downstreamOperationContext);
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
     * merge to rows inside the request
     */
    public void addToContext(DistributedResultRequest request) throws Exception {
        logger.trace("addToContext: hasrows: {}", request.rowsRead());
        DownstreamOperationContext operationContext;
        if (request.rowsRead()) {
            operationContext = activeMergeOperations.get(request.contextId());
            assert operationContext != null;
            logger.trace("addToContext rowsRead: {}", operationContext);
            if (request.failure()) {
                operationContext.addFailure(null);
            } else {
                operationContext.add(request.rows());
            }
            logger.trace("addToContext rowsRead succes");
            return;
        }
        synchronized (lock) {
            operationContext = activeMergeOperations.get(request.contextId());
            logger.trace("addToContext: norows: operationContext: {} {} {}", this, request.contextId(), operationContext);
            if (operationContext == null) {
                logger.trace("addToContext: without context norows failure: {}", request.failure());
                assert !request.rowsRead();
                if (request.failure()) {
                    unreadFailures.add(request.contextId());
                    logger.error("adding unread failure from distributed result for context: ", request.contextId());
                } else {
                    assert request.memoryStream() != null;
                    List<BytesReference> bytesStreamOutputs = unreadStreams.get(request.contextId());
                    if (bytesStreamOutputs == null) {
                        bytesStreamOutputs = new ArrayList<>();
                        unreadStreams.put(request.contextId(), bytesStreamOutputs);
                    }
                    bytesStreamOutputs.add(request.memoryStream().bytes());
                }
            } else {
                logger.trace("addToContext: with context norows failure: {}", request.failure());
                if (request.failure()) {
                    operationContext.addFailure(null);
                    logger.error("addToContext: failure in distributed result");
                    return;
                }
                logger.trace("addToContext: using memory stream: ", request.memoryStream());
                addFromBytesReference(request.memoryStream().bytes(), operationContext);
            }
        }
        logger.trace("addToContext: finished");
    }

    private SettableFuture<Object[][]> wrapActionListener(final Streamer<?>[] streamers,
                                                          final ActionListener<NodeMergeResponse> listener) {
        SettableFuture<Object[][]> settableFuture = SettableFuture.create();
        Futures.addCallback(settableFuture, new FutureCallback<Object[][]>() {
            @Override
            public void onSuccess(@Nullable Object[][] result) {
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
        List<BytesReference> bytesReferences;
        synchronized (lock) {
            logger.trace("put: {} {}", contextId, downstreamOperationContext);
            activeMergeOperations.put(contextId, downstreamOperationContext);
            bytesReferences = unreadStreams.remove(contextId);
            if (unreadFailures.contains(contextId)) {
                unreadFailures.remove(contextId);
                downstreamOperationContext.addFailure(null);
            }
        }
        if (bytesReferences != null) {
            for (BytesReference bytes : bytesReferences) {
                addFromBytesReference(bytes, downstreamOperationContext);
            }
        }
    }

    private void addFromBytesReference(BytesReference bytesReference, DownstreamOperationContext ctx) {
        // bytesReference must be wrapped into HandlesStreamInput because it has a different readString()
        // implementation than BytesStreamInput alone.
        // and the memoryOutputStream originates from a HandlesStreamOutput.
        HandlesStreamInput wrappedStream = new HandlesStreamInput(new BytesStreamInput(bytesReference));
        Object[][] rows = null;
        try {
            rows = DistributedResultRequest.readRemaining(ctx.streamers(), wrappedStream);
        } catch (IOException e) {
            ctx.addFailure(e);
            logger.error("unable to deserialize upstream result", e);
            return;
        }
        assert rows != null;
        ctx.add(rows);
    }

    public void setFailure(UUID contextId) {
        synchronized (lock) {
            DownstreamOperationContext downstreamOperationContext = activeMergeOperations.get(contextId);
            if (downstreamOperationContext == null) {
                unreadFailures.add(contextId);
            } else {
                downstreamOperationContext.addFailure(null);
            }
        }
    }

    public interface DoneCallback {
        public void finished();
    }
}
