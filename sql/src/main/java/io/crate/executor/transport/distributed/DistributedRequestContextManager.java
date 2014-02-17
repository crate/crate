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
import io.crate.executor.transport.merge.NodeMergeResponse;
import io.crate.metadata.Functions;
import io.crate.operator.operations.DownstreamOperationFactory;
import io.crate.planner.node.MergeNode;
import io.crate.planner.node.PlanNodeStreamerVisitor;
import org.cratedb.DataType;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.HandlesStreamInput;

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

    private final Map<UUID, DownstreamOperationContext> activeMergeOperations = new HashMap<>();
    private final Map<UUID, List<BytesReference>> unreadStreams = new HashMap<>();
    private final Set<UUID> unreadFailures = new HashSet<>();
    private final Object lock = new Object();
    private final DownstreamOperationFactory downstreamOperationFactory;
    private final PlanNodeStreamerVisitor planNodeStreamerVisitor;

    public DistributedRequestContextManager(DownstreamOperationFactory downstreamOperationFactory,
                                            Functions functions) {
        this.downstreamOperationFactory = downstreamOperationFactory;
        this.planNodeStreamerVisitor = new PlanNodeStreamerVisitor(functions);
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
        PlanNodeStreamerVisitor.Context streamerContext = planNodeStreamerVisitor.process(mergeNode);
        SettableFuture<Object[][]> settableFuture = wrapActionListener(streamerContext.outputStreamers(), listener);
        DownstreamOperationContext downstreamOperationContext = new DownstreamOperationContext(
                downstreamOperationFactory.create(mergeNode),
                settableFuture,
                streamerContext.inputStreamers(),
                new DoneCallback() {
                    @Override
                    public void finished() {
                        activeMergeOperations.remove(mergeNode.contextId());
                    }
                }
        );

        put(mergeNode.contextId(), downstreamOperationContext);
    }


    /**
     * use to retrieve the streamers to read the incoming rows
     */
    public Optional<DataType.Streamer<?>[]> getStreamer(UUID contextId) {
        DownstreamOperationContext downstreamOperationContext = activeMergeOperations.get(contextId);
        if (downstreamOperationContext != null) {
            return Optional.of(downstreamOperationContext.streamers());
        }

        return Optional.absent();
    }

    /**
     * merge to rows inside the request
     */
    public void addToContext(DistributedResultRequest request) throws IOException {
        DownstreamOperationContext operationContext;
        if (request.rowsRead()) {
            operationContext = activeMergeOperations.get(request.contextId());
            if (request.failure()) {
                operationContext.addFailure();
            } else {
                operationContext.add(request.rows());
            }
            return;
        }

        synchronized (lock) {
            operationContext = activeMergeOperations.get(request.contextId());

            if (operationContext == null) {
                if (request.failure()) {
                    unreadFailures.add(request.contextId());
                }

                assert !request.rowsRead() && request.memoryStream() != null;

                List<BytesReference> bytesStreamOutputs = unreadStreams.get(request.contextId());
                if (bytesStreamOutputs == null) {
                    bytesStreamOutputs = new ArrayList<>();
                    unreadStreams.put(request.contextId(), bytesStreamOutputs);
                }
                bytesStreamOutputs.add(request.memoryStream().bytes());
            } else {
                if (request.failure()) {
                    operationContext.addFailure();
                    return;
                }
                mergeFromBytesReference(request.memoryStream().bytes(), operationContext);
            }
        }
    }

    private SettableFuture<Object[][]> wrapActionListener(final DataType.Streamer<?>[] streamers,
                                                          final ActionListener<NodeMergeResponse> listener) {
        SettableFuture<Object[][]> settableFuture = SettableFuture.create();
        Futures.addCallback(settableFuture, new FutureCallback<Object[][]>() {
            @Override
            public void onSuccess(@Nullable Object[][] result) {
                listener.onResponse(new NodeMergeResponse(streamers, result));
            }

            @Override
            public void onFailure(Throwable t) {
                listener.onFailure(t);
            }
        });
        return settableFuture;
    }

    private void put(UUID contextId, DownstreamOperationContext downstreamOperationContext) throws IOException {
        List<BytesReference> bytesReferences;
        synchronized (lock) {
            activeMergeOperations.put(contextId, downstreamOperationContext);
            bytesReferences = unreadStreams.get(contextId);
            unreadStreams.remove(contextId);
            if (unreadFailures.contains(contextId)) {
                downstreamOperationContext.addFailure();
            }
            unreadFailures.remove(contextId);
        }

        if (bytesReferences != null) {
            for (BytesReference bytes : bytesReferences) {
                mergeFromBytesReference(bytes, downstreamOperationContext);
            }
        }
    }

    private void mergeFromBytesReference(BytesReference bytesReference, DownstreamOperationContext mergeOperationCtx) throws IOException {
        // bytesReference must be wrapped into HandlesStreamInput because it has a different readString()
        // implementation than BytesStreamInput alone.
        // and the memoryOutputStream originates from a HandlesStreamOutput.
        HandlesStreamInput wrappedStream = new HandlesStreamInput(new BytesStreamInput(bytesReference));
        mergeOperationCtx.add(
                DistributedResultRequest.readRemaining(mergeOperationCtx.streamers(), wrappedStream));
    }

    public interface DoneCallback {
        public void finished();
    }
}
