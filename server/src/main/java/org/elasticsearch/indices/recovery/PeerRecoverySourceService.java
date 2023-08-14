/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.indices.recovery;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportService;
import org.jetbrains.annotations.Nullable;

import io.crate.blob.BlobTransferTarget;
import io.crate.blob.recovery.BlobRecoveryHandler;
import io.crate.blob.v2.BlobIndex;
import io.crate.blob.v2.BlobIndicesService;

/**
 * The source recovery accepts recovery requests from other peer shards and start the recovery process from this
 * source shard to the target shard.
 */
public class PeerRecoverySourceService extends AbstractLifecycleComponent implements IndexEventListener, ClusterStateListener {

    private static final Logger LOGGER = LogManager.getLogger(PeerRecoverySourceService.class);

    public static class Actions {
        public static final String START_RECOVERY = "internal:index/shard/recovery/start_recovery";
        public static final String REESTABLISH_RECOVERY = "internal:index/shard/recovery/reestablish_recovery";
    }

    private final TransportService transportService;
    private final IndicesService indicesService;
    private final ClusterService clusterService;
    private final RecoverySettings recoverySettings;
    private final BlobTransferTarget blobTransferTarget;
    private final BlobIndicesService blobIndicesService;

    final OngoingRecoveries ongoingRecoveries = new OngoingRecoveries();

    public PeerRecoverySourceService(TransportService transportService,
                                     IndicesService indicesService,
                                     ClusterService clusterService,
                                     RecoverySettings recoverySettings,
                                     BlobTransferTarget blobTransferTarget,
                                     BlobIndicesService blobIndicesService) {
        this.transportService = transportService;
        this.indicesService = indicesService;
        this.clusterService = clusterService;
        this.recoverySettings = recoverySettings;
        this.blobTransferTarget = blobTransferTarget;
        this.blobIndicesService = blobIndicesService;
        // When the target node wants to start a peer recovery it sends a START_RECOVERY request to the source
        // node. Upon receiving START_RECOVERY, the source node will initiate the peer recovery.
        transportService.registerRequestHandler(Actions.START_RECOVERY, ThreadPool.Names.GENERIC, StartRecoveryRequest::new,
            new StartRecoveryTransportRequestHandler());
        // When the target node's START_RECOVERY request has failed due to a network disconnection, it will
        // send a REESTABLISH_RECOVERY. This attempts to reconnect to an existing recovery process taking
        // place on the source node. If the recovery process no longer exists, then the REESTABLISH_RECOVERY
        // action will fail and the target node will send a new START_RECOVERY request.
        transportService.registerRequestHandler(Actions.REESTABLISH_RECOVERY, ThreadPool.Names.GENERIC, ReestablishRecoveryRequest::new,
            new ReestablishRecoveryTransportRequestHandler());
    }

    @Override
    protected void doStart() {
        if (DiscoveryNode.isDataNode(clusterService.getSettings())) {
            clusterService.addListener(this);
        }
    }

    @Override
    protected void doStop() {
        if (DiscoveryNode.isDataNode(clusterService.getSettings())) {
            ongoingRecoveries.awaitEmpty();
            clusterService.removeListener(this);
        }
    }

    @Override
    protected void doClose() {
    }

    @Override
    public void beforeIndexShardClosed(ShardId shardId, @Nullable IndexShard indexShard,
                                       Settings indexSettings) {
        if (indexShard != null) {
            ongoingRecoveries.cancel(indexShard, "shard is closed");
        }
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.nodesRemoved()) {
            for (DiscoveryNode removedNode : event.nodesDelta().removedNodes()) {
                ongoingRecoveries.cancelOnNodeLeft(removedNode);
            }
        }
    }

    private void recover(StartRecoveryRequest request, ActionListener<RecoveryResponse> listener) {
        final IndexService indexService = indicesService.indexServiceSafe(request.shardId().getIndex());
        final IndexShard shard = indexService.getShard(request.shardId().id());

        final ShardRouting routingEntry = shard.routingEntry();

        if (routingEntry.primary() == false || routingEntry.active() == false) {
            throw new DelayRecoveryException("source shard [" + routingEntry + "] is not an active primary");
        }

        if (request.isPrimaryRelocation()
            && (
                routingEntry.relocating() == false
                || routingEntry.relocatingNodeId().equals(request.targetNode().getId()) == false)) {
            LOGGER.debug(
                "delaying recovery of {} as source shard is not marked yet as relocating to {}",
                request.shardId(), request.targetNode());
            throw new DelayRecoveryException("source shard is not marked yet as relocating to [" + request.targetNode() + "]");
        }

        RecoverySourceHandler handler = ongoingRecoveries.addNewRecovery(request, shard);
        LOGGER.trace(
            "[{}][{}] starting recovery to {}",
            request.shardId().getIndex().getName(), request.shardId().id(), request.targetNode());
        handler.recoverToTarget(ActionListener.runAfter(listener, () -> ongoingRecoveries.remove(shard, handler)));
    }

    private void reestablish(ReestablishRecoveryRequest request, ActionListener<RecoveryResponse> listener) {
        final IndexService indexService = indicesService.indexServiceSafe(request.shardId().getIndex());
        final IndexShard shard = indexService.getShard(request.shardId().id());

        LOGGER.trace("[{}][{}] reestablishing recovery {}", request.shardId().getIndex().getName(), request.shardId().id(),
            request.recoveryId());
        ongoingRecoveries.reestablishRecovery(request, shard, listener);
    }

    class StartRecoveryTransportRequestHandler implements TransportRequestHandler<StartRecoveryRequest> {

        @Override
        public void messageReceived(final StartRecoveryRequest request, final TransportChannel channel) throws Exception {
            recover(request, new ChannelActionListener<>(channel, Actions.START_RECOVERY, request));
        }
    }

    class ReestablishRecoveryTransportRequestHandler implements TransportRequestHandler<ReestablishRecoveryRequest> {
        @Override
        public void messageReceived(ReestablishRecoveryRequest request, TransportChannel channel) throws Exception {
            reestablish(request, new ChannelActionListener<>(channel, Actions.REESTABLISH_RECOVERY, request));
        }
    }

    // exposed for testing
    final int numberOfOngoingRecoveries() {
        return ongoingRecoveries.ongoingRecoveries.size();
    }

    record RecoveryHandlers(RecoverySourceHandler sourceHandler, RemoteRecoveryTargetHandler remoteHandler) {}

    final class OngoingRecoveries {

        private final Map<IndexShard, ShardRecoveryContext> ongoingRecoveries = new HashMap<>();

        private final Map<DiscoveryNode, Collection<RemoteRecoveryTargetHandler>> nodeToHandlers = new HashMap<>();

        @Nullable
        private List<ActionListener<Void>> emptyListeners;

        synchronized RecoverySourceHandler addNewRecovery(StartRecoveryRequest request, IndexShard shard) {
            assert lifecycle.started();
            final ShardRecoveryContext shardContext = ongoingRecoveries.computeIfAbsent(shard, s -> new ShardRecoveryContext());
            final RecoveryHandlers handlers = shardContext.addNewRecovery(request, shard);
            final RemoteRecoveryTargetHandler recoveryTargetHandler = handlers.remoteHandler();
            nodeToHandlers.computeIfAbsent(recoveryTargetHandler.targetNode(), k -> new HashSet<>()).add(recoveryTargetHandler);
            shard.recoveryStats().incCurrentAsSource();
            return handlers.sourceHandler();
        }

        synchronized void cancelOnNodeLeft(DiscoveryNode node) {
            final Collection<RemoteRecoveryTargetHandler> handlers = nodeToHandlers.get(node);
            if (handlers != null) {
                for (RemoteRecoveryTargetHandler handler : handlers) {
                    handler.cancel();
                }
            }
        }

        synchronized void reestablishRecovery(ReestablishRecoveryRequest request, IndexShard shard,
                                              ActionListener<RecoveryResponse> listener) {
            assert lifecycle.started();
            final ShardRecoveryContext shardContext = ongoingRecoveries.get(shard);
            if (shardContext == null) {
                throw new PeerRecoveryNotFound(request.recoveryId(), request.shardId(), request.targetAllocationId());
            }
            shardContext.reestablishRecovery(request, listener);
        }

        synchronized void remove(IndexShard shard, RecoverySourceHandler handler) {
            final ShardRecoveryContext shardRecoveryContext = ongoingRecoveries.get(shard);
            assert shardRecoveryContext != null : "Shard was not registered [" + shard + "]";
            final RemoteRecoveryTargetHandler removed = shardRecoveryContext.recoveryHandlers.remove(handler);
            assert removed != null : "Handler was not registered [" + handler + "]";
            if (removed != null) {
                shard.recoveryStats().decCurrentAsSource();
                removed.cancel();
                assert nodeToHandlers.getOrDefault(removed.targetNode(), Collections.emptySet()).contains(removed)
                        : "Remote recovery was not properly tracked [" + removed + "]";
                nodeToHandlers.computeIfPresent(removed.targetNode(), (k, handlersForNode) -> {
                    handlersForNode.remove(removed);
                    if (handlersForNode.isEmpty()) {
                        return null;
                    }
                    return handlersForNode;
                });
            }
            if (shardRecoveryContext.recoveryHandlers.isEmpty()) {
                ongoingRecoveries.remove(shard);
            }
            if (ongoingRecoveries.isEmpty()) {
                if (emptyListeners != null) {
                    final List<ActionListener<Void>> onEmptyListeners = emptyListeners;
                    emptyListeners = null;
                    ActionListener.onResponse(onEmptyListeners, null);
                }
            }
        }

        synchronized void cancel(IndexShard shard, String reason) {
            final ShardRecoveryContext shardRecoveryContext = ongoingRecoveries.get(shard);
            if (shardRecoveryContext != null) {
                final List<Exception> failures = new ArrayList<>();
                for (RecoverySourceHandler handlers : shardRecoveryContext.recoveryHandlers.keySet()) {
                    try {
                        handlers.cancel(reason);
                    } catch (Exception ex) {
                        failures.add(ex);
                    } finally {
                        shard.recoveryStats().decCurrentAsSource();
                    }
                }
                ExceptionsHelper.maybeThrowRuntimeAndSuppress(failures);
            }
        }

        void awaitEmpty() {
            assert lifecycle.stoppedOrClosed();
            final PlainActionFuture<Void> future;
            synchronized (this) {
                if (ongoingRecoveries.isEmpty()) {
                    return;
                }
                future = new PlainActionFuture<>();
                if (emptyListeners == null) {
                    emptyListeners = new ArrayList<>();
                }
                emptyListeners.add(future);
            }
            FutureUtils.get(future);
        }

        private final class ShardRecoveryContext {
            final Map<RecoverySourceHandler, RemoteRecoveryTargetHandler> recoveryHandlers = new HashMap<>();

            /**
             * Adds recovery source handler.
             */
            synchronized RecoveryHandlers addNewRecovery(StartRecoveryRequest request, IndexShard shard) {
                for (RecoverySourceHandler existingHandler : recoveryHandlers.keySet()) {
                    if (existingHandler.getRequest().targetAllocationId().equals(request.targetAllocationId())) {
                        throw new DelayRecoveryException("recovery with same target already registered, waiting for " +
                            "previous recovery attempt to be cancelled or completed");
                    }
                }
                RecoveryHandlers handlers = createRecoverySourceHandler(request, shard);
                recoveryHandlers.put(handlers.sourceHandler(), handlers.remoteHandler());
                return handlers;
            }

            /**
             * Adds recovery source handler.
             */
            synchronized void reestablishRecovery(ReestablishRecoveryRequest request, ActionListener<RecoveryResponse> listener) {
                RecoverySourceHandler handler = null;
                for (RecoverySourceHandler existingHandler : recoveryHandlers.keySet()) {
                    if (existingHandler.getRequest().recoveryId() == request.recoveryId() &&
                        existingHandler.getRequest().targetAllocationId().equals(request.targetAllocationId())) {
                        handler = existingHandler;
                        break;
                    }
                }
                if (handler == null) {
                    throw new ResourceNotFoundException("Cannot reestablish recovery, recovery id [" + request.recoveryId()
                        + "] not found.");
                }
                handler.addListener(listener);
            }

            private RecoveryHandlers createRecoverySourceHandler(StartRecoveryRequest request, IndexShard shard) {
                final RemoteRecoveryTargetHandler recoveryTarget = new RemoteRecoveryTargetHandler(
                    request.recoveryId(),
                    request.shardId(),
                    transportService,
                    request.targetNode(),
                    recoverySettings,
                    throttleTime -> shard.recoveryStats().addThrottleTime(throttleTime));

                int recoveryChunkSizeInBytes = recoverySettings.getChunkSize().bytesAsInt();
                RecoverySourceHandler handler;
                if (BlobIndex.isBlobIndex(shard.shardId().getIndexName())) {
                    handler = new BlobRecoveryHandler(
                        shard,
                        recoveryTarget,
                        request,
                        recoveryChunkSizeInBytes,
                        recoverySettings.getMaxConcurrentFileChunks(),
                        recoverySettings.getMaxConcurrentOperations(),
                        transportService,
                        blobTransferTarget,
                        blobIndicesService
                    );
                } else {
                    handler = new RecoverySourceHandler(
                        shard,
                        recoveryTarget,
                        shard.getThreadPool(),
                        request,
                        recoveryChunkSizeInBytes,
                        recoverySettings.getMaxConcurrentFileChunks(),
                        recoverySettings.getMaxConcurrentOperations()
                    );
                }
                return new RecoveryHandlers(handler, recoveryTarget);
            }
        }
    }
}
