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

import org.apache.lucene.store.RateLimiter;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.seqno.ReplicationTracker;
import org.elasticsearch.index.seqno.RetentionLeases;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.EmptyTransportResponseHandler;
import org.elasticsearch.transport.TransportFuture;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

public class RemoteRecoveryTargetHandler implements RecoveryTargetHandler {

    private final TransportService transportService;
    private final long recoveryId;
    private final ShardId shardId;
    private final DiscoveryNode targetNode;
    private final RecoverySettings recoverySettings;

    private final TransportRequestOptions translogOpsRequestOptions;
    private final TransportRequestOptions fileChunkRequestOptions;

    private final AtomicLong bytesSinceLastPause = new AtomicLong();

    private final Consumer<Long> onSourceThrottle;

    public RemoteRecoveryTargetHandler(long recoveryId, ShardId shardId, TransportService transportService,
                                       DiscoveryNode targetNode, RecoverySettings recoverySettings, Consumer<Long> onSourceThrottle) {
        this.transportService = transportService;
        this.recoveryId = recoveryId;
        this.shardId = shardId;
        this.targetNode = targetNode;
        this.recoverySettings = recoverySettings;
        this.onSourceThrottle = onSourceThrottle;
        this.translogOpsRequestOptions = TransportRequestOptions.builder()
                .withType(TransportRequestOptions.Type.RECOVERY)
                .withTimeout(recoverySettings.internalActionLongTimeout())
                .build();
        this.fileChunkRequestOptions = TransportRequestOptions.builder()
                // we are saving the cpu for other things
                .withType(TransportRequestOptions.Type.RECOVERY)
                .withTimeout(recoverySettings.internalActionTimeout())
                .build();

    }

    @Override
    public void prepareForTranslogOperations(int totalTranslogOps,
                                             ActionListener<Void> listener) {
        transportService.sendRequest(
            targetNode,
            PeerRecoveryTargetService.Actions.PREPARE_TRANSLOG,
            new RecoveryPrepareForTranslogOperationsRequest(recoveryId, shardId, totalTranslogOps),
            TransportRequestOptions.builder().withTimeout(recoverySettings.internalActionTimeout()).build(),
            new ActionListenerResponseHandler<>(
                ActionListener.map(listener, r -> null),
                in -> TransportResponse.Empty.INSTANCE,
                ThreadPool.Names.GENERIC
            )
        );
    }

    @Override
    public void finalizeRecovery(long globalCheckpoint, long trimAboveSeqNo, ActionListener<Void> listener) {
        transportService.sendRequest(
            targetNode,
            PeerRecoveryTargetService.Actions.FINALIZE,
            new RecoveryFinalizeRecoveryRequest(recoveryId, shardId, globalCheckpoint, trimAboveSeqNo),
            TransportRequestOptions.builder().withTimeout(recoverySettings.internalActionLongTimeout()).build(),
            new ActionListenerResponseHandler<>(
                ActionListener.map(listener, r -> null),
                in -> TransportResponse.Empty.INSTANCE,
                ThreadPool.Names.GENERIC
            )
        );
    }

    @Override
    public void handoffPrimaryContext(final ReplicationTracker.PrimaryContext primaryContext) {
        TransportFuture<TransportResponse.Empty> handler = new TransportFuture<>(EmptyTransportResponseHandler.INSTANCE_SAME);
        transportService.sendRequest(
            targetNode, PeerRecoveryTargetService.Actions.HANDOFF_PRIMARY_CONTEXT,
            new RecoveryHandoffPrimaryContextRequest(recoveryId, shardId, primaryContext),
            TransportRequestOptions.builder().withTimeout(recoverySettings.internalActionTimeout()).build(), handler);
        handler.txGet();
    }

    @Override
    public void indexTranslogOperations(List<Translog.Operation> operations,
                                        int totalTranslogOps,
                                        long maxSeenAutoIdTimestampOnPrimary,
                                        long maxSeqNoOfDeletesOrUpdatesOnPrimary,
                                        RetentionLeases retentionLeases,
                                        long mappingVersionOnPrimary,
                                        ActionListener<Long> listener) {
        final RecoveryTranslogOperationsRequest request = new RecoveryTranslogOperationsRequest(
            recoveryId,
            shardId,
            operations,
            totalTranslogOps,
            maxSeenAutoIdTimestampOnPrimary,
            maxSeqNoOfDeletesOrUpdatesOnPrimary,
            retentionLeases,
            mappingVersionOnPrimary
        );
        transportService.sendRequest(
            targetNode,
            PeerRecoveryTargetService.Actions.TRANSLOG_OPS,
            request,
            translogOpsRequestOptions,
            new ActionListenerResponseHandler<>(
                ActionListener.map(listener, r -> r.localCheckpoint),
                RecoveryTranslogOperationsResponse::new,
                ThreadPool.Names.GENERIC)
        );
    }

    @Override
    public void receiveFileInfo(List<String> phase1FileNames, List<Long> phase1FileSizes, List<String> phase1ExistingFileNames,
                                List<Long> phase1ExistingFileSizes, int totalTranslogOps, ActionListener<Void> listener) {
        RecoveryFilesInfoRequest recoveryInfoFilesRequest = new RecoveryFilesInfoRequest(recoveryId, shardId,
            phase1FileNames, phase1FileSizes, phase1ExistingFileNames, phase1ExistingFileSizes, totalTranslogOps);
        transportService.sendRequest(targetNode, PeerRecoveryTargetService.Actions.FILES_INFO, recoveryInfoFilesRequest,
            TransportRequestOptions.builder().withTimeout(recoverySettings.internalActionTimeout()).build(),
            new ActionListenerResponseHandler<>(ActionListener.map(listener, r -> null),
                in -> TransportResponse.Empty.INSTANCE, ThreadPool.Names.GENERIC));
    }

    @Override
    public void cleanFiles(int totalTranslogOps,
                           long globalCheckpoint,
                           Store.MetadataSnapshot sourceMetadata,
                           ActionListener<Void> listener) {
        transportService.sendRequest(
            targetNode,
            PeerRecoveryTargetService.Actions.CLEAN_FILES,
            new RecoveryCleanFilesRequest(recoveryId, shardId, sourceMetadata, totalTranslogOps, globalCheckpoint),
            TransportRequestOptions.builder().withTimeout(recoverySettings.internalActionTimeout()).build(),
            new ActionListenerResponseHandler<>(
                ActionListener.map(listener, r -> null),
                in -> TransportResponse.Empty.INSTANCE, ThreadPool.Names.GENERIC
            )
        );
    }

    @Override
    public void writeFileChunk(StoreFileMetadata fileMetadata,
                               long position,
                               BytesReference content,
                               boolean lastChunk,
                               int totalTranslogOps,
                               ActionListener<Void> listener) {
        // Pause using the rate limiter, if desired, to throttle the recovery
        final long throttleTimeInNanos;
        // always fetch the ratelimiter - it might be updated in real-time on the recovery settings
        final RateLimiter rl = recoverySettings.rateLimiter();
        if (rl != null) {
            long bytes = bytesSinceLastPause.addAndGet(content.length());
            if (bytes > rl.getMinPauseCheckBytes()) {
                // Time to pause
                bytesSinceLastPause.addAndGet(-bytes);
                try {
                    throttleTimeInNanos = rl.pause(bytes);
                    onSourceThrottle.accept(throttleTimeInNanos);
                } catch (IOException e) {
                    throw new ElasticsearchException("failed to pause recovery", e);
                }
            } else {
                throttleTimeInNanos = 0;
            }
        } else {
            throttleTimeInNanos = 0;
        }

        transportService.sendRequest(targetNode, PeerRecoveryTargetService.Actions.FILE_CHUNK,
            new RecoveryFileChunkRequest(recoveryId, shardId, fileMetadata, position, content, lastChunk,
                totalTranslogOps,
                /* we send estimateTotalOperations with every request since we collect stats on the target and that way we can
                 * see how many translog ops we accumulate while copying files across the network. A future optimization
                 * would be in to restart file copy again (new deltas) if we have too many translog ops are piling up.
                 */
                throttleTimeInNanos
            ),
            fileChunkRequestOptions,
            new ActionListenerResponseHandler<>(
                ActionListener.map(listener, r -> null),
                in -> TransportResponse.Empty.INSTANCE,
                ThreadPool.Names.GENERIC
            )
        );
    }

}
