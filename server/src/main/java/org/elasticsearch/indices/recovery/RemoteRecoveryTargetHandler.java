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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.RateLimiter;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.support.RetryableAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.CancellableThreads;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.index.seqno.ReplicationTracker;
import org.elasticsearch.index.seqno.RetentionLeases;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.transport.SendRequestTransportException;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportRequestOptions.Type;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

import io.crate.common.unit.TimeValue;
import io.crate.concurrent.FutureActionListener;
import io.crate.exceptions.SQLExceptions;

public class RemoteRecoveryTargetHandler implements RecoveryTargetHandler {

    private static final Logger LOGGER = LogManager.getLogger(RemoteRecoveryTargetHandler.class);

    private final TransportService transportService;
    private final ThreadPool threadPool;
    private final long recoveryId;
    private final ShardId shardId;
    private final DiscoveryNode targetNode;
    private final RecoverySettings recoverySettings;
    private final Map<Object, RetryableAction<?>> onGoingRetryableActions = new ConcurrentHashMap<>();

    private final TransportRequestOptions translogOpsRequestOptions;
    private final TransportRequestOptions fileChunkRequestOptions;

    private final AtomicLong bytesSinceLastPause = new AtomicLong();
    private final AtomicLong requestSeqNoGenerator = new AtomicLong(0);

    private final Consumer<Long> onSourceThrottle;
    private final boolean retriesSupported;
    private volatile boolean isCancelled = false;

    public RemoteRecoveryTargetHandler(long recoveryId, ShardId shardId, TransportService transportService,
                                       DiscoveryNode targetNode, RecoverySettings recoverySettings, Consumer<Long> onSourceThrottle) {
        this.transportService = transportService;
        this.threadPool = transportService.getThreadPool();
        this.recoveryId = recoveryId;
        this.shardId = shardId;
        this.targetNode = targetNode;
        this.recoverySettings = recoverySettings;
        this.onSourceThrottle = onSourceThrottle;
        this.translogOpsRequestOptions = new TransportRequestOptions(
            recoverySettings.internalActionLongTimeout(),
            Type.RECOVERY
        );
        this.fileChunkRequestOptions = new TransportRequestOptions(
            // we are saving the cpu for other things
            recoverySettings.internalActionTimeout(),
            TransportRequestOptions.Type.RECOVERY
        );
        this.retriesSupported = targetNode.getVersion().onOrAfter(Version.V_5_1_0);
    }

    public DiscoveryNode targetNode() {
        return targetNode;
    }

    @Override
    public void prepareForTranslogOperations(int totalTranslogOps, ActionListener<Void> listener) {
        final String action = PeerRecoveryTargetService.Actions.PREPARE_TRANSLOG;
        final long requestSeqNo = requestSeqNoGenerator.getAndIncrement();
        final RecoveryPrepareForTranslogOperationsRequest request =
            new RecoveryPrepareForTranslogOperationsRequest(recoveryId, requestSeqNo, shardId, totalTranslogOps);
        final TransportRequestOptions options = new TransportRequestOptions(recoverySettings.internalActionTimeout());
        final Writeable.Reader<TransportResponse.Empty> reader = in -> TransportResponse.Empty.INSTANCE;
        final ActionListener<TransportResponse.Empty> responseListener = listener.map(r -> null);
        executeRetryableAction(action, request, options, responseListener, reader);
    }

    @Override
    public void finalizeRecovery(final long globalCheckpoint, final long trimAboveSeqNo, final ActionListener<Void> listener) {
        final String action = PeerRecoveryTargetService.Actions.FINALIZE;
        final long requestSeqNo = requestSeqNoGenerator.getAndIncrement();
        final RecoveryFinalizeRecoveryRequest request =
            new RecoveryFinalizeRecoveryRequest(recoveryId, requestSeqNo, shardId, globalCheckpoint, trimAboveSeqNo);
        final TransportRequestOptions options = new TransportRequestOptions(recoverySettings.internalActionLongTimeout());
        final Writeable.Reader<TransportResponse.Empty> reader = in -> TransportResponse.Empty.INSTANCE;
        final ActionListener<TransportResponse.Empty> responseListener = listener.map(r -> null);
        executeRetryableAction(action, request, options, responseListener, reader);
    }

    @Override
    public void handoffPrimaryContext(final ReplicationTracker.PrimaryContext primaryContext, ActionListener<Void> listener) {
        FutureActionListener<TransportResponse.Empty> future = new FutureActionListener<>();
        ActionListenerResponseHandler<TransportResponse.Empty> handler = new ActionListenerResponseHandler<>(
            PeerRecoveryTargetService.Actions.HANDOFF_PRIMARY_CONTEXT,
            future,
            in -> TransportResponse.Empty.INSTANCE,
            ThreadPool.Names.SAME
        );
        transportService.sendRequest(
            targetNode,
            PeerRecoveryTargetService.Actions.HANDOFF_PRIMARY_CONTEXT,
            new RecoveryHandoffPrimaryContextRequest(recoveryId, shardId, primaryContext),
            new TransportRequestOptions(recoverySettings.internalActionTimeout()),
            handler
        );
        try {
            future.get();
            listener.accept(null, null);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Future got interrupted", ex);
        } catch (ExecutionException ex) {
            listener.accept(null, ex);
            if (ex.getCause() instanceof ElasticsearchException esEx) {
                throw esEx;
            } else {
                throw new TransportException("Failed execution", ex);
            }
        }
    }

    @Override
    public void indexTranslogOperations(
            final List<Translog.Operation> operations,
            final int totalTranslogOps,
            final long maxSeenAutoIdTimestampOnPrimary,
            final long maxSeqNoOfDeletesOrUpdatesOnPrimary,
            final RetentionLeases retentionLeases,
            final long mappingVersionOnPrimary,
            final ActionListener<Long> listener) {
        final String action = PeerRecoveryTargetService.Actions.TRANSLOG_OPS;
        final long requestSeqNo = requestSeqNoGenerator.getAndIncrement();
        final RecoveryTranslogOperationsRequest request = new RecoveryTranslogOperationsRequest(
                recoveryId,
                requestSeqNo,
                shardId,
                operations,
                totalTranslogOps,
                maxSeenAutoIdTimestampOnPrimary,
                maxSeqNoOfDeletesOrUpdatesOnPrimary,
                retentionLeases,
                mappingVersionOnPrimary);
        final Writeable.Reader<RecoveryTranslogOperationsResponse> reader = RecoveryTranslogOperationsResponse::new;
        final ActionListener<RecoveryTranslogOperationsResponse> responseListener = listener.map(r -> r.localCheckpoint);
        executeRetryableAction(action, request, translogOpsRequestOptions, responseListener, reader);
    }

    @Override
    public void receiveFileInfo(List<String> phase1FileNames, List<Long> phase1FileSizes, List<String> phase1ExistingFileNames,
                                List<Long> phase1ExistingFileSizes, int totalTranslogOps, ActionListener<Void> listener) {
        final String action = PeerRecoveryTargetService.Actions.FILES_INFO;
        final long requestSeqNo = requestSeqNoGenerator.getAndIncrement();
        RecoveryFilesInfoRequest request = new RecoveryFilesInfoRequest(recoveryId, requestSeqNo, shardId, phase1FileNames, phase1FileSizes,
            phase1ExistingFileNames, phase1ExistingFileSizes, totalTranslogOps);
        final TransportRequestOptions options = new TransportRequestOptions(recoverySettings.internalActionTimeout());
        final Writeable.Reader<TransportResponse.Empty> reader = in -> TransportResponse.Empty.INSTANCE;
        final ActionListener<TransportResponse.Empty> responseListener = listener.map(r -> null);
        executeRetryableAction(action, request, options, responseListener, reader);
    }

    @Override
    public void cleanFiles(int totalTranslogOps,
                           long globalCheckpoint,
                           Store.MetadataSnapshot sourceMetadata,
                           ActionListener<Void> listener) {
        final String action = PeerRecoveryTargetService.Actions.CLEAN_FILES;
        final long requestSeqNo = requestSeqNoGenerator.getAndIncrement();
        final RecoveryCleanFilesRequest request =
            new RecoveryCleanFilesRequest(recoveryId, requestSeqNo, shardId, sourceMetadata, totalTranslogOps, globalCheckpoint);
        final TransportRequestOptions options = new TransportRequestOptions(recoverySettings.internalActionTimeout());
        final Writeable.Reader<TransportResponse.Empty> reader = in -> TransportResponse.Empty.INSTANCE;
        final ActionListener<TransportResponse.Empty> responseListener = listener.map(r -> null);
        executeRetryableAction(action, request, options, responseListener, reader);
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

        final String action = PeerRecoveryTargetService.Actions.FILE_CHUNK;
        final long requestSeqNo = requestSeqNoGenerator.getAndIncrement();
        /* we send estimateTotalOperations with every request since we collect stats on the target and that way we can
         * see how many translog ops we accumulate while copying files across the network. A future optimization
         * would be in to restart file copy again (new deltas) if we have too many translog ops are piling up.
         */
        final RecoveryFileChunkRequest request = new RecoveryFileChunkRequest(
            recoveryId, requestSeqNo, shardId, fileMetadata, position, content, lastChunk, totalTranslogOps, throttleTimeInNanos);
        final Writeable.Reader<TransportResponse.Empty> reader = in -> TransportResponse.Empty.INSTANCE;
        executeRetryableAction(action, request, fileChunkRequestOptions, listener.map(r -> null), reader);
    }

    @Override
    public void cancel() {
        isCancelled = true;
        if (onGoingRetryableActions.isEmpty()) {
            return;
        }
        final RuntimeException exception = new CancellableThreads.ExecutionCancelledException("recovery was cancelled");
        // Dispatch to generic as cancellation calls can come on the cluster state applier thread
        threadPool.generic().execute(() -> {
            for (RetryableAction<?> action : onGoingRetryableActions.values()) {
                action.cancel(exception);
            }
            onGoingRetryableActions.clear();
        });
    }

    private <T extends TransportResponse> void executeRetryableAction(String action, RecoveryTransportRequest request,
                                                                      TransportRequestOptions options, ActionListener<T> actionListener,
                                                                      Writeable.Reader<T> reader) {
        final Object key = new Object();
        final ActionListener<T> removeListener = actionListener.runBefore(() -> onGoingRetryableActions.remove(key));
        final TimeValue initialDelay = TimeValue.timeValueMillis(200);
        final TimeValue timeout = recoverySettings.internalActionRetryTimeout();
        final RetryableAction<T> retryableAction = new RetryableAction<T>(
                LOGGER,
                threadPool.scheduler(),
                BackoffPolicy.exponentialBackoff(initialDelay, timeout),
                removeListener) {

            @Override
            public void tryAction(ActionListener<T> listener) {
                transportService.sendRequest(targetNode, action, request, options,
                    new ActionListenerResponseHandler<>(action, listener, reader, ThreadPool.Names.GENERIC));
            }

            @Override
            public boolean shouldRetry(Throwable e) {
                return retriesSupported && retryableException(e);
            }
        };
        onGoingRetryableActions.put(key, retryableAction);
        retryableAction.run();
        if (isCancelled) {
            retryableAction.cancel(new CancellableThreads.ExecutionCancelledException("recovery was cancelled"));
        }
    }

    private static boolean retryableException(Throwable t) {
        if (t instanceof ConnectTransportException) {
            return true;
        } else if (t instanceof SendRequestTransportException) {
            final Throwable cause = SQLExceptions.unwrap(t);
            return cause instanceof ConnectTransportException;
        } else if (t instanceof RemoteTransportException) {
            final Throwable cause = SQLExceptions.unwrap(t);
            return cause instanceof CircuitBreakingException ||
                cause instanceof EsRejectedExecutionException;
        }
        return false;
    }
}
