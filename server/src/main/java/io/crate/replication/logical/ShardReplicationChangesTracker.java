/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.replication.logical;

import static io.crate.replication.logical.LogicalReplicationSettings.PUBLISHER_INDEX_UUID;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.support.RetryableAction;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.support.replication.ReplicationResponse.ShardInfo;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.Translog.Operation;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.Scheduler.Cancellable;
import org.elasticsearch.threadpool.ThreadPool;
import org.jetbrains.annotations.Nullable;

import io.crate.concurrent.FutureActionListener;
import io.crate.exceptions.SQLExceptions;
import io.crate.execution.support.RetryRunnable;
import io.crate.replication.logical.action.ReplayChangesAction;
import io.crate.replication.logical.action.ShardChangesAction;
import io.crate.replication.logical.exceptions.InvalidShardEngineException;
import io.crate.replication.logical.seqno.RetentionLeaseHelper;

/**
 * Replicates batches of {@link org.elasticsearch.index.translog.Translog.Operation}'s to the subscribers target shards.
 * <p>
 * Derived from org.opensearch.replication.task.shard.ShardReplicationChangesTracker
 */
public class ShardReplicationChangesTracker implements Closeable {

    private static final Logger LOGGER = LogManager.getLogger(ShardReplicationChangesTracker.class);

    private final String subscriptionName;
    private final ShardId localShardId;
    private final ShardId remoteShardId;
    private final LogicalReplicationSettings replicationSettings;
    private final ThreadPool threadPool;
    private final Client localClient;
    private final ShardReplicationService shardReplicationService;
    private final String clusterName;
    private final Deque<SeqNoRange> missingBatches = new ArrayDeque<>();
    private final AtomicLong observedSeqNoAtLeader;
    private final AtomicLong seqNoAlreadyRequested;

    private volatile Scheduler.Cancellable cancellable;
    private volatile boolean closed = false;


    public ShardReplicationChangesTracker(String subscriptionName,
                                          IndexShard indexShard,
                                          ThreadPool threadPool,
                                          LogicalReplicationSettings replicationSettings,
                                          ShardReplicationService shardReplicationService,
                                          String clusterName,
                                          Client client) {
        this.subscriptionName = subscriptionName;
        this.localShardId = indexShard.shardId();
        this.remoteShardId = new ShardId(new Index(indexShard.shardId().getIndexName(), indexShard.indexSettings().getSettings().get(PUBLISHER_INDEX_UUID.getKey())), indexShard.shardId().id());
        this.replicationSettings = replicationSettings;
        this.threadPool = threadPool;
        this.localClient = client;
        this.shardReplicationService = shardReplicationService;
        this.clusterName = clusterName;
        var seqNoStats = indexShard.seqNoStats();
        this.observedSeqNoAtLeader = new AtomicLong(seqNoStats.getGlobalCheckpoint());
        this.seqNoAlreadyRequested = new AtomicLong(seqNoStats.getMaxSeqNo());
    }

    record SeqNoRange(long fromSeqNo, long toSeqNo) {
    }

    public void start() {
        LOGGER.debug("[{}] Spawning the shard changes reader", localShardId);
        var retryRunnable = newRunnable();
        cancellable = retryRunnable;
        retryRunnable.run();
    }

    private RetryRunnable newRunnable() {
        return new RetryRunnable(
            threadPool,
            ThreadPool.Names.LOGICAL_REPLICATION,
            this::pollAndProcessPendingChanges,
            BackoffPolicy.exponentialBackoff()
        );
    }

    private void pollAndProcessPendingChanges() {
        if (closed) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("[{}] ShardReplicationChangesTracker closed. Stopping tracking", localShardId);
            }
            return;
        }
        SeqNoRange rangeToFetch = getNextSeqNoRange();
        if (rangeToFetch == null) {
            cancellable = threadPool.scheduleUnlessShuttingDown(
                replicationSettings.pollDelay(),
                ThreadPool.Names.LOGICAL_REPLICATION,
                newRunnable()
            );
            return;
        }
        long fromSeqNo = rangeToFetch.fromSeqNo();
        long toSeqNo = rangeToFetch.toSeqNo();

        var futureClient = shardReplicationService.getRemoteClusterClient(
            remoteShardId.getIndex(),
            subscriptionName
        );
        var getPendingChangesRequest = new ShardChangesAction.Request(remoteShardId, fromSeqNo, toSeqNo);
        var futurePendingChanges = futureClient.thenCompose(remoteClient -> {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("[{}] Getting changes {}-{}", remoteShardId, fromSeqNo, toSeqNo);
            }
            return remoteClient.execute(ShardChangesAction.INSTANCE, getPendingChangesRequest);
        });
        var futureReplicationResponse = futurePendingChanges.thenCompose(this::replayChanges);
        futureReplicationResponse.whenComplete((replicationResp, e) -> {
            if (e == null) {
                var pendingChanges = futurePendingChanges.join();
                long lastSeqNo;
                List<Operation> translogOps = pendingChanges.changes();
                if (translogOps.isEmpty()) {
                    lastSeqNo = fromSeqNo - 1;
                } else {
                    lastSeqNo = translogOps.get(translogOps.size() - 1).seqNo();
                }
                updateBatchFetched(true, fromSeqNo, toSeqNo, lastSeqNo, pendingChanges.lastSyncedGlobalCheckpoint());
            } else {
                var t = SQLExceptions.unwrap(e);
                if (!closed && SQLExceptions.maybeTemporary(t)) {
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info(
                            "[{}] Temporary error during tracking of upstream shard changes for subscription '{}'. Retrying: {}:{}",
                            remoteShardId,
                            subscriptionName,
                            t.getClass().getSimpleName(),
                            t.getMessage()
                        );
                    }
                    updateBatchFetched(false, fromSeqNo, toSeqNo, fromSeqNo - 1, -1);
                } else if (t instanceof InvalidShardEngineException) {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("Shard is not accepting replayed changes, engine changed", t);
                    }
                } else {
                    LOGGER.warn(
                        "[{}] Error during tracking of upstream shard changes for subscription '{}'. Tracking stopped: {}",
                        localShardId,
                        subscriptionName,
                        t
                    );
                }
            }
        });
    }


    private CompletableFuture<ReplicationResponse> replayChanges(ShardChangesAction.Response response) {
        List<Translog.Operation> translogOps = response.changes();
        if (translogOps.isEmpty()) {
            return CompletableFuture.completedFuture(new ReplicationResponse());
        }
        var replayRequest = new ReplayChangesAction.Request(
            localShardId,
            translogOps,
            response.maxSeqNoOfUpdatesOrDeletes()
        );
        FutureActionListener<ReplicationResponse> listener = new FutureActionListener<>();
        RetryableAction<ReplicationResponse> retryableAction = new RetryableAction<ReplicationResponse>(
                LOGGER,
                threadPool.scheduler(),
                BackoffPolicy.exponentialBackoff(),
                listener) {

            @Override
            public void tryAction(ActionListener<ReplicationResponse> listener) {
                localClient.execute(ReplayChangesAction.INSTANCE, replayRequest).whenComplete(listener);
            }

            @Override
            public boolean shouldRetry(Throwable t) {
                return t instanceof ClusterBlockException || super.shouldRetry(t);
            }
        };
        retryableAction.run();
        return listener.thenApply(resp -> {
            ShardInfo shardInfo = resp.getShardInfo();
            if (shardInfo.getFailed() > 0) {
                for (ReplicationResponse.ShardInfo.Failure failure : shardInfo.getFailures()) {
                    LOGGER.error("[{}] Failed replaying changes. Failure: {}", localShardId, failure);
                }
                throw new RuntimeException("Some changes failed while replaying");
            }
            return resp;
        });
    }

    /**
     * Provides a range of operations to be fetched next.
     */
    @Nullable
    private SeqNoRange getNextSeqNoRange() {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("[{}] Waiting to get batch. requested: {}, leader: {}",
                         localShardId, seqNoAlreadyRequested.get(), observedSeqNoAtLeader.get());
        }

        // Wait till we have batch to fetch. Note that if seqNoAlreadyRequested is equal to observedSeqNoAtLeader,
        // we still should be sending one more request to fetch which will just do a poll and eventually timeout
        // if no new operations are there on the leader (configured via TransportGetChangesAction.WAIT_FOR_NEW_OPS_TIMEOUT)
        if (seqNoAlreadyRequested.get() > observedSeqNoAtLeader.get() && missingBatches.isEmpty()) {
            return null;
        }

        // missing batch takes higher priority.
        if (missingBatches.isEmpty() == false) {
            var missingBatch = missingBatches.removeFirst();
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("[{}] Fetching missing batch {}-{}", remoteShardId, missingBatch.fromSeqNo(), missingBatch.toSeqNo());
            }
            return missingBatch;
        } else {
            // return the next batch to fetch and update seqNoAlreadyRequested.
            var batchSize = replicationSettings.batchSize();
            var fromSeq = seqNoAlreadyRequested.getAndAdd(batchSize) + 1;
            var toSeq = fromSeq + batchSize - 1;
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("[{}] Fetching the batch {}-{}", remoteShardId, fromSeq, toSeq);
            }
            return new SeqNoRange(fromSeq, toSeq);
        }
    }

    /**
     * Ensures that we've successfully fetched a particular range of operations.
     * In case of any failure(or we didn't get complete batch), we make sure that we're fetching the
     * missing operations in the next batch.
     */
    private void updateBatchFetched(boolean success,
                                    long fromSeqNoRequested,
                                    long toSeqNoRequested,
                                    long toSeqNoReceived,
                                    long seqNoAtLeader) {
        if (closed) {
            return;
        }
        if (success) {
            // we shouldn't ever be getting more operations than requested.
            assert toSeqNoRequested >= toSeqNoReceived :
                Thread.currentThread().getName() + " Got more operations in the batch than requested";
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("[{}] Updating the batch fetched. {}-{}/{}, seqNoAtLeader:{}",
                             localShardId,
                             fromSeqNoRequested,
                             toSeqNoReceived,
                             toSeqNoRequested,
                             seqNoAtLeader
                );
            }

            // If we didn't get the complete batch that we had requested.
            if (toSeqNoRequested > toSeqNoReceived) {
                // If this is the last batch being fetched, update the seqNoAlreadyRequested.
                if (!seqNoAlreadyRequested.compareAndSet(toSeqNoRequested, toSeqNoReceived)) {
                    // Else, add to the missing operations to missing batch
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("[{}] Didn't get the complete batch. Adding the missing operations {}-{}",
                                     localShardId,
                                     toSeqNoReceived + 1,
                                     toSeqNoRequested
                        );
                    }
                    missingBatches.add(new SeqNoRange(toSeqNoReceived + 1, toSeqNoRequested));
                }
            }

            // Update the sequence number observed at leader.
            var currentSeqNoAtLeader = observedSeqNoAtLeader.getAndUpdate(value -> Math.max(seqNoAtLeader, value));
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("[{}] observedSeqNoAtLeader: {}", localShardId, currentSeqNoAtLeader);
            }
        } else {
            // If this is the last batch being fetched, update the seqNoAlreadyRequested.
            if (seqNoAlreadyRequested.get() == toSeqNoRequested) {
                seqNoAlreadyRequested.set(fromSeqNoRequested - 1);
            } else {
                // If this was not the last batch, we might have already fetched other batch of
                // operations after this. Adding this to missing.
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("[{}] Adding batch to missing {}-{}", localShardId, fromSeqNoRequested, toSeqNoRequested);
                }
                missingBatches.add(new SeqNoRange(fromSeqNoRequested, toSeqNoRequested));
            }
        }
        renewLeasesThenReschedule(toSeqNoReceived);
    }

    private void renewLeasesThenReschedule(long toSeqNoReceived) {
        // Renew retention lease with global checkpoint so that any shard that picks up shard replication task
        // has data until then.
        // Method is called inside a transport thread (response listener), so dispatch away
        threadPool.executor(ThreadPool.Names.LOGICAL_REPLICATION).execute(() ->
            shardReplicationService.getRemoteClusterClient(remoteShardId.getIndex(), subscriptionName).thenAccept(remoteClient ->
                RetentionLeaseHelper.renewRetentionLease(
                    remoteShardId,
                    toSeqNoReceived,
                    clusterName,
                    remoteClient,
                    ActionListener.wrap(
                        r -> {
                            if (!closed) {
                                cancellable = threadPool.scheduleUnlessShuttingDown(
                                    replicationSettings.pollDelay(),
                                    ThreadPool.Names.LOGICAL_REPLICATION,
                                    newRunnable()
                                );
                            }
                        },
                        e -> {
                            var t = SQLExceptions.unwrap(e);
                            boolean isClosed = closed; // one volatile read
                            if (!isClosed && SQLExceptions.maybeTemporary(t)) {
                                LOGGER.info(
                                    "[{}] Temporary error during renewal of retention leases for subscription '{}'. Retrying: {}:{}",
                                    remoteShardId,
                                    subscriptionName,
                                    t.getClass().getSimpleName(),
                                    t.getMessage()
                                );
                                cancellable = threadPool.scheduleUnlessShuttingDown(
                                    replicationSettings.pollDelay(),
                                    ThreadPool.Names.LOGICAL_REPLICATION,
                                    () -> renewLeasesThenReschedule(toSeqNoReceived)
                                );
                            } else if (isClosed) {
                                LOGGER.debug("Exception renewing retention lease. Stopping tracking (closed=true)", e);
                            } else {
                                LOGGER.warn("Exception renewing retention lease. Stopping tracking (closed=false)", e);
                            }
                        }
                    )
                )
            ));
    }

    @Override
    public void close() throws IOException {
        closed = true;
        Cancellable currentCancellable = cancellable;
        if (currentCancellable != null) {
            currentCancellable.cancel();
            cancellable = null;
        }
        shardReplicationService.getRemoteClusterClient(remoteShardId.getIndex(), subscriptionName)
            .thenAccept(client -> RetentionLeaseHelper.attemptRetentionLeaseRemoval(
                remoteShardId,
                clusterName,
                client,
                ActionListener.wrap(() -> {})
            ));
    }
}
