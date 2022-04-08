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

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import javax.annotation.Nullable;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.support.replication.ReplicationResponse.ShardInfo;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.Translog.Operation;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteClusters;

import io.crate.action.FutureActionListener;
import io.crate.common.unit.TimeValue;
import io.crate.exceptions.SQLExceptions;
import io.crate.execution.support.RetryListener;
import io.crate.execution.support.RetryRunnable;
import io.crate.replication.logical.action.ReplayChangesAction;
import io.crate.replication.logical.action.ShardChangesAction;
import io.crate.replication.logical.metadata.Subscription;
import io.crate.replication.logical.seqno.RetentionLeaseHelper;

/**
 * Replicates batches of {@link org.elasticsearch.index.translog.Translog.Operation}'s to the subscribers target shards.
 * <p>
 * Derived from org.opensearch.replication.task.shard.ShardReplicationChangesTracker
 */
public class ShardReplicationChangesTracker implements Closeable {

    private static final Logger LOGGER = Loggers.getLogger(ShardReplicationChangesTracker.class);

    private final ShardId shardId;
    private final LogicalReplicationSettings replicationSettings;
    private final ThreadPool threadPool;
    private final Client localClient;
    private final String clusterName;
    private final Deque<SeqNoRange> missingBatches = new ArrayDeque<>();
    private final AtomicLong observedSeqNoAtLeader;
    private final AtomicLong seqNoAlreadyRequested;
    private final String subscriptionName;
    private final Subscription subscription;
    private final RemoteClusters remoteClusters;
    private Scheduler.ScheduledCancellable cancellable;


    public ShardReplicationChangesTracker(String subscriptionName,
                                          Subscription subscription,
                                          IndexShard indexShard,
                                          ThreadPool threadPool,
                                          LogicalReplicationSettings replicationSettings,
                                          RemoteClusters remoteClusters,
                                          String clusterName,
                                          Client client) {
        this.subscriptionName = subscriptionName;
        this.subscription = subscription;
        this.remoteClusters = remoteClusters;
        this.shardId = indexShard.shardId();
        this.replicationSettings = replicationSettings;
        this.threadPool = threadPool;
        this.localClient = client;
        this.clusterName = clusterName;
        var seqNoStats = indexShard.seqNoStats();
        this.observedSeqNoAtLeader = new AtomicLong(seqNoStats.getGlobalCheckpoint());
        this.seqNoAlreadyRequested = new AtomicLong(seqNoStats.getMaxSeqNo());
    }

    record SeqNoRange(long fromSeqNo, long toSeqNo) {
    }

    public void start() {
        LOGGER.debug("[{}] Spawning the shard changes reader", shardId);
        newRunnable().run();
    }

    private RetryRunnable newRunnable() {
        return new RetryRunnable(
            threadPool.executor(ThreadPool.Names.LOGICAL_REPLICATION),
            threadPool.scheduler(),
            this::pollAndProcessPendingChanges,
            BackoffPolicy.exponentialBackoff()
        );
    }

    private void pollAndProcessPendingChanges() {
        SeqNoRange rangeToFetch = getNextSeqNoRange();
        if (rangeToFetch == null) {
            cancellable = threadPool.schedule(
                newRunnable(),
                replicationSettings.pollDelay(),
                ThreadPool.Names.LOGICAL_REPLICATION
            );
            return;
        }
        long fromSeqNo = rangeToFetch.fromSeqNo();
        long toSeqNo = rangeToFetch.toSeqNo();

        var futureClient = remoteClusters.connect(subscriptionName, subscription.connectionInfo());
        var getPendingChangesRequest = new ShardChangesAction.Request(shardId, fromSeqNo, toSeqNo);
        var futurePendingChanges = futureClient.thenCompose(remoteClient -> {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("[{}] Getting changes {}-{}", shardId, fromSeqNo, toSeqNo);
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
                if (LogicalReplicationRetry.failureIsTemporary(t)) {
                    updateBatchFetched(false, fromSeqNo, toSeqNo, fromSeqNo - 1, -1);
                } else {
                    LOGGER.warn(
                        "Unrecoverable error during processing of upstream changes for subscription '{}'. Tracking stopped with failure: {}",
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
            shardId,
            translogOps,
            response.maxSeqNoOfUpdatesOrDeletes()
        );
        FutureActionListener<ReplicationResponse, ReplicationResponse> listener = new FutureActionListener<>(resp -> {
            ShardInfo shardInfo = resp.getShardInfo();
            if (shardInfo.getFailed() > 0) {
                for (ReplicationResponse.ShardInfo.Failure failure : shardInfo.getFailures()) {
                    LOGGER.error("[{}] Failed replaying changes. Failure: {}", shardId, failure);
                }
                throw new RuntimeException("Some changes failed while replaying");
            }
            return resp;
        });
        var retryListener = new ReplayChangesRetryListener<>(
            threadPool.scheduler(),
            l -> localClient.execute(ReplayChangesAction.INSTANCE, replayRequest, l),
            listener,
            BackoffPolicy.exponentialBackoff()
        );
        localClient.execute(ReplayChangesAction.INSTANCE, replayRequest, retryListener);
        return listener;
    }

    /**
     * Provides a range of operations to be fetched next.
     */
    @Nullable
    private SeqNoRange getNextSeqNoRange() {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("[{}] Waiting to get batch. requested: {}, leader: {}",
                         shardId, seqNoAlreadyRequested.get(), observedSeqNoAtLeader.get());
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
                LOGGER.debug("[{}] Fetching missing batch {}-{}", shardId, missingBatch.fromSeqNo(), missingBatch.toSeqNo());
            }
            return missingBatch;
        } else {
            // return the next batch to fetch and update seqNoAlreadyRequested.
            var batchSize = replicationSettings.batchSize();
            var fromSeq = seqNoAlreadyRequested.getAndAdd(batchSize) + 1;
            var toSeq = fromSeq + batchSize - 1;
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("[{}] Fetching the batch {}-{}", shardId, fromSeq, toSeq);
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
        if (success) {
            // we shouldn't ever be getting more operations than requested.
            assert toSeqNoRequested >= toSeqNoReceived :
                Thread.currentThread().getName() + " Got more operations in the batch than requested";
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("[{}] Updating the batch fetched. {}-{}/{}, seqNoAtLeader:{}",
                             shardId,
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
                                     shardId,
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
                LOGGER.debug("[{}] observedSeqNoAtLeader: {}", shardId, currentSeqNoAtLeader);
            }
        } else {
            // If this is the last batch being fetched, update the seqNoAlreadyRequested.
            if (seqNoAlreadyRequested.get() == toSeqNoRequested) {
                seqNoAlreadyRequested.set(fromSeqNoRequested - 1);
            } else {
                // If this was not the last batch, we might have already fetched other batch of
                // operations after this. Adding this to missing.
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("[{}] Adding batch to missing {}-{}", shardId, fromSeqNoRequested, toSeqNoRequested);
                }
                missingBatches.add(new SeqNoRange(fromSeqNoRequested, toSeqNoRequested));
            }
        }

        renewRetentionLease(toSeqNoReceived, null);
    }

    private void renewRetentionLease(long toSeqNoReceived, @Nullable Iterator<TimeValue> backOff) {
        // Renew retention lease with global checkpoint so that any shard that picks up shard replication task
        // has data until then.
        // Method is called inside a transport thread (response listener), so dispatch away
        threadPool.executor(ThreadPool.Names.LOGICAL_REPLICATION).execute(() -> {
            remoteClusters.connect(subscriptionName, subscription.connectionInfo()).thenAccept(client -> {
                RetentionLeaseHelper.renewRetentionLease(
                    shardId,
                    toSeqNoReceived,
                    clusterName,
                    client,
                    ActionListener.wrap(
                        r -> {
                            cancellable = threadPool.schedule(
                                newRunnable(),
                                replicationSettings.pollDelay(),
                                ThreadPool.Names.LOGICAL_REPLICATION
                            );
                        },
                        e -> {
                            var err = SQLExceptions.unwrap(e);
                            if (!LogicalReplicationRetry.failureIsTemporary(err)) {
                                LOGGER.warn("Exception renewing retention lease. Stopping tracking changes.", err);
                                return;
                            }

                            Iterator<TimeValue> backOffIt =
                                backOff == null ? BackoffPolicy.exponentialBackoff().iterator() : backOff;

                            try {
                                TimeValue delay = backOffIt.next();
                                cancellable = threadPool.schedule(
                                    () -> renewRetentionLease(toSeqNoReceived, backOffIt),
                                    TimeValue.timeValueNanos(delay.nanos() + replicationSettings.pollDelay().nanos()),
                                    ThreadPool.Names.LOGICAL_REPLICATION
                                );
                            } catch (NoSuchElementException ignored) {
                                LOGGER.warn("Exception renewing retention lease. Stopping tracking changes.", err);
                            }
                        }
                    )
                );
            });
        });
    }


    @Override
    public void close() throws IOException {
        if (cancellable != null) {
            cancellable.cancel();
        }
        remoteClusters.connect(subscriptionName, subscription.connectionInfo())
            .thenAccept(client -> {
                RetentionLeaseHelper.attemptRetentionLeaseRemoval(
                    shardId,
                    clusterName,
                    client,
                    ActionListener.wrap(() -> {})
                );
            });
    }

    private static class ReplayChangesRetryListener<TResp> extends RetryListener<TResp> {

        public ReplayChangesRetryListener(ScheduledExecutorService scheduler,
                                          Consumer<ActionListener<TResp>> command, ActionListener<TResp> delegate,
                                          Iterable<TimeValue> backOffPolicy) {
            super(scheduler, command, delegate, backOffPolicy);
        }

        @Override
        protected boolean shouldRetry(Throwable throwable) {
            return super.shouldRetry(throwable)
                || throwable instanceof ClusterBlockException
                || throwable instanceof IndexNotFoundException;
        }
    }
}
