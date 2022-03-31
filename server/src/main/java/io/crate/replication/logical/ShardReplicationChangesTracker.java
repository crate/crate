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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardClosedException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.NodeDisconnectedException;
import org.elasticsearch.transport.NodeNotConnectedException;

import io.crate.exceptions.Exceptions;
import io.crate.exceptions.SQLExceptions;
import io.crate.execution.support.RetryListener;
import io.crate.execution.support.RetryRunnable;
import io.crate.replication.logical.action.ReplayChangesAction;
import io.crate.replication.logical.action.ShardChangesAction;
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
    private final ShardReplicationService shardReplicationService;
    private final String clusterName;
    private final List<SeqNoRange> missingBatches = Collections.synchronizedList(new ArrayList<>());
    private final AtomicLong observedSeqNoAtLeader;
    private final AtomicLong seqNoAlreadyRequested;
    private Scheduler.ScheduledCancellable cancellable;

    public ShardReplicationChangesTracker(IndexShard indexShard,
                                          ThreadPool threadPool,
                                          LogicalReplicationSettings replicationSettings,
                                          ShardReplicationService shardReplicationService,
                                          String clusterName,
                                          Client client) {
        this.shardId = indexShard.shardId();
        this.replicationSettings = replicationSettings;
        this.threadPool = threadPool;
        this.localClient = client;
        this.shardReplicationService = shardReplicationService;
        this.clusterName = clusterName;
        var seqNoStats = indexShard.seqNoStats();
        observedSeqNoAtLeader = new AtomicLong(seqNoStats.getGlobalCheckpoint());
        seqNoAlreadyRequested = new AtomicLong(seqNoStats.getMaxSeqNo());
    }

    record SeqNoRange(long fromSeqNo, long toSeqNo) {
    }

    public void start() {
        LOGGER.debug("[{}] Spawning the shard changes reader", shardId);
        var runnable = new RetryRunnable(
            threadPool.executor(ThreadPool.Names.LOGICAL_REPLICATION),
            threadPool.scheduler(),
            this::requestBatchToFetch,
            BackoffPolicy.exponentialBackoff()
        );
        runnable.run();
    }

    private void requestBatchToFetch() {
        requestBatchToFetch(batchToFetch -> {
            long fromSeqNo = batchToFetch.fromSeqNo();
            long toSeqNo = batchToFetch.toSeqNo();
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("[{}] Getting changes {}-{}", shardId, fromSeqNo, toSeqNo);
            }
            getChanges(
                fromSeqNo,
                toSeqNo,
                response -> {
                    List<Translog.Operation> translogOps = response.changes();
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("[{}] Got {} changes starting from seqNo: {}",
                                     shardId, translogOps.size(), fromSeqNo);
                    }
                    replayChanges(
                        translogOps,
                        response.maxSeqNoOfUpdatesOrDeletes(),
                        ignored -> {
                            if (LOGGER.isDebugEnabled()) {
                                LOGGER.debug("[{}] Replayed changes {}-{}", shardId, fromSeqNo, toSeqNo);
                            }
                            long lastSeqNo = fromSeqNo - 1;
                            if (translogOps.isEmpty() == false) {
                                lastSeqNo = translogOps.get(translogOps.size() - 1).seqNo();
                            }
                            updateBatchFetched(
                                true,
                                fromSeqNo,
                                toSeqNo,
                                lastSeqNo,
                                response.lastSyncedGlobalCheckpoint()
                            );
                        },
                        e -> {}
                    );
                },
                e -> {
                    var t = SQLExceptions.unwrap(e);
                    if (t instanceof ElasticsearchTimeoutException) {
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug("[{}] Timed out waiting for new changes. Current seqNo: {}",
                                         shardId,
                                         fromSeqNo);
                        }
                        updateBatchFetched(false, fromSeqNo, toSeqNo, fromSeqNo - 1, -1);
                    } else if (t instanceof NodeNotConnectedException
                        || t instanceof NodeDisconnectedException
                        || t instanceof NodeClosedException) {
                        LOGGER.info("[{}] Node not connected. Retrying.. {}", shardId, e);
                        updateBatchFetched(false, fromSeqNo, toSeqNo, fromSeqNo - 1, -1);
                    } else if (t instanceof IndexShardClosedException) {
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug("[{}] Remote shard closed (table closed?), will stop tracking changes", shardId);
                        }
                    } else if (t instanceof IndexNotFoundException || t instanceof NoShardAvailableActionException) {
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug("[{}] Remote shard not found (dropped table?), will stop tracking changes", shardId);
                        }
                    } else {
                        LOGGER.warn(
                            String.format(Locale.ENGLISH,
                                          "[%s] Unable to get changes from seqNo: %d, will stop tracking",
                                          shardId, fromSeqNo),
                            t);
                    }

                }
            );
        });
    }

    /**
     * Provides a range of operations to be fetched next.
     */
    private void requestBatchToFetch(Consumer<SeqNoRange> consumer) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("[{}] Waiting to get batch. requested: {}, leader: {}",
                         shardId, seqNoAlreadyRequested.get(), observedSeqNoAtLeader.get());
        }

        // Wait till we have batch to fetch. Note that if seqNoAlreadyRequested is equal to observedSeqNoAtLeader,
        // we still should be sending one more request to fetch which will just do a poll and eventually timeout
        // if no new operations are there on the leader (configured via TransportGetChangesAction.WAIT_FOR_NEW_OPS_TIMEOUT)
        if (seqNoAlreadyRequested.get() > observedSeqNoAtLeader.get() && missingBatches.isEmpty()) {
            cancellable = threadPool.schedule(
                () -> requestBatchToFetch(consumer),
                replicationSettings.pollDelay(),
                ThreadPool.Names.LOGICAL_REPLICATION
            );
            return;
        }

        // missing batch takes higher priority.
        if (missingBatches.isEmpty() == false) {
            var missingBatch = missingBatches.remove(0);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("[{}] Fetching missing batch {}-{}", shardId, missingBatch.fromSeqNo(), missingBatch.toSeqNo());
            }
            consumer.accept(missingBatch);
        } else {
            // return the next batch to fetch and update seqNoAlreadyRequested.
            var batchSize = replicationSettings.batchSize();
            var fromSeq = seqNoAlreadyRequested.getAndAdd(batchSize) + 1;
            var toSeq = fromSeq + batchSize - 1;
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("[{}] Fetching the batch {}-{}", shardId, fromSeq, toSeq);
            }
            consumer.accept(new SeqNoRange(fromSeq, toSeq));
        }
    }

    private void getChanges(long fromSeqNo,
                            long toSeqNo,
                            CheckedConsumer<ShardChangesAction.Response, ? extends Exception> onSuccess,
                            Consumer<Exception> onFailure) {
        var request = new ShardChangesAction.Request(shardId, fromSeqNo, toSeqNo);
        shardReplicationService.getRemoteClusterClient(shardId.getIndex())
            .whenComplete((client, err) -> {
                if (err == null) {
                    client.execute(ShardChangesAction.INSTANCE, request, ActionListener.wrap(onSuccess, onFailure));
                } else {
                    onFailure.accept(Exceptions.toException(err));
                }
            });
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
                if (seqNoAlreadyRequested.get() == toSeqNoRequested) {
                    seqNoAlreadyRequested.set(toSeqNoReceived);
                } else {
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
                LOGGER.debug("[{}] observedSeqNoAtLeader: {}", shardId, observedSeqNoAtLeader.get());
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

        // Renew retention lease with global checkpoint so that any shard that picks up shard replication task
        // has data until then.
        // Method is called inside a transport thread (response listener), so dispatch away
        threadPool.executor(ThreadPool.Names.LOGICAL_REPLICATION).execute(
            () -> {
                shardReplicationService.getRemoteClusterClient(shardId.getIndex())
                    .thenAccept(client -> {
                        RetentionLeaseHelper.renewRetentionLease(
                            shardId,
                            toSeqNoReceived,
                            clusterName,
                            client,
                            ActionListener.wrap(
                                r -> {
                                    // schedule next poll
                                    cancellable = threadPool.schedule(
                                        this::requestBatchToFetch,
                                        replicationSettings.pollDelay(),
                                        ThreadPool.Names.LOGICAL_REPLICATION
                                    );
                                },
                                e -> {
                                    LOGGER.warn("Exception renewing retention lease.", e);
                                }
                            )
                        );
                    });
            }
        );
    }

    private void replayChanges(List<Translog.Operation> translogOps,
                               long maxSeqNoOfUpdatesOrDeletes,
                               Consumer<Void> onSuccess,
                               Consumer<Exception> onFailure) {
        if (translogOps.size() > 0) {
            var replayRequest = new ReplayChangesAction.Request(
                shardId,
                translogOps,
                maxSeqNoOfUpdatesOrDeletes
            );
            var listener = new ActionListener<ReplicationResponse>() {
                @Override
                public void onResponse(ReplicationResponse replayResponse) {
                    var shardInfo = replayResponse.getShardInfo();
                    if (shardInfo.getFailed() > 0) {
                        for (ReplicationResponse.ShardInfo.Failure failure : shardInfo.getFailures()) {
                            LOGGER.error("[{}] Failed replaying changes. Failure: {}", shardId, failure);
                        }
                        onFailure.accept(new RuntimeException("Some changes failed while replaying"));
                    }
                    onSuccess.accept(null);
                }

                @Override
                public void onFailure(Exception e) {
                    var msg = String.format(Locale.ENGLISH, "[%s] Changes cannot be replayed, tracking will stop", shardId);
                    LOGGER.error(msg, e);
                    onFailure.accept(new RuntimeException(msg));
                }
            };
            BiConsumer<ReplayChangesAction.Request, ActionListener<ReplicationResponse>> operation =
                (req, l) -> localClient.execute(ReplayChangesAction.INSTANCE, req, l);
            operation.accept(
                replayRequest,
                new RetryListener<>(
                    threadPool.scheduler(),
                    l -> operation.accept(replayRequest, l),
                    listener,
                    BackoffPolicy.exponentialBackoff()
                ));
        } else {
            onSuccess.accept(null);
        }
    }

    @Override
    public void close() throws IOException {
        if (cancellable != null) {
            cancellable.cancel();
        }
        shardReplicationService.getRemoteClusterClient(shardId.getIndex())
            .thenAccept(client -> {
                RetentionLeaseHelper.attemptRetentionLeaseRemoval(
                    shardId,
                    clusterName,
                    client,
                    ActionListener.wrap(() -> {})
                );
            });
    }
}
