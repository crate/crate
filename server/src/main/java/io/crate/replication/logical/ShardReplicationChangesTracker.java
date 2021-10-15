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

import io.crate.common.collections.Tuple;
import io.crate.replication.logical.action.ShardChangesAction;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.NodeNotConnectedException;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;

import static io.crate.replication.logical.action.ShardChangesAction.TransportAction.WAIT_FOR_NEW_OPS_TIMEOUT;

/**
 * Replicates batches of {@link org.elasticsearch.index.translog.Translog.Operation}'s to the subcribers target shards.
 *
 * Derived from org.opensearch.replication.task.shard.ShardReplicationChangesTracker
 */
public class ShardReplicationChangesTracker implements Closeable {

    private static final Logger LOGGER = Loggers.getLogger(ShardReplicationChangesTracker.class);

    private final ShardId shardId;
    private final LogicalReplicationSettings replicationSettings;
    private final ThreadPool threadPool;
    private final Function<String, Client> remoteClientResolver;
    private final ShardTranslogSequencer sequencer;
    private final List<Tuple<Long, Long>> missingBatches = Collections.synchronizedList(new ArrayList<>());
    private final AtomicLong observedSeqNoAtLeader;
    private final AtomicLong seqNoAlreadyRequested;
    private Scheduler.ScheduledCancellable cancellable;

    public ShardReplicationChangesTracker(IndexShard indexShard,
                                          LogicalReplicationSettings replicationSettings,
                                          ThreadPool threadPool,
                                          Function<String, Client> remoteClientResolver,
                                          Client client) {
        this.shardId = indexShard.shardId();
        this.replicationSettings = replicationSettings;
        this.threadPool = threadPool;
        this.remoteClientResolver = remoteClientResolver;
        this.sequencer = new ShardTranslogSequencer(shardId, client, indexShard.getLocalCheckpoint());
        observedSeqNoAtLeader = new AtomicLong(indexShard.getLocalCheckpoint());
        seqNoAlreadyRequested = new AtomicLong(indexShard.getLocalCheckpoint());
    }

    public void start() {
        LOGGER.debug("[{}] Spawning the shard changes reader", shardId);
        var executor = threadPool.executor(ThreadPool.Names.GENERIC);
        executor.execute(
            () -> requestBatchToFetch(batchToFetch -> {
                var fromSeqNo = batchToFetch.v1();
                var toSeqNo = batchToFetch.v2();
                try {
                    LOGGER.debug("[{}] Getting changes {}-{}", shardId, fromSeqNo, toSeqNo);
                    var changesResponse = getChanges(fromSeqNo, toSeqNo);
                    LOGGER.debug("[{}] Got {} changes starting from seqNo: {}",
                                 shardId, changesResponse.changes().size(), fromSeqNo);
                    sequencer.send(changesResponse);
                    LOGGER.debug("[{}] Pushed to sequencer {}-{}", shardId, fromSeqNo, toSeqNo);
                    long lastSeqNo = fromSeqNo - 1;
                    if (changesResponse.changes().isEmpty() == false) {
                        lastSeqNo = changesResponse.changes().get(changesResponse.changes().size() - 1).seqNo();
                    }
                    updateBatchFetched(
                        true,
                        fromSeqNo,
                        toSeqNo,
                        lastSeqNo,
                        changesResponse.lastSyncedGlobalCheckpoint()
                    );
                } catch (ElasticsearchTimeoutException e) {
                    LOGGER.debug("[{}] Timed out waiting for new changes. Current seqNo: {}", shardId, fromSeqNo);
                    updateBatchFetched(false, fromSeqNo, toSeqNo, fromSeqNo - 1, -1);
                } catch (NodeNotConnectedException e) {
                    LOGGER.info("[{}] Node not connected. Retrying request using a different node. {}",
                                shardId, e.getStackTrace());
                    // TODO: retry using different node
                    //delay(backOffForNodeDiscovery)
                    updateBatchFetched(false, fromSeqNo, toSeqNo, fromSeqNo - 1, -1);
                } catch (Exception e) {
                    LOGGER.error("[{}] Unable to get changes from seqNo: {}. {}",
                                 shardId, fromSeqNo, e.getStackTrace());
                    updateBatchFetched(false, fromSeqNo, toSeqNo, fromSeqNo - 1, -1);
                }

            })
        );
    }

    /**
     * Provides a range of operations to be fetched next.
     */
    public void requestBatchToFetch(Consumer<Tuple<Long, Long>> consumer) {
        LOGGER.debug("[{}] Waiting to get batch. requested: {}, leader: {}",
                     shardId, seqNoAlreadyRequested.get(), observedSeqNoAtLeader.get());

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
            LOGGER.debug("[{}] Fetching missing batch {}-{}", shardId, missingBatch.v1(), missingBatch.v2());
            consumer.accept(missingBatch);
        } else {
            // return the next batch to fetch and update seqNoAlreadyRequested.
            var batchSize = replicationSettings.batchSize();
            var fromSeq = seqNoAlreadyRequested.getAndAdd(batchSize) + 1;
            var toSeq = fromSeq + batchSize - 1;
            LOGGER.debug("[{}] Fetching the batch {}-{}", shardId, fromSeq, toSeq);
            consumer.accept(new Tuple<>(fromSeq, toSeq));
        }
    }

    private ShardChangesAction.Response getChanges(Long fromSeqNo, Long toSeqNo) {
        var remoteClient = remoteClientResolver.apply(shardId.getIndexName());
        if (remoteClient == null) {
            throw new RuntimeException("Could not resolve a remote client for index=" + shardId.getIndexName());
        }
        var request = new ShardChangesAction.Request(shardId, fromSeqNo, toSeqNo);
        return remoteClient.execute(ShardChangesAction.INSTANCE, request).actionGet(WAIT_FOR_NEW_OPS_TIMEOUT);
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
            LOGGER.debug("Updating the batch fetched. {}-{}/{}, seqNoAtLeader:{}",
                         fromSeqNoRequested, toSeqNoReceived, toSeqNoRequested, seqNoAtLeader);

            // If we didn't get the complete batch that we had requested.
            if (toSeqNoRequested > toSeqNoReceived) {
                // If this is the last batch being fetched, update the seqNoAlreadyRequested.
                if (seqNoAlreadyRequested.get() == toSeqNoRequested) {
                    seqNoAlreadyRequested.set(toSeqNoReceived);
                } else {
                    // Else, add to the missing operations to missing batch
                    LOGGER.debug("Didn't get the complete batch. Adding the missing operations {}-{}",
                                 toSeqNoReceived + 1, toSeqNoRequested);
                    missingBatches.add(new Tuple<>(toSeqNoReceived + 1, toSeqNoRequested));
                }
            }

            // Update the sequence number observed at leader.
            observedSeqNoAtLeader.getAndUpdate(value -> Math.max(seqNoAtLeader, value));
            LOGGER.debug("observedSeqNoAtLeader: {}", observedSeqNoAtLeader.get());
        } else {
            // If this is the last batch being fetched, update the seqNoAlreadyRequested.
            if (seqNoAlreadyRequested.get() == toSeqNoRequested) {
                seqNoAlreadyRequested.set(fromSeqNoRequested - 1);
            } else {
                // If this was not the last batch, we might have already fetched other batch of
                // operations after this. Adding this to missing.
                LOGGER.debug("Adding batch to missing {}-{}", fromSeqNoRequested, toSeqNoRequested);
                missingBatches.add(new Tuple<>(fromSeqNoRequested, toSeqNoRequested));
            }
        }
    }

    @Override
    public void close() throws IOException {
        if (cancellable != null) {
            cancellable.cancel();
        }
    }
}
