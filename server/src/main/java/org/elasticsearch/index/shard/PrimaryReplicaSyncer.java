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

package org.elasticsearch.index.shard;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.resync.ResyncReplicationRequest;
import org.elasticsearch.action.resync.TransportResyncReplicationAction;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportService;

import io.crate.common.io.IOUtils;

public class PrimaryReplicaSyncer {

    private static final Logger LOGGER = LogManager.getLogger(PrimaryReplicaSyncer.class);

    private final SyncAction syncAction;

    public static final ByteSizeValue DEFAULT_CHUNK_SIZE = new ByteSizeValue(512, ByteSizeUnit.KB);

    private volatile ByteSizeValue chunkSize = DEFAULT_CHUNK_SIZE;

    @Inject
    public PrimaryReplicaSyncer(TransportService transportService, TransportResyncReplicationAction syncAction) {
        this(syncAction);
    }

    // for tests
    public PrimaryReplicaSyncer(SyncAction syncAction) {
        this.syncAction = syncAction;
    }

    void setChunkSize(ByteSizeValue chunkSize) { // only settable for tests
        if (chunkSize.getBytes() <= 0) {
            throw new IllegalArgumentException("chunkSize must be > 0");
        }
        this.chunkSize = chunkSize;
    }

    public void resync(final IndexShard indexShard, final ActionListener<ResyncTask> listener) {
        Translog.Snapshot snapshot = null;
        try {
            final long startingSeqNo = indexShard.getLastKnownGlobalCheckpoint() + 1;
            final long maxSeqNo = indexShard.seqNoStats().getMaxSeqNo();
            final ShardId shardId = indexShard.shardId();
            // Wrap translog snapshot to make it synchronized as it is accessed by different threads through SnapshotSender.
            // Even though those calls are not concurrent, snapshot.next() uses non-synchronized state and is not multi-thread-compatible
            // Also fail the resync early if the shard is shutting down
            snapshot = indexShard.getHistoryOperations("resync",
                indexShard.indexSettings.isSoftDeleteEnabled() ? Engine.HistorySource.INDEX : Engine.HistorySource.TRANSLOG,
                startingSeqNo);
            final Translog.Snapshot originalSnapshot = snapshot;
            final Translog.Snapshot wrappedSnapshot = new Translog.Snapshot() {
                @Override
                public synchronized void close() throws IOException {
                    originalSnapshot.close();
                }

                @Override
                public synchronized int totalOperations() {
                    return originalSnapshot.totalOperations();
                }

                @Override
                public synchronized Translog.Operation next() throws IOException {
                    IndexShardState state = indexShard.state();
                    if (state == IndexShardState.CLOSED) {
                        throw new IndexShardClosedException(shardId);
                    } else {
                        assert state == IndexShardState.STARTED : "resync should only happen on a started shard, but state was: " + state;
                    }
                    return originalSnapshot.next();
                }
            };
            final ActionListener<ResyncTask> resyncListener = new ActionListener<ResyncTask>() {
                @Override
                public void onResponse(final ResyncTask resyncTask) {
                    try {
                        wrappedSnapshot.close();
                        listener.onResponse(resyncTask);
                    } catch (final Exception e) {
                        onFailure(e);
                    }
                }

                @Override
                public void onFailure(final Exception e) {
                    try {
                        wrappedSnapshot.close();
                    } catch (final Exception inner) {
                        e.addSuppressed(inner);
                    } finally {
                        listener.onFailure(e);
                    }
                }
            };
            // We must capture the timestamp after snapshotting a snapshot of operations to make sure
            // that the auto_id_timestamp of every operation in the snapshot is at most this value.
            final long maxSeenAutoIdTimestamp = indexShard.getMaxSeenAutoIdTimestamp();
            resync(shardId, indexShard.routingEntry().allocationId().getId(), indexShard.getPendingPrimaryTerm(), wrappedSnapshot,
                startingSeqNo, maxSeqNo, maxSeenAutoIdTimestamp, resyncListener);
        } catch (Exception e) {
            try {
                IOUtils.close(snapshot);
            } catch (IOException inner) {
                e.addSuppressed(inner);
            } finally {
                listener.onFailure(e);
            }
        }
    }

    private void resync(final ShardId shardId, final String primaryAllocationId, final long primaryTerm, final Translog.Snapshot snapshot,
                        long startingSeqNo, long maxSeqNo, long maxSeenAutoIdTimestamp, ActionListener<ResyncTask> listener) {
        ResyncTask resyncTask = new ResyncTask();
        ActionListener<Void> wrappedListener = new ActionListener<Void>() {
            @Override
            public void onResponse(Void ignore) {
                resyncTask.setPhase("finished");
                listener.onResponse(resyncTask);
            }

            @Override
            public void onFailure(Exception e) {
                resyncTask.setPhase("finished");
                listener.onFailure(e);
            }
        };
        try {
            new SnapshotSender(
                syncAction,
                resyncTask,
                shardId,
                primaryAllocationId,
                primaryTerm,
                snapshot,
                chunkSize.bytesAsInt(),
                startingSeqNo,
                maxSeqNo,
                maxSeenAutoIdTimestamp,
                wrappedListener
            ).run();
        } catch (Exception e) {
            wrappedListener.onFailure(e);
        }
    }

    public interface SyncAction {
        void sync(ResyncReplicationRequest request,
                  String primaryAllocationId,
                  long primaryTerm,
                  ActionListener<ReplicationResponse> listener);
    }

    static class SnapshotSender extends AbstractRunnable implements ActionListener<ReplicationResponse> {
        private final SyncAction syncAction;
        private final ResyncTask task; // to track progress
        private final String primaryAllocationId;
        private final long primaryTerm;
        private final ShardId shardId;
        private final Translog.Snapshot snapshot;
        private final long startingSeqNo;
        private final long maxSeqNo;
        private final long maxSeenAutoIdTimestamp;
        private final int chunkSizeInBytes;
        private final ActionListener<Void> listener;
        private final AtomicBoolean firstMessage = new AtomicBoolean(true);
        private final AtomicInteger totalSentOps = new AtomicInteger();
        private final AtomicInteger totalSkippedOps = new AtomicInteger();
        private final AtomicBoolean closed = new AtomicBoolean();

        SnapshotSender(SyncAction syncAction,
                       ResyncTask task,
                       ShardId shardId,
                       String primaryAllocationId,
                       long primaryTerm,
                       Translog.Snapshot snapshot,
                       int chunkSizeInBytes,
                       long startingSeqNo,
                       long maxSeqNo,
                       long maxSeenAutoIdTimestamp,
                       ActionListener<Void> listener) {
            this.syncAction = syncAction;
            this.task = task;
            this.shardId = shardId;
            this.primaryAllocationId = primaryAllocationId;
            this.primaryTerm = primaryTerm;
            this.snapshot = snapshot;
            this.chunkSizeInBytes = chunkSizeInBytes;
            this.startingSeqNo = startingSeqNo;
            this.maxSeqNo = maxSeqNo;
            this.maxSeenAutoIdTimestamp = maxSeenAutoIdTimestamp;
            this.listener = listener;
            task.setTotalOperations(snapshot.totalOperations());
        }

        @Override
        public void onResponse(ReplicationResponse response) {
            run();
        }

        @Override
        public void onFailure(Exception e) {
            if (closed.compareAndSet(false, true)) {
                listener.onFailure(e);
            }
        }

        private static final Translog.Operation[] EMPTY_ARRAY = new Translog.Operation[0];

        @Override
        protected void doRun() throws Exception {
            long size = 0;
            final List<Translog.Operation> operations = new ArrayList<>();

            task.setPhase("collecting_ops");
            task.setResyncedOperations(totalSentOps.get());
            task.setSkippedOperations(totalSkippedOps.get());

            Translog.Operation operation;
            while ((operation = snapshot.next()) != null) {
                final long seqNo = operation.seqNo();
                if (seqNo == SequenceNumbers.UNASSIGNED_SEQ_NO || seqNo < startingSeqNo) {
                    totalSkippedOps.incrementAndGet();
                    continue;
                }
                assert operation.seqNo() >= 0 : "sending operation with unassigned sequence number [" + operation + "]";
                operations.add(operation);
                size += operation.estimateSize();
                totalSentOps.incrementAndGet();

                // check if this request is past bytes threshold, and if so, send it off
                if (size >= chunkSizeInBytes) {
                    break;
                }
            }
            final long trimmedAboveSeqNo = firstMessage.get() ? maxSeqNo : SequenceNumbers.UNASSIGNED_SEQ_NO;
            // have to send sync request even in case of there are no operations to sync - have to sync trimmedAboveSeqNo at least
            if (!operations.isEmpty() || trimmedAboveSeqNo != SequenceNumbers.UNASSIGNED_SEQ_NO) {
                task.setPhase("sending_ops");
                ResyncReplicationRequest request =
                    new ResyncReplicationRequest(shardId, trimmedAboveSeqNo, maxSeenAutoIdTimestamp, operations.toArray(EMPTY_ARRAY));
                LOGGER.trace("{} sending batch of [{}][{}] (total sent: [{}], skipped: [{}])", shardId, operations.size(),
                    new ByteSizeValue(size), totalSentOps.get(), totalSkippedOps.get());
                firstMessage.set(false);
                syncAction.sync(request, primaryAllocationId, primaryTerm, this);
            } else if (closed.compareAndSet(false, true)) {
                LOGGER.trace("{} resync completed (total sent: [{}], skipped: [{}])", shardId, totalSentOps.get(), totalSkippedOps.get());
                listener.onResponse(null);
            }
        }
    }

    public static class ResyncRequest extends TransportRequest {

        private final ShardId shardId;
        private final String allocationId;

        public ResyncRequest(ShardId shardId, String allocationId) {
            this.shardId = shardId;
            this.allocationId = allocationId;
        }

        @Override
        public String getDescription() {
            return toString();
        }

        @Override
        public String toString() {
            return "ResyncRequest{ " + shardId + ", " + allocationId + " }";
        }
    }

    public static class ResyncTask {
        private volatile String phase = "starting";
        private volatile int totalOperations;
        private volatile int resyncedOperations;
        private volatile int skippedOperations;

        public ResyncTask() {
        }

        /**
         * Set the current phase of the task.
         */
        public void setPhase(String phase) {
            this.phase = phase;
        }

        /**
         * Get the current phase of the task.
         */
        public String getPhase() {
            return phase;
        }

        /**
         * total number of translog operations that were captured by translog snapshot
         */
        public int getTotalOperations() {
            return totalOperations;
        }

        public void setTotalOperations(int totalOperations) {
            this.totalOperations = totalOperations;
        }

        /**
         * number of operations that have been successfully replicated
         */
        public int getResyncedOperations() {
            return resyncedOperations;
        }

        public void setResyncedOperations(int resyncedOperations) {
            this.resyncedOperations = resyncedOperations;
        }

        /**
         * number of translog operations that have been skipped
         */
        public int getSkippedOperations() {
            return skippedOperations;
        }

        public void setSkippedOperations(int skippedOperations) {
            this.skippedOperations = skippedOperations;
        }
    }
}
