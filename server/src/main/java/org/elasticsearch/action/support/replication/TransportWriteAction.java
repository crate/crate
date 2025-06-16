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

package org.elasticsearch.action.support.replication;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.PrimaryShardClosedException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.Translog.Location;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.jetbrains.annotations.Nullable;

import io.crate.exceptions.SQLExceptions;

/**
 * Base class for transport actions that modify data in some shard like index, delete, and shardBulk.
 * Allows performing async actions (e.g. refresh) after performing write operations on primary and replica shards
 */
public abstract class TransportWriteAction<
            Request extends ReplicationRequest<Request>,
            ReplicaRequest extends ReplicationRequest<ReplicaRequest>,
            Response extends ReplicationResponse
        > extends TransportReplicationAction<Request, ReplicaRequest, Response> {

    protected TransportWriteAction(Settings settings,
                                   String actionName,
                                   TransportService transportService,
                                   ClusterService clusterService,
                                   IndicesService indicesService,
                                   ThreadPool threadPool,
                                   ShardStateAction shardStateAction,
                                   Writeable.Reader<Request> reader,
                                   Writeable.Reader<ReplicaRequest> replicaReader,
                                   String executor,
                                   boolean forceExecutionOnPrimary) {
        super(
            settings,
            actionName,
            transportService,
            clusterService,
            indicesService,
            threadPool,
            shardStateAction,
            reader,
            replicaReader,
            executor,
            true,
            forceExecutionOnPrimary);
    }

    /** Syncs operation result to the translog or throws a shard not available failure */
    protected static Location syncOperationResultOrThrow(final Engine.Result operationResult,
                                                         final Location currentLocation) throws Exception {
        final Location location;
        if (operationResult.getFailure() != null) {
            // check if any transient write operation failures should be bubbled up
            Exception failure = operationResult.getFailure();
            assert failure instanceof MapperParsingException : "expected mapper parsing failures. got " + failure;
            throw failure;
        } else {
            location = locationToSync(currentLocation, operationResult.getTranslogLocation());
        }
        return location;
    }

    public static Location locationToSync(Location current, Location next) {
        /* here we are moving forward in the translog with each operation. Under the hood this might
         * cross translog files which is ok since from the user perspective the translog is like a
         * tape where only the highest location needs to be fsynced in order to sync all previous
         * locations even though they are not in the same file. When the translog rolls over files
         * the previous file is fsynced on after closing if needed.*/
        assert next != null : "next operation can't be null";
        assert current == null || current.compareTo(next) < 0 :
                "translog locations are not increasing";
        return next;
    }

    @Override
    protected ReplicationOperation.Replicas<ReplicaRequest> newReplicasProxy() {
        return new WriteActionReplicasProxy();
    }

    /**
     * Result of taking the action on the primary.
     */
    protected static class WritePrimaryResult<ReplicaRequest extends ReplicationRequest<ReplicaRequest>,
            Response extends ReplicationResponse> extends PrimaryResult<ReplicaRequest, Response> {
        public final Location location;
        public final IndexShard primary;

        public WritePrimaryResult(ReplicaRequest request,
                                  Response finalResponse,
                                  @Nullable Location location,
                                  IndexShard primary) {
            super(request, finalResponse);
            this.location = location;
            this.primary = primary;
        }

        @Override
        public void runPostReplicationActions(ActionListener<Void> listener) {
            /*
                * We call this after replication because this might wait for a refresh and that can take a while.
                * This way we wait for the refresh in parallel on the primary and on the replica.
                */
            new AsyncAfterWriteAction(primary, location, listener).run();
        }
    }

    /**
     * Result of taking the action on the replica.
     */
    protected static class WriteReplicaResult extends ReplicaResult {
        private final Location location;
        private final IndexShard replica;

        public WriteReplicaResult(@Nullable Location location,
                                  @Nullable Exception operationFailure,
                                  IndexShard replica) {
            super(operationFailure);
            this.location = location;
            this.replica = replica;
        }

        @Override
        public void runPostReplicaActions(ActionListener<Void> listener) {
            if (finalFailure == null) {
                new AsyncAfterWriteAction(replica, location, listener).run();
            } else {
                listener.onFailure(finalFailure);
            }
        }
    }

    @Override
    protected ClusterBlockLevel globalBlockLevel() {
        return ClusterBlockLevel.WRITE;
    }

    @Override
    public ClusterBlockLevel indexBlockLevel() {
        return ClusterBlockLevel.WRITE;
    }

    /**
     * This class encapsulates post write actions like async waits for
     * translog syncs or waiting for a refresh to happen making the write operation
     * visible.
     */
    static final class AsyncAfterWriteAction {
        private final Location location;
        private final boolean sync;
        private final AtomicInteger pendingOps = new AtomicInteger(1);
        private final AtomicReference<Exception> syncFailure = new AtomicReference<>(null);
        private final ActionListener<Void> listener;
        private final IndexShard indexShard;

        AsyncAfterWriteAction(final IndexShard indexShard,
                             @Nullable final Translog.Location location,
                             ActionListener<Void> listener) {
            this.indexShard = indexShard;
            this.location = location;
            this.listener = listener;
            if ((sync = indexShard.getTranslogDurability() == Translog.Durability.REQUEST && location != null)) {
                pendingOps.incrementAndGet();
            }
            assert pendingOps.get() >= 0 && pendingOps.get() <= 3 : "pendingOpts was: " + pendingOps.get();
        }

        /** calls the response listener if all pending operations have returned otherwise it just decrements the pending opts counter.*/
        private void maybeFinish() {
            final int numPending = pendingOps.decrementAndGet();
            if (numPending == 0) {
                Exception exception = syncFailure.get();
                if (exception == null) {
                    listener.onResponse(null);
                } else {
                    listener.onFailure(exception);
                }
            }
            assert numPending >= 0 && numPending <= 2 : "numPending must either 2, 1 or 0 but was " + numPending;
        }

        void run() {
            /*
             * We either respond immediately (i.e., if we do not fsync per request or wait for
             * refresh), or we there are past async operations and we wait for them to return to
             * respond.
             */
            indexShard.afterWriteOperation();
            // decrement pending by one, if there is nothing else to do we just respond with success
            maybeFinish();
            if (sync) {
                assert pendingOps.get() > 0;
                indexShard.sync(location, (ex) -> {
                    syncFailure.set(ex);
                    maybeFinish();
                });
            }
        }
    }

    /**
     * A proxy for <b>write</b> operations that need to be performed on the
     * replicas, where a failure to execute the operation should fail
     * the replica shard and/or mark the replica as stale.
     *
     * This extends {@code TransportReplicationAction.ReplicasProxy} to do the
     * failing and stale-ing.
     */
    class WriteActionReplicasProxy extends ReplicasProxy {

        @Override
        public void failShardIfNeeded(ShardRouting replica,
                                      long primaryTerm,
                                      String message,
                                      Exception exception,
                                      ActionListener<Void> listener) {
            if (SQLExceptions.isShardNotAvailable(exception) == false) {
                logger.warn(new ParameterizedMessage("[{}] {}", replica.shardId(), message), exception);
            }
            // If a write action fails due to the closure of the primary shard
            // then the replicas should not be marked as failed since they are
            // still up-to-date with the (now closed) primary shard
            if (exception instanceof PrimaryShardClosedException == false) {
                shardStateAction.remoteShardFailed(
                    replica.shardId(),
                    replica.allocationId().getId(),
                    primaryTerm,
                    true,
                    message,
                    exception,
                    listener
                );
            }
        }

        @Override
        public void markShardCopyAsStaleIfNeeded(ShardId shardId, String allocationId, long primaryTerm, ActionListener<Void> listener) {
            shardStateAction.remoteShardFailed(shardId, allocationId, primaryTerm, true, "mark copy as stale", null, listener);
        }
    }
}
