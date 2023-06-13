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

package io.crate.replication.logical.action;

import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeoutException;

import org.jetbrains.annotations.Nullable;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.single.shard.SingleShardRequest;
import org.elasticsearch.action.support.single.shard.TransportSingleShardAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.MissingHistoryOperationsException;
import org.elasticsearch.index.seqno.RetentionLease;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.shard.GlobalCheckpointListeners.GlobalCheckpointListener;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportActionProxy;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

import io.crate.common.unit.TimeValue;
import io.crate.metadata.RelationName;
import io.crate.replication.logical.exceptions.MissingShardOperationsException;

public class ShardChangesAction extends ActionType<ShardChangesAction.Response> {

    public static final String NAME = "internal:crate:replication/logical/shard/changes";
    public static final ShardChangesAction INSTANCE = new ShardChangesAction();

    public ShardChangesAction() {
        super(NAME);
    }

    @Override
    public Writeable.Reader<Response> getResponseReader() {
        return Response::new;
    }

    @Singleton
    public static class TransportAction extends TransportSingleShardAction<Request, Response> {

        public static final TimeValue WAIT_FOR_NEW_OPS_TIMEOUT = TimeValue.timeValueMinutes(1);
        private static final Logger LOGGER = Loggers.getLogger(TransportAction.class);

        private final IndicesService indicesService;

        @Inject
        public TransportAction(ThreadPool threadPool,
                               ClusterService clusterService,
                               TransportService transportService,
                               IndicesService indicesService) {
            super(NAME,
                  threadPool,
                  clusterService,
                  transportService,
                  Request::new,
                  ThreadPool.Names.SEARCH);
            this.indicesService = indicesService;
            TransportActionProxy.registerProxyAction(transportService, NAME, Response::new);
        }

        @Override
        protected Response shardOperation(Request request, ShardId shardId) throws IOException {
            var indexService = indicesService.indexServiceSafe(shardId.getIndex());
            var indexShard = indexService.getShard(shardId.id());
            var seqNoStats = indexShard.seqNoStats();
            // At this point globalCheckpoint is at least fromSeqNo
            var toSeqNo = Math.min(seqNoStats.getGlobalCheckpoint(), request.toSeqNo());
            var fromSeqNo = request.fromSeqNo();

            List<Translog.Operation> ops = new ArrayList<>();

            // TODO: read changes from translog, see org.opensearch.replication.seqno.RemoteClusterTranslogService

            if (LOGGER.isTraceEnabled()) {
                LOGGER.info("[{}] Fetching changes from lucene for {} - from:{}, to:{}",
                            shardId,
                            request.shardId(),
                            request.fromSeqNo(),
                            toSeqNo
                );
            }
            var source = "logical-replication";
            try (Translog.Snapshot snapshot = indexShard.newChangesSnapshot(source, fromSeqNo, toSeqNo, true)) {
                var op = snapshot.next();
                while (op != null) {
                    ops.add(op);
                    op = snapshot.next();
                }
            } catch (MissingHistoryOperationsException e) {
                final Collection<RetentionLease> retentionLeases = indexShard.getRetentionLeases().leases();
                final String message = "Operations are no longer available for replicating. " +
                                       "Existing retention leases [" + retentionLeases + "]; maybe increase the retention lease period setting " +
                                       "[" + IndexSettings.INDEX_SOFT_DELETES_RETENTION_LEASE_PERIOD_SETTING.getKey() + "]?";
                throw new MissingShardOperationsException(RelationName.fromIndexName(shardId.getIndexName()), message);
            }

            return new Response(
                ops,
                fromSeqNo,
                indexShard.getMaxSeqNoOfUpdatesOrDeletes(),
                seqNoStats.getGlobalCheckpoint(),
                indexService.getMetadata().getVersion()
            );
        }

        @Override
        protected void asyncShardOperation(Request request,
                                           ShardId shardId,
                                           ActionListener<Response> listener) throws IOException {
            var indexShard = indicesService.indexServiceSafe(shardId.getIndex()).getShard(shardId.id());
            final SeqNoStats seqNoStats = indexShard.seqNoStats();
            if (seqNoStats.getGlobalCheckpoint() < request.fromSeqNo()) {
                // There are no new operations to sync. Do a long poll and wait for GlobalCheckpoint to advance. If
                // the checkpoint doesn't advance by the timeout this throws an ESTimeoutException which the caller
                // should catch and start a new poll.

                if (logger.isTraceEnabled()) {
                    LOGGER.trace("[{}] Waiting for globalCheckpoint to advance from {} to {}",
                                 shardId,
                                 seqNoStats.getGlobalCheckpoint(),
                                 request.fromSeqNo()
                    );
                }

                indexShard.addGlobalCheckpointListener(
                    request.fromSeqNo(),
                    new GlobalCheckpointListener() {

                        @Override
                        public Executor executor() {
                            return threadPool.executor(ThreadPool.Names.LISTENER);
                        }

                        @Override
                        public void accept(long globalCheckpoint, Exception e) {
                            if (globalCheckpoint != UNASSIGNED_SEQ_NO) {
                                // At this point indexShard.lastKnownGlobalCheckpoint  has advanced but it may not yet have been synced
                                // to the translog, which means we can't return those changes. Return to the caller to retry.
                                // TODO: Figure out a better way to wait for the global checkpoint to be synced to the translog
                                if (globalCheckpoint < request.fromSeqNo()) {
                                    assert globalCheckpoint >
                                        indexShard.getLastSyncedGlobalCheckpoint() : "Checkpoint didn't advance at all";
                                    listener.onFailure(new ElasticsearchTimeoutException("global checkpoint not synced."));
                                    return;
                                }
                                try {
                                    TransportAction.super.asyncShardOperation(request, shardId, listener);
                                } catch (IOException ioException) {
                                    listener.onFailure(ioException);
                                }
                            } else {
                                assert e != null : "Exception expected if globalCheckout != " + UNASSIGNED_SEQ_NO;
                                if (e instanceof TimeoutException) {
                                    LOGGER.trace("Waiting for advanced globalCheckpoint timed out", e);
                                    var latestSeqNoStats = indexShard.seqNoStats();
                                    var indexService = indicesService.indexServiceSafe(shardId.getIndex());
                                    var response = new Response(
                                        List.of(),
                                        request.fromSeqNo(),
                                        indexShard.getMaxSeqNoOfUpdatesOrDeletes(),
                                        latestSeqNoStats.getGlobalCheckpoint(),
                                        indexService.getMetadata().getVersion()
                                    );
                                    listener.onResponse(response);
                                } else {
                                    listener.onFailure(e);
                                }
                            }
                        }
                    },
                    WAIT_FOR_NEW_OPS_TIMEOUT
                );

            } else {
                super.asyncShardOperation(request, shardId, listener);
            }
        }

        @Override
        protected Writeable.Reader<Response> getResponseReader() {
            return Response::new;
        }

        @Override
        protected boolean resolveIndex(Request request) {
            return true;
        }

        @Nullable
        @Override
        protected ShardsIterator shards(ClusterState state,
                                        TransportSingleShardAction<Request, Response>.InternalRequest request) {
            return state.routingTable().shardRoutingTable(
                request.request().shardId().getIndexName(),
                request.request().shardId().id()
            ).activeInitializingShardsRandomIt();
        }
    }

    public static class Request extends SingleShardRequest<Request> {

        private final ShardId shardId;
        private final long fromSeqNo;
        private final long toSeqNo;

        public Request(ShardId shardId, long fromSeqNo, long toSeqNo) {
            super(shardId.getIndexName());
            this.shardId = shardId;
            this.fromSeqNo = fromSeqNo;
            this.toSeqNo = toSeqNo;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.shardId = new ShardId(in);
            this.fromSeqNo = in.readLong();
            this.toSeqNo = in.readVLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            shardId.writeTo(out);
            out.writeLong(fromSeqNo);
            out.writeVLong(toSeqNo);
        }

        public ShardId shardId() {
            return shardId;
        }

        public long fromSeqNo() {
            return fromSeqNo;
        }

        public long toSeqNo() {
            return toSeqNo;
        }

        @Override
        public String toString() {
            return "Request{shardId=" + shardId + ", fromSeqNo=" + fromSeqNo + ", toSeqNo=" + toSeqNo + "}";
        }
    }

    public static class Response extends TransportResponse {

        private final List<Translog.Operation> changes;
        private final long fromSeqNo;
        private final long maxSeqNoOfUpdatesOrDeletes;
        private final long lastSyncedGlobalCheckpoint;
        private final long version;

        public Response(List<Translog.Operation> changes,
                        long fromSeqNo,
                        long maxSeqNoOfUpdatesOrDeletes,
                        long lastSyncedGlobalCheckpoint,
                        long version) {
            this.changes = changes;
            this.fromSeqNo = fromSeqNo;
            this.maxSeqNoOfUpdatesOrDeletes = maxSeqNoOfUpdatesOrDeletes;
            this.lastSyncedGlobalCheckpoint = lastSyncedGlobalCheckpoint;
            this.version = version;
        }

        public Response(StreamInput in) throws IOException {
            changes = in.readList(Translog.Operation::readOperation);
            fromSeqNo = in.readVLong();
            maxSeqNoOfUpdatesOrDeletes = in.readLong();
            lastSyncedGlobalCheckpoint = in.readLong();
            version = in.readVLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeCollection(changes, Translog.Operation::writeOperation);
            out.writeVLong(fromSeqNo);
            out.writeLong(maxSeqNoOfUpdatesOrDeletes);
            out.writeLong(lastSyncedGlobalCheckpoint);
            out.writeVLong(version);
        }

        public List<Translog.Operation> changes() {
            return changes;
        }

        public long fromSeqNo() {
            return fromSeqNo;
        }

        public long maxSeqNoOfUpdatesOrDeletes() {
            return maxSeqNoOfUpdatesOrDeletes;
        }

        public long lastSyncedGlobalCheckpoint() {
            return lastSyncedGlobalCheckpoint;
        }

        public long version() {
            return version;
        }
    }
}
