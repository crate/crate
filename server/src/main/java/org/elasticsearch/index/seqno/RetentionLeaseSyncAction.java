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

package org.elasticsearch.index.seqno;

import java.io.IOException;
import java.util.Objects;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.support.replication.ReplicationRequest;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.support.replication.TransportWriteAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardClosedException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 * Write action responsible for syncing retention leases to replicas. This action is deliberately a write action so that if a replica misses
 * a retention lease sync then that shard will be marked as stale.
 */
public class RetentionLeaseSyncAction extends
        TransportWriteAction<RetentionLeaseSyncAction.Request, RetentionLeaseSyncAction.Request, ReplicationResponse> {

    public static String ACTION_NAME = "indices:admin/seq_no/retention_lease_sync";

    private static final Logger LOGGER = LogManager.getLogger(RetentionLeaseSyncAction.class);

    protected Logger getLogger() {
        return LOGGER;
    }

    @Inject
    public RetentionLeaseSyncAction(
            final Settings settings,
            final TransportService transportService,
            final ClusterService clusterService,
            final IndicesService indicesService,
            final ThreadPool threadPool,
            final ShardStateAction shardStateAction,
            final IndexNameExpressionResolver indexNameExpressionResolver) {
        super(
            ACTION_NAME,
            transportService,
            clusterService,
            indicesService,
            threadPool,
            shardStateAction,
            indexNameExpressionResolver,
            RetentionLeaseSyncAction.Request::new,
            RetentionLeaseSyncAction.Request::new,
            ThreadPool.Names.MANAGEMENT);
    }

    /**
     * Sync the specified retention leases for the specified shard. The callback is invoked when the sync succeeds or fails.
     *
     * @param shardId         the shard to sync
     * @param retentionLeases the retention leases to sync
     * @param listener        the callback to invoke when the sync completes normally or abnormally
     */
    public void syncRetentionLeasesForShard(
            final ShardId shardId,
            final RetentionLeases retentionLeases,
            final ActionListener<ReplicationResponse> listener) {
        Objects.requireNonNull(shardId);
        Objects.requireNonNull(retentionLeases);
        Objects.requireNonNull(listener);
        execute(
            new RetentionLeaseSyncAction.Request(shardId, retentionLeases),
            ActionListener.wrap(
                listener::onResponse,
                e -> {
                    if (ExceptionsHelper.unwrap(e, AlreadyClosedException.class, IndexShardClosedException.class) == null) {
                        getLogger().warn(new ParameterizedMessage("{} retention lease sync failed", shardId), e);
                    }
                    listener.onFailure(e);
                }
            )
        );
    }

    @Override
    protected WritePrimaryResult<Request, ReplicationResponse> shardOperationOnPrimary(final Request request, final IndexShard primary) {
        Objects.requireNonNull(request);
        Objects.requireNonNull(primary);
        // we flush to ensure that retention leases are committed
        flush(primary);
        return new WritePrimaryResult<>(request, new ReplicationResponse(), null, null, primary);
    }

    @Override
    protected WriteReplicaResult<Request> shardOperationOnReplica(final Request request, final IndexShard replica) {
        Objects.requireNonNull(request);
        Objects.requireNonNull(replica);
        replica.updateRetentionLeasesOnReplica(request.getRetentionLeases());
        // we flush to ensure that retention leases are committed
        flush(replica);
        return new WriteReplicaResult<>(request, null, null, replica, logger);
    }

    private void flush(final IndexShard indexShard) {
        final FlushRequest flushRequest = new FlushRequest();
        flushRequest.force(true);
        flushRequest.waitIfOngoing(true);
        indexShard.flush(flushRequest);
    }

    public static final class Request extends ReplicationRequest<Request> {

        private RetentionLeases retentionLeases;

        public RetentionLeases getRetentionLeases() {
            return retentionLeases;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            retentionLeases = new RetentionLeases(in);
        }

        public Request(ShardId shardId, RetentionLeases retentionLeases) {
            super(Objects.requireNonNull(shardId));
            this.retentionLeases = Objects.requireNonNull(retentionLeases);
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            super.writeTo(Objects.requireNonNull(out));
            retentionLeases.writeTo(out);
        }

        @Override
        public String toString() {
            return "Request{" +
                    "retentionLeases=" + retentionLeases +
                    ", shardId=" + shardId +
                    ", timeout=" + timeout +
                    ", index='" + index + '\'' +
                    ", waitForActiveShards=" + waitForActiveShards +
                    '}';
        }

    }

    @Override
    protected ReplicationResponse read(StreamInput in) throws IOException {
        return new ReplicationResponse(in);
    }
}
