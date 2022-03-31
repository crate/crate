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
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.replication.ReplicationRequest;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.support.replication.TransportWriteAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.gateway.WriteStateException;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardClosedException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

/**
 * Write action responsible for syncing retention leases to replicas. This action is deliberately a write action so that if a replica misses
 * a retention lease sync then that shard will be marked as stale.
 */
public class RetentionLeaseSyncAction extends
        TransportWriteAction<RetentionLeaseSyncAction.Request, RetentionLeaseSyncAction.Request, ReplicationResponse> {

    public static final String ACTION_NAME = "indices:admin/seq_no/retention_lease_sync";
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
            final ShardStateAction shardStateAction) {
        super(
            settings,
            ACTION_NAME,
            transportService,
            clusterService,
            indicesService,
            threadPool,
            shardStateAction,
            RetentionLeaseSyncAction.Request::new,
            RetentionLeaseSyncAction.Request::new,
            ThreadPool.Names.MANAGEMENT,
            false);
    }

    @Override
    protected void doExecute(Request request, ActionListener<ReplicationResponse> listener) {
        assert false : "use RetentionLeaseSyncAction#sync";
    }

    final void sync(ShardId shardId, String primaryAllocationId, long primaryTerm, RetentionLeases retentionLeases,
                    ActionListener<ReplicationResponse> listener) {
        final Request request = new Request(shardId, retentionLeases);
        transportService.sendChildRequest(
            clusterService.localNode(),
            transportPrimaryAction,
            new ConcreteShardRequest<>(request, primaryAllocationId, primaryTerm),
            transportOptions,
            new TransportResponseHandler<ReplicationResponse>() {
                @Override
                public ReplicationResponse read(StreamInput in) throws IOException {
                    return newResponseInstance(in);
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.SAME;
                }

                @Override
                public void handleResponse(ReplicationResponse response) {
                    listener.onResponse(response);
                }

                @Override
                public void handleException(TransportException e) {
                    if (ExceptionsHelper.unwrap(e, AlreadyClosedException.class, IndexShardClosedException.class) == null) {
                        getLogger().warn(new ParameterizedMessage("{} retention lease sync failed", shardId), e);
                    }
                    listener.onFailure(e);
                }
            });
    }

    @Override
    protected void shardOperationOnPrimary(Request request,
                                           IndexShard primary,
                                           ActionListener<PrimaryResult<Request, ReplicationResponse>> listener) {
        ActionListener.completeWith(listener, () -> {
            assert request.waitForActiveShards().equals(ActiveShardCount.NONE) : request.waitForActiveShards();
            Objects.requireNonNull(request);
            Objects.requireNonNull(primary);
            primary.persistRetentionLeases();
            return new WritePrimaryResult<>(request, new ReplicationResponse(), null, null, primary);
        });
    }

    @Override
    protected WriteReplicaResult<Request> shardOperationOnReplica(final Request request, final IndexShard replica) throws WriteStateException {
        Objects.requireNonNull(request);
        Objects.requireNonNull(replica);
        replica.updateRetentionLeasesOnReplica(request.getRetentionLeases());
        replica.persistRetentionLeases();
        return new WriteReplicaResult<>(request, null, null, replica, getLogger());
    }

    @Override
    public ClusterBlockLevel indexBlockLevel() {
        return null;
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
            waitForActiveShards(ActiveShardCount.NONE);
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            super.writeTo(Objects.requireNonNull(out));
            retentionLeases.writeTo(out);
        }

        @Override
        public String toString() {
            return "RetentionLeaseSyncAction.Request{" +
                    "retentionLeases=" + retentionLeases +
                    ", shardId=" + shardId +
                    ", timeout=" + timeout +
                    ", index='" + index + '\'' +
                    ", waitForActiveShards=" + waitForActiveShards +
                    '}';
        }

    }

    @Override
    protected ReplicationResponse newResponseInstance(StreamInput in) throws IOException {
        return new ReplicationResponse(in);
    }
}
