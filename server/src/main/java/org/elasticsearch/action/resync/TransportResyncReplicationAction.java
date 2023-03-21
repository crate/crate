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

package org.elasticsearch.action.resync;

import java.io.IOException;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.replication.ReplicationOperation;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.support.replication.TransportReplicationAction;
import org.elasticsearch.action.support.replication.TransportWriteAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.PrimaryReplicaSyncer;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

public class TransportResyncReplicationAction extends TransportWriteAction<ResyncReplicationRequest,
    ResyncReplicationRequest, ReplicationResponse> implements PrimaryReplicaSyncer.SyncAction {

    private static final String ACTION_NAME = "internal:index/seq_no/resync";

    @Inject
    public TransportResyncReplicationAction(Settings settings,
                                            TransportService transportService,
                                            ClusterService clusterService,
                                            IndicesService indicesService,
                                            ThreadPool threadPool,
                                            ShardStateAction shardStateAction) {
        super(
            settings,
            ACTION_NAME,
            transportService,
            clusterService,
            indicesService,
            threadPool,
            shardStateAction,
            ResyncReplicationRequest::new,
            ResyncReplicationRequest::new,
            ThreadPool.Names.WRITE,
            true /* we should never reject resync because of thread pool capacity on primary */
        );
    }

    @Override
    public ClusterBlockLevel indexBlockLevel() {
        // resync should never be blocked because it's an internal action
        return null;
    }

    @Override
    protected ReplicationResponse newResponseInstance(StreamInput in) throws IOException {
        return new ReplicationResponse(in);
    }

    @Override
    protected ReplicationOperation.Replicas<ResyncReplicationRequest> newReplicasProxy() {
        return new ResyncActionReplicasProxy();
    }

    @Override
    protected void shardOperationOnPrimary(ResyncReplicationRequest request,
                                           IndexShard primary,
                                           ActionListener<PrimaryResult<ResyncReplicationRequest, ReplicationResponse>> listener) {
        ActionListener.completeWith(
            listener,
            () -> new WritePrimaryResult<>(performOnPrimary(request, primary), new ReplicationResponse(), null, null, primary)
        );
    }

    public static ResyncReplicationRequest performOnPrimary(ResyncReplicationRequest request, IndexShard primary) {
        return request;
    }

    @Override
    protected WriteReplicaResult<ResyncReplicationRequest> shardOperationOnReplica(ResyncReplicationRequest request,
                                                                                   IndexShard replica) throws Exception {
        Translog.Location location = performOnReplica(request, replica);
        return new WriteReplicaResult<>(location, null, replica);
    }

    public static Translog.Location performOnReplica(ResyncReplicationRequest request, IndexShard replica) throws Exception {
        Translog.Location location = null;
        /*
         * Operations received from resync do not have auto_id_timestamp individually, we need to bootstrap this max_seen_timestamp
         * (at least the highest timestamp from any of these operations) to make sure that we will disable optimization for the same
         * append-only requests with timestamp (sources of these operations) that are replicated; otherwise we may have duplicates.
         */
        replica.updateMaxUnsafeAutoIdTimestamp(request.getMaxSeenAutoIdTimestampOnPrimary());
        for (Translog.Operation operation : request.getOperations()) {
            final Engine.Result operationResult = replica.applyTranslogOperation(operation, Engine.Operation.Origin.REPLICA);
            if (operationResult.getResultType() == Engine.Result.Type.MAPPING_UPDATE_REQUIRED) {
                throw new TransportReplicationAction.RetryOnReplicaException(replica.shardId(),
                    "Mappings are not available on the replica yet, triggered update: " + operationResult.getRequiredMappingUpdate());
            }
            location = syncOperationResultOrThrow(operationResult, location);
        }
        if (request.getTrimAboveSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO) {
            replica.trimOperationOfPreviousPrimaryTerms(request.getTrimAboveSeqNo());
        }
        return location;
    }

    @Override
    public void sync(ResyncReplicationRequest request,
                     String primaryAllocationId,
                     long primaryTerm,
                     ActionListener<ReplicationResponse> listener) {
        // skip reroute phase
        transportService.sendChildRequest(
            clusterService.localNode(),
            transportPrimaryAction,
            new ConcreteShardRequest<>(request, primaryAllocationId, primaryTerm),
            transportOptions,
            new TransportResponseHandler<ReplicationResponse>() {

                @Override
                public ReplicationResponse read(StreamInput in) throws IOException {
                    return TransportResyncReplicationAction.this.newResponseInstance(in);
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.SAME;
                }

                @Override
                public void handleResponse(ReplicationResponse response) {
                    final ReplicationResponse.ShardInfo.Failure[] failures = response.getShardInfo().getFailures();
                    // noinspection ForLoopReplaceableByForEach
                    for (int i = 0; i < failures.length; i++) {
                        final ReplicationResponse.ShardInfo.Failure f = failures[i];
                        logger.info(
                                new ParameterizedMessage(
                                        "{} primary-replica resync to replica on node [{}] failed", f.fullShardId(), f.nodeId()),
                                f.getCause());
                    }
                    listener.onResponse(response);
                }

                @Override
                public void handleException(TransportException exp) {
                    listener.onFailure(exp);
                }
            });
    }

    /**
     * A proxy for primary-replica resync operations which are performed on replicas when a new primary is promoted.
     * Replica shards fail to execute resync operations will be failed but won't be marked as stale.
     * This avoids marking shards as stale during cluster restart but enforces primary-replica resync mandatory.
     */
    class ResyncActionReplicasProxy extends ReplicasProxy {

        @Override
        public void failShardIfNeeded(ShardRouting replica, long primaryTerm, String message, Exception exception,
                                      ActionListener<Void> listener) {
            shardStateAction.remoteShardFailed(
                replica.shardId(), replica.allocationId().getId(), primaryTerm, false, message, exception, listener);
        }
    }
}
