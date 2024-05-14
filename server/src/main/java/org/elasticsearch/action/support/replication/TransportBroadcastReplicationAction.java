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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.action.support.broadcast.BroadcastRequest;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.action.support.broadcast.BroadcastShardOperationFailedException;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.transport.TransportService;

import com.carrotsearch.hppc.cursors.IntObjectCursor;

import io.crate.exceptions.SQLExceptions;

/**
 * Base class for requests that should be executed on all shards of an index or several indices.
 * This action sends shard requests to all primary shards of the indices and they are then replicated like write requests
 */
public abstract class TransportBroadcastReplicationAction<Request extends BroadcastRequest<Request>, Response extends BroadcastResponse, ShardRequest extends ReplicationRequest<ShardRequest>, ShardResponse extends ReplicationResponse>
        extends HandledTransportAction<Request, Response> {

    private final ActionType<ShardResponse> replicatedBroadcastShardAction;
    private final ClusterService clusterService;
    private final NodeClient client;

    public TransportBroadcastReplicationAction(String name,
                                               Writeable.Reader<Request> reader,
                                               ClusterService clusterService,
                                               TransportService transportService,
                                               NodeClient client,
                                               ActionType<ShardResponse> replicatedBroadcastShardAction) {
        super(name, transportService, reader);
        this.client = client;
        this.replicatedBroadcastShardAction = replicatedBroadcastShardAction;
        this.clusterService = clusterService;
    }

    @Override
    protected void doExecute(Request request, ActionListener<Response> listener) {
        final ClusterState clusterState = clusterService.state();
        List<ShardId> shards = shards(request, clusterState);
        final CopyOnWriteArrayList<ShardResponse> shardsResponses = new CopyOnWriteArrayList<>();
        if (shards.size() == 0) {
            finishAndNotifyListener(listener, shardsResponses);
        }
        final CountDown responsesCountDown = new CountDown(shards.size());
        for (final ShardId shardId : shards) {
            ActionListener<ShardResponse> shardActionListener = new ActionListener<ShardResponse>() {
                @Override
                public void onResponse(ShardResponse shardResponse) {
                    shardsResponses.add(shardResponse);
                    logger.trace("{}: got response from {}", actionName, shardId);
                    if (responsesCountDown.countDown()) {
                        finishAndNotifyListener(listener, shardsResponses);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    logger.trace("{}: got failure from {}", actionName, shardId);
                    int totalNumCopies = clusterState.metadata().getIndexSafe(shardId.getIndex()).getNumberOfReplicas() + 1;
                    ShardResponse shardResponse = newShardResponse();
                    ReplicationResponse.ShardInfo.Failure[] failures;
                    if (TransportActions.isShardNotAvailableException(e)) {
                        failures = new ReplicationResponse.ShardInfo.Failure[0];
                    } else {
                        ReplicationResponse.ShardInfo.Failure failure = new ReplicationResponse.ShardInfo.Failure(shardId, null, e, SQLExceptions.status(e), true);
                        failures = new ReplicationResponse.ShardInfo.Failure[totalNumCopies];
                        Arrays.fill(failures, failure);
                    }
                    shardResponse.setShardInfo(new ReplicationResponse.ShardInfo(totalNumCopies, 0, failures));
                    shardsResponses.add(shardResponse);
                    if (responsesCountDown.countDown()) {
                        finishAndNotifyListener(listener, shardsResponses);
                    }
                }
            };
            shardExecute(request, shardId, shardActionListener);
        }
    }

    protected void shardExecute(Request request, ShardId shardId, ActionListener<ShardResponse> shardActionListener) {
        ShardRequest shardRequest = newShardRequest(request, shardId);
        client.execute(replicatedBroadcastShardAction, shardRequest).whenComplete(shardActionListener);
    }

    /**
     * @return all shard ids the request should run on
     */
    protected List<ShardId> shards(Request request, ClusterState clusterState) {
        List<ShardId> shardIds = new ArrayList<>();
        String[] concreteIndices = IndexNameExpressionResolver.concreteIndexNames(clusterState, request);
        for (String index : concreteIndices) {
            IndexMetadata indexMetadata = clusterState.metadata().indices().get(index);
            if (indexMetadata != null) {
                IndexRoutingTable indexRoutingTable = clusterState.routingTable().indicesRouting().get(index);
                for (IntObjectCursor<IndexShardRoutingTable> shardRouting : indexRoutingTable.getShards()) {
                    shardIds.add(shardRouting.value.shardId());
                }
            }
        }
        return shardIds;
    }

    protected abstract ShardResponse newShardResponse();

    protected abstract ShardRequest newShardRequest(Request request, ShardId shardId);

    private void finishAndNotifyListener(ActionListener listener, CopyOnWriteArrayList<ShardResponse> shardsResponses) {
        logger.trace("{}: got all shard responses", actionName);
        int successfulShards = 0;
        int failedShards = 0;
        int totalNumCopies = 0;
        List<DefaultShardOperationFailedException> shardFailures = null;
        for (int i = 0; i < shardsResponses.size(); i++) {
            ReplicationResponse shardResponse = shardsResponses.get(i);
            if (shardResponse == null) {
                // non active shard, ignore
            } else {
                failedShards += shardResponse.getShardInfo().getFailed();
                successfulShards += shardResponse.getShardInfo().getSuccessful();
                totalNumCopies += shardResponse.getShardInfo().getTotal();
                if (shardFailures == null) {
                    shardFailures = new ArrayList<>();
                }
                for (ReplicationResponse.ShardInfo.Failure failure : shardResponse.getShardInfo().getFailures()) {
                    shardFailures.add(new DefaultShardOperationFailedException(new BroadcastShardOperationFailedException(failure.fullShardId(), failure.getCause())));
                }
            }
        }
        listener.onResponse(newResponse(successfulShards, failedShards, totalNumCopies, shardFailures));
    }

    protected abstract BroadcastResponse newResponse(int successfulShards, int failedShards, int totalNumCopies,
                                                     List<DefaultShardOperationFailedException> shardFailures);
}
