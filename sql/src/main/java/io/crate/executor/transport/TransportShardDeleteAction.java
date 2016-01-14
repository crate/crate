/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.executor.transport;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;

@Singleton
public class TransportShardDeleteAction extends TransportShardAction<ShardDeleteRequest> {

    private final static String ACTION_NAME = "indices:crate/data/write/delete";

    @Inject
    public TransportShardDeleteAction(Settings settings,
                                      TransportService transportService,
                                      ClusterService clusterService,
                                      IndicesService indicesService,
                                      ThreadPool threadPool,
                                      ShardStateAction shardStateAction,
                                      ActionFilters actionFilters) {
        super(settings, ACTION_NAME, transportService, clusterService, indicesService, threadPool, shardStateAction, actionFilters);
    }

    @Override
    protected ShardDeleteRequest newRequestInstance() {
        return new ShardDeleteRequest();
    }

    @Override
    protected ShardDeleteRequest newReplicaRequestInstance() {
        return new ShardDeleteRequest();
    }

    @Override
    protected boolean checkWriteConsistency() {
        return true;
    }

    @Override
    protected ShardIterator shards(ClusterState clusterState, InternalRequest request) {
        return clusterState.routingTable().index(request.concreteIndex()).shard(request.request().shardId()).shardsIt();
    }

    @Override
    protected ShardResponse processRequestItems(ShardId shardId, ShardDeleteRequest request, AtomicBoolean killed) {
        ShardResponse shardResponse = new ShardResponse();
        IndexService indexService = indicesService.indexServiceSafe(request.index());
        IndexShard indexShard = indexService.shardSafe(request.shardId());
        for (int i = 0; i < request.itemIndices().size(); i++) {
            int location = request.itemIndices().get(i);
            ShardDeleteRequest.Item item = request.items().get(i);
            if (killed.get()) {
                throw new CancellationException();
            }
            try {
                boolean found = shardDeleteOperationOnPrimary(request, item, indexShard);
                if (found) {
                    logger.debug("{} successfully deleted [{}]/[{}]", request.shardId(), request.type(), item.id());
                    shardResponse.add(location);
                } else {
                    logger.debug("{} failed to execute delete for [{}]/[{}], doc not found",
                            request.shardId(), request.type(), item.id());
                    shardResponse.add(location,
                            new ShardResponse.Failure(
                                    item.id(),
                                    "Document not found while deleting",
                                    false));

                }
            } catch (Throwable t) {
                if (!TransportActions.isShardNotAvailableException(t)) {
                    throw t;
                } else {
                    logger.debug("{} failed to execute delete for [{}]/[{}]",
                            t, request.shardId(), request.type(), item.id());
                    shardResponse.add(location,
                            new ShardResponse.Failure(
                                    item.id(),
                                    ExceptionsHelper.detailedMessage(t),
                                    (t instanceof VersionConflictEngineException)));
                }
            }
        }

        return shardResponse;
    }

    @Override
    protected void processRequestItemsOnReplica(ShardId shardId, ShardDeleteRequest request, AtomicBoolean killed) {
        IndexService indexService = indicesService.indexServiceSafe(request.index());
        IndexShard indexShard = indexService.shardSafe(request.shardId());
        for (ShardDeleteRequest.Item item : request.items()) {
            if (killed.get()) {
                throw new CancellationException();
            }
            try {
                Engine.Delete delete = indexShard.prepareDelete(request.type(), item.id(), item.version(), item.versionType(), Engine.Operation.Origin.REPLICA);
                indexShard.delete(delete);
                logger.trace("{} REPLICA: successfully deleted [{}]/[{}]", request.shardId(), request.type(), item.id());
            } catch (Throwable e) {
                // if its not an ignore replica failure, we need to make sure to bubble up the failure
                // so we will fail the shard
                if (!ignoreReplicaException(e)) {
                    throw e;
                }
            }
        }

    }

    private boolean shardDeleteOperationOnPrimary(ShardDeleteRequest request, ShardDeleteRequest.Item item, IndexShard indexShard) {
        Engine.Delete delete = indexShard.prepareDelete(request.type(), item.id(), item.version(), item.versionType(), Engine.Operation.Origin.PRIMARY);
        indexShard.delete(delete);
        // update the request with the version so it will go to the replicas
        item.versionType(delete.versionType().versionTypeForReplicationAndRecovery());
        item.version(delete.version());

        assert item.versionType().validateVersionForWrites(item.version());

        return delete.found();
    }
}
