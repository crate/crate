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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.replication.ReplicationRequest;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.support.replication.TransportReplicationAction;
import org.elasticsearch.action.support.replication.TransportWriteAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.StrictDynamicMappingException;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.jetbrains.annotations.VisibleForTesting;

import io.crate.common.unit.TimeValue;
import io.crate.replication.logical.engine.SubscriberEngine;
import io.crate.replication.logical.exceptions.InvalidShardEngineException;

public class ReplayChangesAction extends ActionType<ReplicationResponse> {

    public static final String NAME = "internal:crate:replication/logical/shard/changes/replay";

    public static final ReplayChangesAction INSTANCE = new ReplayChangesAction();

    public ReplayChangesAction() {
        super(NAME);
    }

    @Singleton
    public static class TransportAction extends TransportWriteAction<Request, Request, ReplicationResponse> {

        private static final Logger LOGGER = LogManager.getLogger(ReplayChangesAction.class);

        @Inject
        public TransportAction(Settings settings,
                               TransportService transportService,
                               ClusterService clusterService,
                               IndicesService indicesService,
                               ThreadPool threadPool,
                               ShardStateAction shardStateAction) {
            super(settings,
                  NAME,
                  transportService,
                  clusterService,
                  indicesService,
                  threadPool,
                  shardStateAction,
                  Request::new,
                  Request::new,
                  ThreadPool.Names.WRITE,
                  false);
        }

        @Override
        protected ReplicationResponse newResponseInstance(StreamInput in) throws IOException {
            return new ReplicationResponse(in);
        }

        @Override
        protected void shardOperationOnPrimary(Request request,
                                               IndexShard primary,
                                               ActionListener<PrimaryResult<Request, ReplicationResponse>> listener) {
            performOnPrimary(primary, request, new ArrayList<>(),0, results -> {
                ArrayList<Translog.Operation> replicaOps = new ArrayList<>();
                Translog.Location location = null;
                try {
                    for (int i = 0; i < results.size(); i++) {
                        var engineResult = results.get(i);
                        var failure = engineResult.getFailure();
                        if (failure instanceof InvalidShardEngineException) {
                            if (location == null) {
                                // Bail out if the failure happened on the first item -> nothing to replicate
                                listener.onFailure(failure);
                            }
                            break;
                        }
                        // Add successfully on primary written operation to the to-replicate operations
                        replicaOps.add(request.changes().get(i));
                        location = syncOperationResultOrThrow(engineResult, location);
                    }
                } catch (Exception e) {
                    listener.onFailure(e);
                }
                listener.onResponse(new WritePrimaryResult<>(
                    new Request(primary.shardId(), replicaOps, request.maxSeqNoOfUpdatesOrDeletes()),
                    new ReplicationResponse(),
                    location,
                    null,
                    primary)
                );
            }, listener::onFailure);
        }

        @VisibleForTesting
        void performOnPrimary(IndexShard primary,
                              Request request,
                              List<Engine.Result> accumulator,
                              int offset,
                              Consumer<List<Engine.Result>> result,
                              Consumer<Exception> failure) {
            for (int i = offset; i < request.changes().size(); i++) {
                final var op = request.changes().get(i);
                final var opWithPrimary = withPrimaryTerm(op, primary.getOperationPrimaryTerm());
                if (primary.getMaxSeqNoOfUpdatesOrDeletes() < request.maxSeqNoOfUpdatesOrDeletes()) {
                    primary.advanceMaxSeqNoOfUpdatesOrDeletes(request.maxSeqNoOfUpdatesOrDeletes());
                }

                if ((primary.getEngineOrNull() instanceof SubscriberEngine) == false) {
                    accumulator.add(
                        new Engine.IndexResult(new InvalidShardEngineException(primary.shardId()), Versions.MATCH_ANY)
                    );
                    break;
                }

                Engine.Result engineResult = null;
                try {
                    engineResult = primary.applyTranslogOperation(opWithPrimary, Engine.Operation.Origin.PRIMARY);
                } catch (IOException e) {
                    failure.accept(e);
                    return;
                }
                if (engineResult.getResultType() == Engine.Result.Type.MAPPING_UPDATE_REQUIRED ||
                    (engineResult.getResultType() == Engine.Result.Type.FAILURE &&
                     engineResult.getFailure() instanceof StrictDynamicMappingException)
                ) {
                    // Retry to process the translog operations, once the mappings are updated
                    retryWhenMappingsAreUpdated(primary, request, accumulator, i, result, failure);
                    return;
                }
                accumulator.add(engineResult);
            }
            result.accept(accumulator);
        }

        private void retryWhenMappingsAreUpdated(IndexShard primary,
                                                 Request request,
                                                 List<Engine.Result> accumulator,
                                                 int offset,
                                                 Consumer<List<Engine.Result>> result,
                                                 Consumer<Exception> failure) {
            var indexName = request.shardId().getIndexName();
            var currentMappingVersion = clusterService.state().metadata().index(indexName).getMappingVersion();
            var clusterStateObserver = new ClusterStateObserver(clusterService, new TimeValue(60_000), LOGGER);
            clusterStateObserver.waitForNextChange(
                new ClusterStateObserver.Listener() {
                    @Override
                    public void onNewClusterState(ClusterState state) {
                        performOnPrimary(primary, request, accumulator, offset, result, failure);
                    }

                    @Override
                    public void onClusterServiceClose() {
                        failure.accept(new IllegalStateException("ClusterService was closed while waiting for mapping update"));
                    }

                    @Override
                    public void onTimeout(TimeValue timeout) {
                        failure.accept(new ElasticsearchTimeoutException("Mappings did not update in time"));
                    }

                }, cs -> isIndexMetadataUpdated(cs, indexName, currentMappingVersion)
            );
        }

        private static boolean isIndexMetadataUpdated(ClusterState cs, String indexName, long mappingVersion) {
            var indexMetadata = cs.metadata().index(indexName);
            if (indexMetadata == null) {
                return false;
            } else {
                var newMappingVersion = indexMetadata.getMappingVersion();
                return newMappingVersion > mappingVersion;
            }
        }

        @Override
        protected WriteReplicaResult<Request> shardOperationOnReplica(Request request, IndexShard replica) throws Exception {
            Translog.Location location = performOnReplica(request, replica);
            return new WriteReplicaResult<>(location, null, replica);
        }

        private Translog.Location performOnReplica(Request request, IndexShard replica) throws Exception {
            Translog.Location location = null;
            for (var translogOp : request.changes()) {
                final var op = withPrimaryTerm(translogOp, replica.getOperationPrimaryTerm());
                var result = replica.applyTranslogOperation(op, Engine.Operation.Origin.REPLICA);
                if (result.getResultType() == Engine.Result.Type.MAPPING_UPDATE_REQUIRED) {
                    throw new TransportReplicationAction.RetryOnReplicaException(
                        replica.shardId(), "Mappings are not available on the replica yet, triggered update");
                }
                location = syncOperationResultOrThrow(result, location);
            }
            return location;
        }

        private static Translog.Operation withPrimaryTerm(Translog.Operation op, long operationPrimaryTerm) {
            return switch (op.opType()) {
                case CREATE, INDEX -> {
                    var sourceOp = (Translog.Index) op;
                    yield new Translog.Index(
                        sourceOp.id(),
                        sourceOp.seqNo(),
                        operationPrimaryTerm,
                        sourceOp.version(),
                        BytesReference.toBytes(sourceOp.getSource()),
                        Translog.UNSET_AUTO_GENERATED_TIMESTAMP
                    );
                }
                case DELETE -> {
                    var sourceOp = (Translog.Delete) op;
                    yield new Translog.Delete(
                        sourceOp.id(),
                        sourceOp.uid(),
                        sourceOp.seqNo(),
                        operationPrimaryTerm,
                        sourceOp.version()
                    );
                }
                case NO_OP -> {
                    var sourceOp = (Translog.NoOp) op;
                    yield new Translog.NoOp(
                        sourceOp.seqNo(),
                        operationPrimaryTerm,
                        sourceOp.reason()
                    );
                }
            };
        }
    }

    public static class Request extends ReplicationRequest<Request> {

        private final List<Translog.Operation> changes;
        private final long maxSeqNoOfUpdatesOrDeletes;

        public Request(ShardId shardId,
                       List<Translog.Operation> changes,
                       long maxSeqNoOfUpdatesOrDeletes) {
            super(shardId);
            this.changes = changes;
            this.maxSeqNoOfUpdatesOrDeletes = maxSeqNoOfUpdatesOrDeletes;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            changes = in.readList(Translog.Operation::readOperation);
            maxSeqNoOfUpdatesOrDeletes = in.readLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeCollection(changes, Translog.Operation::writeOperation);
            out.writeLong(maxSeqNoOfUpdatesOrDeletes);
        }

        public List<Translog.Operation> changes() {
            return changes;
        }

        public long maxSeqNoOfUpdatesOrDeletes() {
            return maxSeqNoOfUpdatesOrDeletes;
        }

        @Override
        public String toString() {
            return "ReplayChangesRequest{" +
                   "changes=" + changes +
                   ", maxSeqNoOfUpdatesOrDeletes=" + maxSeqNoOfUpdatesOrDeletes +
                   ", shardId=" + shardId +
                   '}';
        }
    }
}
