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

import io.crate.common.unit.TimeValue;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.WriteResponse;
import org.elasticsearch.action.support.replication.ReplicationRequest;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.support.replication.TransportReplicationAction;
import org.elasticsearch.action.support.replication.TransportWriteAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportActionProxy;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class ReplayChangesAction extends ActionType<ReplayChangesAction.Response> {

    public static final String NAME = "internal:crate:replication/logical/shard/changes/replay";

    public static final ReplayChangesAction INSTANCE = new ReplayChangesAction();

    public ReplayChangesAction() {
        super(NAME);
    }

    @Singleton
    public static class TransportAction
        extends TransportWriteAction<Request, Request, Response> {

        private static final Logger LOGGER = Loggers.getLogger(ReplayChangesAction.class);

        @Inject
        public TransportAction(TransportService transportService,
                               ClusterService clusterService,
                               IndicesService indicesService,
                               ThreadPool threadPool,
                               ShardStateAction shardStateAction) {
            super(NAME,
                  transportService,
                  clusterService,
                  indicesService,
                  threadPool,
                  shardStateAction,
                  Request::new,
                  Request::new,
                  ThreadPool.Names.WRITE,
                  false);

            TransportActionProxy.registerProxyAction(transportService, NAME, Response::new);
        }

        @Override
        protected Response newResponseInstance(StreamInput in) throws IOException {
            return new Response(in);
        }

        @Override
        protected void shardOperationOnPrimary(Request request,
                                               IndexShard primary,
                                               ActionListener<PrimaryResult<Request, Response>> listener) {
            ActionListener.completeWith(
                listener,
                () -> new WritePrimaryResult<>(
                    request,
                    new Response(),
                    performOnPrimary(request, primary),
                    null,
                    primary
                )
            );
        }

        private static Engine.Result applyTransLogOperation(Translog.Operation op, IndexShard primary, Request request) {
            final var opWithPrimary = withPrimaryTerm(op, primary.getOperationPrimaryTerm());
            if (primary.getMaxSeqNoOfUpdatesOrDeletes() < request.maxSeqNoOfUpdatesOrDeletes()) {
                primary.advanceMaxSeqNoOfUpdatesOrDeletes(request.maxSeqNoOfUpdatesOrDeletes());
            }
            try {
                return primary.applyTranslogOperation(opWithPrimary, Engine.Operation.Origin.PRIMARY);
            } catch(IOException e) {
                throw new RuntimeException(e);
            }
        }

        private void waitMappingUpdateInClusterState(Translog.Operation op, IndexShard primary, Request request, Exception failure, Consumer<Engine.Result> result) {
            final var indexName = request.shardId().getIndexName();
            Metadata metadata = clusterService.state().metadata();
            var currentMappingVersion = metadata.index(indexName).getMappingVersion();
            ClusterStateObserver clusterStateObserver = new ClusterStateObserver(clusterService, LOGGER);
            clusterStateObserver.waitForNextChange(
                new ClusterStateObserver.Listener() {
                    @Override
                    public void onNewClusterState(ClusterState state) {
                        result.accept(applyTransLogOperation(op, primary, request));
                    }

                    @Override
                    public void onClusterServiceClose() {}

                    @Override
                    public void onTimeout(TimeValue timeout) {
                        LOGGER.warn(failure);
                    }

                }, cs -> isIndexMetaDataOnClusterStateUpdated(cs, request.shardId().getIndexName(), currentMappingVersion)
                , TimeValue.timeValueSeconds(10)
            );
        }

        private static boolean isIndexMetaDataOnClusterStateUpdated(ClusterState cs, String indexName, long mappingVersion) {
            var newMappingVersion = cs.metadata().index(indexName).getMappingVersion();
            return newMappingVersion > mappingVersion;
        }

        public Translog.Location performOnPrimary(Request request, IndexShard primary) throws Exception {
            Translog.Location location = null;
            var result = new AtomicReference<Engine.Result>();
            for (var op : request.changes()) {
                result.getAndUpdate(x -> applyTransLogOperation(op, primary, request));
                if (result.get().getResultType() == Engine.Result.Type.MAPPING_UPDATE_REQUIRED ||
                    result.get().getResultType() == Engine.Result.Type.FAILURE) {
                    waitMappingUpdateInClusterState(op,
                                                    primary,
                                                    request,
                                                    result.get().getFailure(),
                                                    r -> result.getAndUpdate(l -> r));
                }
                location =  syncOperationResultOrThrow(result.get(), location);
            }
            return location;
        }

        @Override
        protected WriteReplicaResult<Request> shardOperationOnReplica(Request request,
                                                                      IndexShard replica) throws Exception {
            Translog.Location location = performOnReplica(request, replica);
            return new WriteReplicaResult<>(request, location, null, replica, logger);
        }

        private Translog.Location performOnReplica(Request request,
                                                   IndexShard replica) throws Exception {
            Translog.Location location = null;

            for (var o : request.changes()) {
                final var op = withPrimaryTerm(o, replica.getOperationPrimaryTerm());
                var result = replica.applyTranslogOperation(op, Engine.Operation.Origin.REPLICA);
                if (result.getResultType() == Engine.Result.Type.MAPPING_UPDATE_REQUIRED) {
                    throw new TransportReplicationAction.RetryOnReplicaException(
                        replica.shardId(),
                        "Mappings are not available on the replica yet, triggered update: " + result.getRequiredMappingUpdate());
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
                        BytesReference.toBytes(sourceOp.source()),
                        sourceOp.routing(),
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

    public static class Response extends ReplicationResponse implements WriteResponse {

        public Response() {
        }

        public Response(StreamInput in) throws IOException {
            super(in);
        }
    }
}
