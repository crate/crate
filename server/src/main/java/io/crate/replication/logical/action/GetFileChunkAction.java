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

import io.crate.replication.logical.repository.PublisherRestoreService;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.single.shard.TransportSingleShardAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportActionProxy;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

import javax.annotation.Nullable;
import java.io.IOException;

public class GetFileChunkAction extends ActionType<GetFileChunkAction.Response> {

    public static final String NAME = "internal:crate:replication/logical/file_chunk/get";
    public static final GetFileChunkAction INSTANCE = new GetFileChunkAction();

    public GetFileChunkAction() {
        super(NAME);
    }

    @Override
    public Writeable.Reader<Response> getResponseReader() {
        return Response::new;
    }

    @Singleton
    public static class TransportAction extends TransportSingleShardAction<Request, Response> {

        private final IndicesService indicesService;
        private final PublisherRestoreService publisherRestoreService;

        @Inject
        public TransportAction(ThreadPool threadPool,
                               ClusterService clusterService,
                               TransportService transportService,
                               IndicesService indicesService,
                               PublisherRestoreService publisherRestoreService,
                               IndexNameExpressionResolver indexNameExpressionResolver) {
            super(
                NAME,
                threadPool,
                clusterService,
                transportService,
                indexNameExpressionResolver,
                Request::new,
                ThreadPool.Names.GET
            );
            this.indicesService = indicesService;
            this.publisherRestoreService = publisherRestoreService;
            TransportActionProxy.registerProxyAction(transportService, NAME, Response::new);
        }

        @Override
        protected Response shardOperation(Request request,
                                          ShardId shardId) throws IOException {
            logger.debug(request.toString());
            var indexShard = indicesService.indexServiceSafe(shardId.getIndex()).getShard(shardId.id());
            var store = indexShard.store();
            var buffer = new byte[request.length()];
            var bytesRead = 0;

            store.incRef();
            var fileMetaData = request.storeFileMetadata();
            try (var currentInput = publisherRestoreService.openInputStream(
                request.restoreUUID(), request, fileMetaData.name(), fileMetaData.length())) {
                var offset = request.offset();
                if (offset < fileMetaData.length()) {
                    currentInput.skip(offset);
                    bytesRead = currentInput.read(buffer);
                }
            } finally {
                store.decRef();
            }

            return new Response(
                request.storeFileMetadata(),
                request.offset(),
                new BytesArray(buffer, 0, bytesRead)
            );
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
                                        InternalRequest request) {
            return state.routingTable().shardRoutingTable(request.request().publisherShardId()).primaryShardIt();
        }
    }

    public static class Request extends RestoreShardRequest<Request> {

        private final StoreFileMetadata storeFileMetadata;
        private final long offset;
        private final int length;

        public Request(String restoreUUID,
                       DiscoveryNode node,
                       ShardId publisherShardId,
                       String subscriberClusterName,
                       ShardId subscriberShardId,
                       StoreFileMetadata storeFileMetadata,
                       long offset,
                       int length) {
            super(restoreUUID, node, publisherShardId, subscriberClusterName, subscriberShardId);
            this.storeFileMetadata = storeFileMetadata;
            this.offset = offset;
            this.length = length;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            storeFileMetadata = new StoreFileMetadata(in);
            offset = in.readLong();
            length = in.readInt();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            storeFileMetadata.writeTo(out);
            out.writeLong(offset);
            out.writeInt(length);
        }

        public StoreFileMetadata storeFileMetadata() {
            return storeFileMetadata;
        }

        public long offset() {
            return offset;
        }

        public int length() {
            return length;
        }
    }

    public static class Response extends TransportResponse {

        private final StoreFileMetadata storeFileMetadata;
        private final long offset;
        private final BytesReference data;

        public Response(StoreFileMetadata storeFileMetadata,
                        long offset,
                        BytesReference data) {
            this.storeFileMetadata = storeFileMetadata;
            this.offset = offset;
            this.data = data;
        }

        public Response(StreamInput in) throws IOException {
            storeFileMetadata = new StoreFileMetadata(in);
            offset = in.readLong();
            data = in.readBytesReference();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            storeFileMetadata.writeTo(out);
            out.writeLong(offset);
            out.writeBytesReference(data);
        }

        public StoreFileMetadata storeFileMetadata() {
            return storeFileMetadata;
        }

        public long offset() {
            return offset;
        }

        public BytesReference data() {
            return data;
        }
    }
}
