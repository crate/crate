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
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.single.shard.TransportSingleShardAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportActionProxy;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

import javax.annotation.Nullable;
import java.io.IOException;

public class GetStoreMetadataAction extends ActionType<GetStoreMetadataAction.Response> {

    public static final String NAME = "internal:crate:replication/logical/store/file_metadata";
    public static final GetStoreMetadataAction INSTANCE = new GetStoreMetadataAction();

    public GetStoreMetadataAction() {
        super(NAME);
    }

    @Override
    public Writeable.Reader<Response> getResponseReader() {
        return Response::new;
    }

    @Singleton
    public static class TransportAction extends TransportSingleShardAction<Request, Response> {

        private static final Logger LOGGER = Loggers.getLogger(TransportAction.class);

        private final PublisherRestoreService publisherRestoreService;

        @Inject
        public TransportAction(ThreadPool threadPool,
                               ClusterService clusterService,
                               TransportService transportService,
                               PublisherRestoreService publisherRestoreService,
                               IndexNameExpressionResolver indexNameExpressionResolver) {
            super(
                NAME,
                threadPool,
                clusterService,
                transportService,
                indexNameExpressionResolver,
                Request::new,
                ThreadPool.Names.LOGICAL_REPLICATION
            );
            this.publisherRestoreService = publisherRestoreService;
            TransportActionProxy.registerProxyAction(transportService, NAME, Response::new);
        }

        @Override
        protected Response shardOperation(Request request,
                                          ShardId shardId) throws IOException {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("Processing request: " + request.toString());
            }
            var metadataSnapshot = publisherRestoreService.createRestoreContext(
                request.restoreUUID(),
                request
            ).metadataSnapshot();
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("MetadataSnapshot: {}", metadataSnapshot.asMap());
            }
            return new Response(metadataSnapshot);
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

        public Request(String restoreUUID,
                       DiscoveryNode node,
                       ShardId publisherShardId,
                       String subscriberClusterName,
                       ShardId subscriberShardId) {
            super(restoreUUID, node, publisherShardId, subscriberClusterName, subscriberShardId);
        }

        public Request(StreamInput in) throws IOException {
            super(in);
        }
    }

    public static class Response extends TransportResponse {

        private final Store.MetadataSnapshot metadataSnapshot;

        public Response(Store.MetadataSnapshot metadataSnapshot) {
            this.metadataSnapshot = metadataSnapshot;
        }

        public Response(StreamInput in) throws IOException {
            metadataSnapshot = new Store.MetadataSnapshot(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            metadataSnapshot.writeTo(out);
        }

        public Store.MetadataSnapshot metadataSnapshot() {
            return metadataSnapshot;
        }
    }
}
