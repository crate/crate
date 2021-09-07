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
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.single.shard.TransportSingleShardAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportActionProxy;
import org.elasticsearch.transport.TransportService;

import javax.annotation.Nullable;
import java.io.IOException;

public class ReleasePublisherResourcesAction extends ActionType<AcknowledgedResponse> {

    public static final String NAME = "internal:crate:replication/logical/resources/release";
    public static final ReleasePublisherResourcesAction INSTANCE = new ReleasePublisherResourcesAction();

    public ReleasePublisherResourcesAction() {
        super(NAME);
    }

    @Override
    public Writeable.Reader<AcknowledgedResponse> getResponseReader() {
        return AcknowledgedResponse::new;
    }

    @Singleton
    public static class TransportAction extends TransportSingleShardAction<Request, AcknowledgedResponse> {

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
                ThreadPool.Names.GET
            );
            this.publisherRestoreService = publisherRestoreService;
            TransportActionProxy.registerProxyAction(
                transportService,
                NAME,
                AcknowledgedResponse::new
            );
        }

        @Override
        protected AcknowledgedResponse shardOperation(Request request,
                                                      ShardId shardId) throws IOException {
            LOGGER.info("Releasing resources for {} with restore-id as {}", shardId, request.restoreUUID());
            publisherRestoreService.removeRestoreContext(request.restoreUUID());
            return new AcknowledgedResponse(true);
        }

        @Override
        protected Writeable.Reader<AcknowledgedResponse> getResponseReader() {
            return AcknowledgedResponse::new;
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
}
