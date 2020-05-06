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

package org.elasticsearch.gateway;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.nodes.BaseNodeRequest;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import javax.annotation.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import io.crate.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;

public class TransportNodesListGatewayMetaState extends TransportNodesAction<TransportNodesListGatewayMetaState.Request,
                                                                             TransportNodesListGatewayMetaState.NodesGatewayMetaState,
                                                                             TransportNodesListGatewayMetaState.NodeRequest,
                                                                             TransportNodesListGatewayMetaState.NodeGatewayMetaState> {

    public static final String ACTION_NAME = "internal:gateway/local/meta_state";

    private final GatewayMetaState metaState;

    @Inject
    public TransportNodesListGatewayMetaState(ThreadPool threadPool,
                                              ClusterService clusterService,
                                              TransportService transportService,
                                              IndexNameExpressionResolver indexNameExpressionResolver,
                                              GatewayMetaState metaState) {
        super(ACTION_NAME, threadPool, clusterService, transportService, indexNameExpressionResolver,
            Request::new, NodeRequest::new, ThreadPool.Names.GENERIC, NodeGatewayMetaState.class);
        this.metaState = metaState;
    }

    public ActionFuture<NodesGatewayMetaState> list(DiscoveryNode[] discoveryNodes, @Nullable TimeValue timeout) {
        PlainActionFuture<NodesGatewayMetaState> future = PlainActionFuture.newFuture();
        execute(new Request(discoveryNodes).timeout(timeout), future);
        return future;
    }

    @Override
    protected NodeRequest newNodeRequest(String nodeId, Request request) {
        return new NodeRequest(nodeId);
    }

    @Override
    protected NodeGatewayMetaState read(StreamInput in) throws IOException {
        return new NodeGatewayMetaState(in);
    }

    @Override
    protected NodesGatewayMetaState newResponse(Request request, List<NodeGatewayMetaState> responses, List<FailedNodeException> failures) {
        return new NodesGatewayMetaState(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected NodeGatewayMetaState nodeOperation(NodeRequest request) {
        return new NodeGatewayMetaState(clusterService.localNode(), metaState.getMetaData());
    }

    public static class Request extends BaseNodesRequest<Request> {

        public Request(DiscoveryNode... nodes) {
            super(nodes);
        }

        public Request(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
        }
    }

    public static class NodesGatewayMetaState extends BaseNodesResponse<NodeGatewayMetaState> {

        public NodesGatewayMetaState(ClusterName clusterName, List<NodeGatewayMetaState> nodes, List<FailedNodeException> failures) {
            super(clusterName, nodes, failures);
        }

        @Override
        protected void writeNodesTo(StreamOutput out, List<NodeGatewayMetaState> nodes) throws IOException {
            out.writeList(nodes);
        }
    }

    public static class NodeRequest extends BaseNodeRequest {

        NodeRequest(String nodeId) {
            super(nodeId);
        }

        public NodeRequest(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
        }
    }

    public static class NodeGatewayMetaState extends BaseNodeResponse {

        @Nullable
        private final MetaData metaData;

        public NodeGatewayMetaState(DiscoveryNode node, MetaData metaData) {
            super(node);
            this.metaData = metaData;
        }

        public NodeGatewayMetaState(StreamInput in) throws IOException {
            super(in);
            if (in.readBoolean()) {
                metaData = MetaData.readFrom(in);
            } else {
                metaData = null;
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            if (metaData == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                metaData.writeTo(out);
            }
        }

        @Nullable
        public MetaData metaData() {
            return metaData;
        }
    }
}
