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

package org.elasticsearch.action.admin.cluster.node.stats;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.nodes.BaseNodeRequest;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.node.NodeService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;

public class TransportNodesStatsAction extends TransportNodesAction<NodesStatsRequest, NodesStatsResponse, TransportNodesStatsAction.NodeStatsRequest, NodeStats> {

    private final NodeService nodeService;

    @Inject
    public TransportNodesStatsAction(ThreadPool threadPool,
                                     ClusterService clusterService,
                                     TransportService transportService,
                                     NodeService nodeService,
                                     IndexNameExpressionResolver indexNameExpressionResolver) {
        super(NodesStatsAction.NAME,
              threadPool,
              clusterService,
              transportService,
              indexNameExpressionResolver,
              NodesStatsRequest::new,
              NodeStatsRequest::new,
              ThreadPool.Names.MANAGEMENT,
              NodeStats.class
        );
        this.nodeService = nodeService;
    }

    @Override
    protected NodesStatsResponse newResponse(NodesStatsRequest request, List<NodeStats> responses, List<FailedNodeException> failures) {
        return new NodesStatsResponse(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected NodeStatsRequest newNodeRequest(String nodeId, NodesStatsRequest request) {
        return new NodeStatsRequest(nodeId, request);
    }

    @Override
    protected NodeStats read(StreamInput in) throws IOException {
        return new NodeStats(in);
    }

    @Override
    protected NodeStats nodeOperation(NodeStatsRequest nodeStatsRequest) {
        return nodeService.stats();
    }

    public static class NodeStatsRequest extends BaseNodeRequest {

        NodesStatsRequest request;

        public NodeStatsRequest(StreamInput in) throws IOException {
            super(in);
            this.request = new NodesStatsRequest(in);
        }

        public NodeStatsRequest(String nodeId, NodesStatsRequest request) {
            super(nodeId);
            this.request = request;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            request.writeTo(out);
        }
    }
}
