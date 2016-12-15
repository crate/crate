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

package io.crate.blob.exceptions;

import io.crate.blob.v2.BlobIndex;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Stream;

/**
 * Keeps track of http urls of other nodes in the Cluster to provide redirects for blob operations
 */
public class RedirectService extends AbstractLifecycleComponent {

    private final ClusterService clusterService;
    private final ConcurrentMap<String, String> nodeIdToHttpAddr = new ConcurrentHashMap<>();
    private final Client client;
    private Listener listener = null;

    @Inject
    public RedirectService(Settings settings, ClusterService clusterService, Client client) {
        super(settings);
        this.clusterService = clusterService;
        this.client = client;
    }

    private void fetchHttpAddress(Stream<String> newNodeIds) {
        final String[] nodeIds = newNodeIds.toArray(String[]::new);
        client.admin().cluster().nodesInfo(
            new NodesInfoRequest(nodeIds).http(true), new ActionListener<NodesInfoResponse>() {
                @Override
                public void onResponse(NodesInfoResponse nodesInfoResponse) {
                    fillMapFromResponse(nodesInfoResponse, nodeIds);
                }

                @Override
                public void onFailure(Exception e) {

                }
            }
        );
    }

    private void fillMapFromResponse(NodesInfoResponse nodeInfos, String[] nodeIds) {
        Map<String, NodeInfo> nodesMap = nodeInfos.getNodesMap();
        for (Map.Entry<String, NodeInfo> entry : nodesMap.entrySet()) {
            nodeIdToHttpAddr.put(entry.getKey(), getHttpAddress(entry.getValue()));
        }
    }

    private String getHttpAddress(NodeInfo nodeInfo) {
        TransportAddress publishAddress = nodeInfo.getHttp().address().publishAddress();
        return publishAddress.getHost() + ":" + publishAddress.getPort();
    }


    /**
     * @param index  the name of blob-enabled index
     * @param digest sha-1 hash value of the file
     * @return null if no redirect is required, Otherwise the address to which should be redirected.
     */
    public String getRedirectAddress(String index, String digest) throws MissingHTTPEndpointException {
        ClusterState state = clusterService.state();
        ShardIterator shards = clusterService.operationRouting().getShards(
            state, index, null, digest, "_local");
        DiscoveryNodes nodes = state.getNodes();
        String localNodeId = nodes.getLocalNodeId();
        ShardRouting shard;
        while ((shard = shards.nextOrNull()) != null) {
            if (!shard.active()) {
                continue;
            }
            String shardNodeId = shard.currentNodeId();
            if (shardNodeId.equals(localNodeId)) {
                // no redirect required if the shard is on this node
                return null;
            }
            String httpAddress;
            // containsKey check because it's necessary to differentiate between "no entry yet" and "no http url"
            if (nodeIdToHttpAddr.containsKey(shardNodeId)) {
                httpAddress = nodeIdToHttpAddr.get(shardNodeId);
                if (httpAddress == null) {
                    continue;
                }
            } else {
                // this case mostly occurs during node-startup
                // where the nodeIdToHttpAddr map isn't fully initialized
                NodesInfoResponse nodeInfos = client.admin().cluster().nodesInfo(new NodesInfoRequest(shardNodeId)).actionGet();
                httpAddress = getHttpAddress(nodeInfos.getNodes().iterator().next());
                nodeIdToHttpAddr.put(shardNodeId, httpAddress);
            }
            return httpAddress + "/_blobs/" + BlobIndex.stripPrefix(index) + "/" + digest;
        }
        throw new MissingHTTPEndpointException("Can't find a suitable http server to serve the blob");
    }

    @Override
    protected void doStart() {
        listener = new Listener();
        clusterService.add(listener);
    }

    @Override
    protected void doStop() {
        if (listener != null) {
            clusterService.remove(listener);
        }
    }

    @Override
    protected void doClose() {

    }

    private class Listener implements ClusterStateListener {

        @Override
        public void clusterChanged(ClusterChangedEvent event) {
            if (!event.nodesChanged()) {
                return;
            }
            DiscoveryNodes.Delta delta = event.nodesDelta();
            fetchHttpAddress(delta.addedNodes()
                .stream()
                .map(DiscoveryNode::getId)
            );
            for (DiscoveryNode removedNode : delta.removedNodes()) {
                nodeIdToHttpAddr.remove(removedNode.getId());
            }
        }
    }
}
