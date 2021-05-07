/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.blob;

import io.crate.blob.exceptions.MissingHTTPEndpointException;
import io.crate.blob.recovery.BlobRecoveryHandler;
import io.crate.blob.transfer.BlobHeadRequestHandler;
import io.crate.blob.v2.BlobIndex;
import io.crate.blob.v2.BlobIndicesService;
import io.crate.plugin.PipelineRegistry;
import io.crate.protocols.http.HttpBlobHandler;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.recovery.PeerRecoverySourceService;
import org.elasticsearch.transport.TransportService;

import static org.elasticsearch.indices.recovery.RecoverySettings.INDICES_RECOVERY_MAX_CONCURRENT_FILE_CHUNKS_SETTING;

public class BlobService extends AbstractLifecycleComponent {

    private final BlobIndicesService blobIndicesService;
    private final BlobHeadRequestHandler blobHeadRequestHandler;
    private final PeerRecoverySourceService peerRecoverySourceService;
    private final ClusterService clusterService;
    private final TransportService transportService;
    private final BlobTransferTarget blobTransferTarget;
    private final Client client;
    private final PipelineRegistry pipelineRegistry;
    private final Settings settings;

    @Inject
    public BlobService(ClusterService clusterService,
                       BlobIndicesService blobIndicesService,
                       BlobHeadRequestHandler blobHeadRequestHandler,
                       PeerRecoverySourceService peerRecoverySourceService,
                       TransportService transportService,
                       BlobTransferTarget blobTransferTarget,
                       Client client,
                       PipelineRegistry pipelineRegistry,
                       Settings settings) {
        this.clusterService = clusterService;
        this.blobIndicesService = blobIndicesService;
        this.blobHeadRequestHandler = blobHeadRequestHandler;
        this.peerRecoverySourceService = peerRecoverySourceService;
        this.transportService = transportService;
        this.blobTransferTarget = blobTransferTarget;
        this.client = client;
        this.pipelineRegistry = pipelineRegistry;
        this.settings = settings;
    }

    public RemoteDigestBlob newBlob(String index, String digest) {
        assert client != null : "client for remote digest blob must not be null";
        ShardId shardId = clusterService.operationRouting()
            .indexShards(clusterService.state(), index, digest, null)
            .shardId();
        return new RemoteDigestBlob(client, shardId, digest);
    }

    @Override
    protected void doStart() throws ElasticsearchException {
        pipelineRegistry.addBefore(
            new PipelineRegistry.ChannelPipelineItem(
                "aggregator", "blob_handler", netty4CorsConfig -> new HttpBlobHandler(this, blobIndicesService, netty4CorsConfig))
        );

        blobHeadRequestHandler.registerHandler();
        peerRecoverySourceService.registerRecoverySourceHandlerProvider((shard, request, recoveryTarget, fileChunkSizeInBytes) -> {
            if (!BlobIndex.isBlobIndex(shard.shardId().getIndexName())) {
                return null;
            }
            return new BlobRecoveryHandler(
                shard,
                recoveryTarget,
                request,
                fileChunkSizeInBytes,
                INDICES_RECOVERY_MAX_CONCURRENT_FILE_CHUNKS_SETTING.get(settings),
                transportService,
                blobTransferTarget,
                blobIndicesService
            );
        });
    }

    @Override
    protected void doStop() throws ElasticsearchException {
    }

    @Override
    protected void doClose() throws ElasticsearchException {
    }

    /**
     * @param index  the name of blob-enabled index
     * @param digest sha-1 hash value of the file
     * @return null if no redirect is required, Otherwise the address to which should be redirected.
     */
    public String getRedirectAddress(String index, String digest) throws MissingHTTPEndpointException {
        ShardIterator shards = clusterService.operationRouting().getShards(
            clusterService.state(), index, null, digest, "_local");

        String localNodeId = clusterService.localNode().getId();
        DiscoveryNodes nodes = clusterService.state().getNodes();
        ShardRouting shard;
        while ((shard = shards.nextOrNull()) != null) {
            if (!shard.active()) {
                continue;
            }
            if (shard.currentNodeId().equals(localNodeId)) {
                // no redirect required if the shard is on this node
                return null;
            }

            DiscoveryNode node = nodes.get(shard.currentNodeId());
            String httpAddress = node.getAttributes().get("http_address");
            if (httpAddress != null) {
                return httpAddress + "/_blobs/" + BlobIndex.stripPrefix(index) + "/" + digest;
            }
        }
        throw new MissingHTTPEndpointException("Can't find a suitable http server to serve the blob");
    }

}
