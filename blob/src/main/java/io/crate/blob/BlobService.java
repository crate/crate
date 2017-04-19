/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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
import io.crate.blob.pending_transfer.BlobHeadRequestHandler;
import io.crate.blob.recovery.BlobRecoveryHandler;
import io.crate.blob.v2.BlobIndex;
import io.crate.blob.v2.BlobIndicesService;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.internal.Nullable;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.recovery.*;
import org.elasticsearch.transport.TransportService;

import java.util.function.Function;
import java.util.function.Supplier;

public class BlobService extends AbstractLifecycleComponent {

    private final BlobHeadRequestHandler blobHeadRequestHandler;

    private final ClusterService clusterService;
    private final HttpServerTransport httpServerTransport;
    private final TransportService transportService;
    private final BlobTransferTarget blobTransferTarget;
    private final BlobIndicesService blobIndicesService;
    private final Client client;

    @Inject
    public BlobService(Settings settings,
                       Client client,
                       ClusterService clusterService,
                       @Nullable HttpServerTransport httpServerTransport,
                       TransportService transportService,
                       BlobTransferTarget blobTransferTarget,
                       BlobIndicesService blobIndicesService,
                       BlobHeadRequestHandler blobHeadRequestHandler) {
        super(settings);
        this.client = client;
        this.clusterService = clusterService;
        this.blobHeadRequestHandler = blobHeadRequestHandler;
        this.httpServerTransport = httpServerTransport;
        this.transportService = transportService;
        this.blobTransferTarget = blobTransferTarget;
        this.blobIndicesService = blobIndicesService;
    }

    public RemoteDigestBlob newBlob(String index, String digest) {
        return new RemoteDigestBlob(this, index, digest);
    }

    @Override
    protected void doStart() throws ElasticsearchException {
        logger.info("BlobService.doStart() {}", this);

        blobHeadRequestHandler.registerHandler();
        RecoverySettings recoverySettings = new RecoverySettings(settings, clusterService.getClusterSettings());
        recoverySettings.registerRecoverySourceHandlerProvider(new RecoverySourceHandlerProvider() {
            @Override
            public RecoverySourceHandler get(IndexShard shard,
                                             StartRecoveryRequest request,
                                             RemoteRecoveryTargetHandler recoveryTarget,
                                             Function<String, Releasable> delayNewRecoveries,
                                             Supplier<Long> currentClusterStateVersionSupplier,
                                             Logger logger) {
                if (!BlobIndex.isBlobIndex(shard.shardId().getIndexName())) {
                    return null;
                }
                return new BlobRecoveryHandler(
                    shard,
                    recoveryTarget,
                    request,
                    currentClusterStateVersionSupplier,
                    delayNewRecoveries,
                    recoverySettings,
                    logger,
                    transportService,
                    blobTransferTarget,
                    blobIndicesService
                );
            }
        });

        // by default the http server is started after the discovery service.
        // For the BlobService this is too late.

        // The HttpServer has to be started before so that the boundAddress
        // can be added to DiscoveryNodes - this is required for the redirect logic.
        if (settings.getAsBoolean("http.enabled", true)) {
            httpServerTransport.start();
        } else {
            logger.warn("Http server should be enabled for blob support");
        }
    }

    @Override
    protected void doStop() throws ElasticsearchException {
        if (settings.getAsBoolean("http.enabled", true)) {
            httpServerTransport.stop();
        }
    }

    @Override
    protected void doClose() throws ElasticsearchException {
        if (settings.getAsBoolean("http.enabled", true)) {
            httpServerTransport.close();
        }
    }

    /**
     * @param index  the name of blob-enabled index
     * @param digest sha-1 hash value of the file
     * @return null if no redirect is required, Otherwise the address to which should be redirected.
     */
    public String getRedirectAddress(String index, String digest) throws MissingHTTPEndpointException {
        ShardIterator shards = clusterService.operationRouting().getShards(
            clusterService.state(), index, null, digest, "_local");

        String localNodeId = clusterService.state().nodes().getLocalNodeId();
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

    public Client getClient() {
        return client;
    }
}
