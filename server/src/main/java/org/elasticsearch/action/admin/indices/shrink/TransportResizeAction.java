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
 * KIND, either express or implied.  See the License for the specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.admin.indices.shrink;

import java.io.IOException;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import io.crate.execution.ddl.index.SwapAndDropIndexRequest;
import io.crate.execution.ddl.index.TransportSwapAndDropIndexNameAction;
import io.crate.execution.ddl.tables.AlterTableClient;
import io.crate.execution.ddl.tables.GCDanglingArtifactsRequest;
import io.crate.execution.ddl.tables.TransportGCDanglingArtifacts;
import io.crate.metadata.PartitionName;

/**
 * Main class to initiate resizing (shrink / split) an index into a new index
 */
public class TransportResizeAction extends TransportMasterNodeAction<ResizeRequest, ResizeResponse> {

    private final MetadataCreateIndexService createIndexService;
    private final Client client;
    private final TransportSwapAndDropIndexNameAction swapAndDropIndexAction;
    private final TransportGCDanglingArtifacts gcDanglingArtifactsAction;

    @Inject
    public TransportResizeAction(TransportService transportService,
                                 ClusterService clusterService,
                                 ThreadPool threadPool,
                                 MetadataCreateIndexService createIndexService,
                                 TransportSwapAndDropIndexNameAction swapAndDropIndexAction,
                                 TransportGCDanglingArtifacts gcDanglingArtifactsAction,
                                 Client client) {
        super(ResizeAction.NAME, transportService, clusterService, threadPool, ResizeRequest::new);
        this.createIndexService = createIndexService;
        this.swapAndDropIndexAction = swapAndDropIndexAction;
        this.gcDanglingArtifactsAction = gcDanglingArtifactsAction;
        this.client = client;
    }


    @Override
    protected String executor() {
        // we go async right away
        return ThreadPool.Names.SAME;
    }

    @Override
    protected ResizeResponse read(StreamInput in) throws IOException {
        return new ResizeResponse(in);
    }

    @Override
    protected ClusterBlockException checkBlock(ResizeRequest request, ClusterState state) {
        String[] indices = state.metadata().getIndices(
            request.table(),
            request.partitionValues(),
            false,
            idxMd -> idxMd.getIndex().getName()
        ).toArray(String[]::new);
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_WRITE, indices);
    }

    @Override
    protected void masterOperation(final ResizeRequest request,
                                   final ClusterState state,
                                   final ActionListener<ResizeResponse> listener) {

        String sourceIndex = request.partitionValues().isEmpty()
            ? request.table().indexNameOrAlias()
            : new PartitionName(request.table(), request.partitionValues()).asIndexName();

        // there is no need to fetch docs stats for split but we keep it simple and do it anyway for simplicity of the code
        final String resizedIndex = AlterTableClient.RESIZE_PREFIX + sourceIndex;
        client.admin().indices().stats(new IndicesStatsRequest(sourceIndex)
            .clear()
            .docs(true))
            .thenCompose(statsResponse -> createIndexService.resizeIndex(request, statsResponse))
            .thenCompose(resizeResp -> {
                if (resizeResp.isAcknowledged() && resizeResp.isShardsAcknowledged()) {
                    SwapAndDropIndexRequest req = new SwapAndDropIndexRequest(resizedIndex, sourceIndex);
                    return swapAndDropIndexAction.execute(req).thenApply(ignored -> resizeResp);
                } else {
                    return gcDanglingArtifactsAction.execute(GCDanglingArtifactsRequest.INSTANCE).handle(
                        (ignored, err) -> {
                            throw new IllegalStateException(
                                "Resize operation wasn't acknowledged. Check shard allocation and retry", err);
                        });
                }
            })
            .whenComplete(listener);
    }
}
