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
import java.util.List;

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

import io.crate.execution.ddl.tables.AlterTableClient;
import io.crate.metadata.PartitionName;

/**
 * Main class to initiate resizing (shrink / split) an index into a new index
 */
public class TransportResizeAction extends TransportMasterNodeAction<ResizeRequest, ResizeResponse> {
    private final MetadataCreateIndexService createIndexService;
    private final Client client;

    @Inject
    public TransportResizeAction(TransportService transportService,
                                 ClusterService clusterService,
                                 ThreadPool threadPool,
                                 MetadataCreateIndexService createIndexService,
                                 Client client) {
        super(ResizeAction.NAME, transportService, clusterService, threadPool, ResizeRequest::new);
        this.createIndexService = createIndexService;
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
        List<String> indices = state.metadata().getIndices(
            request.table(),
            request.partitionValues(),
            idxMd -> idxMd.getIndex().getName()
        );
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_WRITE, indices.toArray(String[]::new));
    }

    @Override
    protected void masterOperation(final ResizeRequest request,
                                   final ClusterState state,
                                   final ActionListener<ResizeResponse> listener) {

        List<String> indices = state.metadata().getIndices(
            request.table(),
            request.partitionValues(),
            idxMd -> idxMd.getIndex().getName()
        );
        String sourceIndexName = request.partitionValues().isEmpty()
            ? request.table().indexNameOrAlias()
            : new PartitionName(request.table(), request.partitionValues()).asIndexName();
        final String targetIndex = AlterTableClient.RESIZE_PREFIX + sourceIndexName;
        if (indices.size() != 1) {
            listener.onResponse(new ResizeResponse(false, false, targetIndex));
            return;
        }
        String index = indices.getFirst();

        // there is no need to fetch docs stats for split but we keep it simple and do it anyway for simplicity of the code
        client.admin().indices().stats(new IndicesStatsRequest()
            .clear()
            .indices(index)
            .docs(true))
            .whenComplete(ActionListener.delegateFailure(
                listener,
                (delegate, indicesStatsResponse) -> {
                    // i -> {
                    //     IndexShardStats shard = indicesStatsResponse.getIndex(index).getIndexShards().get(i);
                    //     return shard == null ? null : shard.getPrimary().getDocs();
                    // },
                    createIndexService.resize(
                        request,
                        delegate.map(
                            response -> new ResizeResponse(
                                response.isAcknowledged(),
                                true,
                                sourceIndexName
                            )
                        )
                    );
                }
            ));
    }
}
