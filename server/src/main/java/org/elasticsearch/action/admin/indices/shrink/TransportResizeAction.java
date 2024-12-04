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
import java.util.Objects;
import java.util.Set;
import java.util.function.IntFunction;

import org.apache.lucene.index.IndexWriter;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexClusterStateUpdateRequest;
import org.elasticsearch.action.admin.indices.stats.IndexShardStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.index.shard.ShardId;
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
        if (request.partitionValues().isEmpty()) {
            return state.blocks().indexBlockedException(ClusterBlockLevel.METADATA_WRITE, request.table().indexNameOrAlias());
        }
        PartitionName partitionName = new PartitionName(request.table(), request.partitionValues());
        return state.blocks().indexBlockedException(ClusterBlockLevel.METADATA_WRITE, partitionName.asIndexName());
    }

    @Override
    protected void masterOperation(final ResizeRequest request,
                                   final ClusterState state,
                                   final ActionListener<ResizeResponse> listener) {

        String sourceIndex = request.partitionValues().isEmpty()
            ? request.table().indexNameOrAlias()
            : new PartitionName(request.table(), request.partitionValues()).asIndexName();

        // there is no need to fetch docs stats for split but we keep it simple and do it anyway for simplicity of the code
        final String targetIndex = AlterTableClient.RESIZE_PREFIX + sourceIndex;
        client.admin().indices().stats(new IndicesStatsRequest()
            .clear()
            .indices(sourceIndex)
            .docs(true))
            .whenComplete(ActionListener.delegateFailure(
                listener,
                (delegate, indicesStatsResponse) -> {
                    CreateIndexClusterStateUpdateRequest updateRequest = prepareCreateIndexRequest(
                        request,
                        state,
                        i -> {
                            IndexShardStats shard = indicesStatsResponse.getIndex(sourceIndex).getIndexShards().get(i);
                            return shard == null ? null : shard.getPrimary().getDocs();
                        },
                        sourceIndex,
                        targetIndex
                    );
                    createIndexService.createIndex(
                        updateRequest,
                        delegate.map(
                            response -> new ResizeResponse(
                                response.isAcknowledged(),
                                response.isShardsAcknowledged(),
                                updateRequest.index()
                            )
                        )
                    );
                }
            ));
    }

    // static for unittesting this method
    static CreateIndexClusterStateUpdateRequest prepareCreateIndexRequest(final ResizeRequest request,
                                                                          final ClusterState state,
                                                                          final IntFunction<DocsStats> perShardDocStats,
                                                                          String sourceIndexName,
                                                                          String targetIndexName) {
        final IndexMetadata metadata = state.metadata().index(sourceIndexName);
        if (metadata == null) {
            throw new IndexNotFoundException(sourceIndexName);
        }
        int currentNumShards = metadata.getNumberOfShards();

        final Settings targetSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, request.newNumShards())
            .build();
        final int targetNumShards = request.newNumShards();
        boolean shrink = currentNumShards > targetNumShards;
        for (int i = 0; i < targetNumShards; i++) {
            if (shrink) {
                Set<ShardId> shardIds = IndexMetadata.selectShrinkShards(i, metadata, targetNumShards);
                long count = 0;
                for (ShardId id : shardIds) {
                    DocsStats docsStats = perShardDocStats.apply(id.id());
                    if (docsStats != null) {
                        count += docsStats.getCount();
                    }
                    if (count > IndexWriter.MAX_DOCS) {
                        throw new IllegalStateException("Can't merge index with more than [" + IndexWriter.MAX_DOCS
                            + "] docs - too many documents in shards " + shardIds);
                    }
                }
            } else {
                Objects.requireNonNull(IndexMetadata.selectSplitShard(i, metadata, targetNumShards));
                // we just execute this to ensure we get the right exceptions if the number of shards is wrong or less then etc.
            }
        }
        Set<Alias> aliases;
        String indexNameOrAlias = request.table().indexNameOrAlias();
        if (sourceIndexName.equals(indexNameOrAlias)) {
            aliases = Set.of();
        } else {
            aliases = Set.of(new Alias(indexNameOrAlias));
        }
        return new CreateIndexClusterStateUpdateRequest("resize_table", targetIndexName)
                // mappings are updated on the node when creating in the shards, this prevents race-conditions since all mapping must be
                // applied once we took the snapshot and if somebody messes things up and switches the index read/write and adds docs we
                // miss the mappings for everything is corrupted and hard to debug
                .settings(targetSettings)
                .aliases(aliases)
                .ackTimeout(request.ackTimeout())
                .masterNodeTimeout(request.masterNodeTimeout())
                .waitForActiveShards(ActiveShardCount.ONE)
                .recoverFrom(metadata.getIndex())
                .resizeType(shrink ? ResizeType.SHRINK : ResizeType.SPLIT)
                .copySettings(true);
    }
}
