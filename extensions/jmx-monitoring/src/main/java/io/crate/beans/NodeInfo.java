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

package io.crate.beans;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata.State;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;

import io.crate.common.collections.Tuple;
import io.crate.exceptions.RelationUnknown;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;

public class NodeInfo implements NodeInfoMXBean {

    public static final String NAME = "io.crate.monitoring:type=NodeInfo";

    private final Supplier<ClusterState> clusterState;
    private final Function<ShardId, Tuple<IndexShardState, Long>> shardStateAndSizeProvider;

    public NodeInfo(Supplier<ClusterState> clusterState,
                    Function<ShardId, Tuple<IndexShardState, Long>> shardStateAndSizeProvider) {
        this.clusterState = clusterState;
        this.shardStateAndSizeProvider = shardStateAndSizeProvider;
    }

    @Override
    public String getNodeId() {
        return clusterState.get().nodes().getLocalNodeId();
    }

    @Override
    public String getNodeName() {
        return clusterState.get().nodes().getLocalNode().getName();
    }

    @Override
    public long getClusterStateVersion() {
        return clusterState.get().version();
    }

    public ShardStats getShardStats() {
        int total = 0;
        int replicas = 0;
        int unassigned = 0;
        int primaries = 0;
        var clusterState = this.clusterState.get();
        Metadata metadata = clusterState.metadata();
        String localNodeId = clusterState.nodes().getLocalNodeId();
        boolean isMasterNode = clusterState.nodes().isLocalNodeElectedMaster();

        for (var indexRoutingTable : clusterState.routingTable()) {
            Index index = indexRoutingTable.getIndex();
            IndexMetadata indexMetadata = metadata.index(index);
            if (indexRoutingTable == null || indexMetadata.getState() == State.CLOSE) {
                continue;
            }
            for (var cursor : indexRoutingTable.shards()) {
                IndexShardRoutingTable indexShardRoutingTable = cursor.value;
                for (var shardRouting : indexShardRoutingTable.shards()) {
                    if (isMasterNode && shardRouting.unassigned()) {
                        unassigned++;
                    }
                    if (!localNodeId.equals(shardRouting.currentNodeId())) {
                        continue;
                    }
                    total++;
                    if (shardRouting.primary()) {
                        primaries++;
                    } else {
                        replicas++;
                    }
                }
            }
        }
        return new ShardStats(total, primaries, replicas, unassigned);
    }

    @Override
    public List<ShardInfo> getShardInfo() {
        var clusterState = this.clusterState.get();
        Metadata metadata = clusterState.metadata();
        String localNodeId = clusterState.nodes().getLocalNodeId();
        ArrayList<ShardInfo> result = new ArrayList<>();

        for (var indexRoutingTable : clusterState.routingTable()) {
            Index index = indexRoutingTable.getIndex();
            IndexMetadata indexMetadata = metadata.index(index);
            if (indexRoutingTable == null || indexMetadata.getState() == State.CLOSE) {
                continue;
            }
            for (var cursor : indexRoutingTable.shards()) {
                IndexShardRoutingTable indexShardRoutingTable = cursor.value;
                for (var shardRouting : indexShardRoutingTable.shards()) {
                    if (!localNodeId.equals(shardRouting.currentNodeId())) {
                        continue;
                    }
                    ShardId shardId = shardRouting.shardId();
                    Tuple<IndexShardState, Long> stats = shardStateAndSizeProvider.apply(shardId);
                    if (stats == null) {
                        continue;
                    }
                    PartitionName partitionName;
                    try {
                        partitionName = clusterState.metadata().getPartitionName(shardId.getIndexUUID());
                    } catch (RelationUnknown | IndexNotFoundException e) {
                        continue; // skip shards of indices that are not in the metadata
                    }
                    ShardRoutingState routingState = shardRouting.state();
                    RelationName relationName = partitionName.relationName();
                    result.add(new ShardInfo(
                        shardId.id(),
                        relationName.schema(),
                        relationName.name(),
                        partitionName.ident() == null ? "" : partitionName.ident(),
                        routingState.name(),
                        stats.v1().name(),
                        stats.v2())
                    );
                }
            }
        }
        return result;
    }

    public static class ShardStateAndSizeProvider implements Function<ShardId, Tuple<IndexShardState, Long>> {
        private final IndicesService indicesService;

        public ShardStateAndSizeProvider(IndicesService indicesService) {
            this.indicesService = indicesService;
        }

        public Tuple<IndexShardState, Long> apply(ShardId shardId) {
            var shard = indicesService.getShardOrNull(shardId);
            if (shard == null) {
                return null;
            }
            return new Tuple<>(shard.state(), shard.storeStats().sizeInBytes());
        }
    }
}
