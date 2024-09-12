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
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;

import io.crate.common.collections.Tuple;
import io.crate.metadata.IndexName;

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
        var total = 0;
        var replicas = 0;
        var unassigned = 0;
        var primaries = 0;
        var cs = this.clusterState.get();
        var localNodeId = cs.nodes().getLocalNodeId();
        var isMasterNode = cs.nodes().isLocalNodeElectedMaster();
        for (var shardRouting : shardsForOpenIndices(cs)) {
            if (localNodeId.equals(shardRouting.currentNodeId())) {
                total++;
                if (shardRouting.primary()) {
                    primaries++;
                } else {
                    replicas++;
                }
            } else if (isMasterNode && shardRouting.unassigned()) {
                unassigned++;
            }
        }
        return new ShardStats(total, primaries, replicas, unassigned);
    }

    @Override
    public List<ShardInfo> getShardInfo() {
        var cs = this.clusterState.get();
        var localNodeId = cs.nodes().getLocalNodeId();
        var result = new ArrayList<ShardInfo>();
        for (var shardRouting : shardsForOpenIndices(cs)) {
            var shardId = shardRouting.shardId();
            if (localNodeId.equals(shardRouting.currentNodeId())) {
                var shardStateAndSize = shardStateAndSizeProvider.apply(shardId);
                if (shardStateAndSize != null) {
                    var indexParts = IndexName.decode(shardId.getIndexName());
                    result.add(new ShardInfo(
                        shardId.id(),
                        indexParts.schema(),
                        indexParts.table(),
                        indexParts.partitionIdent(),
                        shardRouting.state().name(),
                        shardStateAndSize.v1().name(),
                        shardStateAndSize.v2())
                    );
                }
            }
        }
        return result;
    }

    private static Iterable<ShardRouting> shardsForOpenIndices(ClusterState clusterState) {
        var concreteIndices = Arrays.stream(clusterState.metadata().getConcreteAllOpenIndices())
            .filter(index -> !IndexName.isDangling(index))
            .toArray(String[]::new);
        return clusterState.routingTable().allShards(concreteIndices);
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
            return new Tuple<>(shard.state(), shard.storeStats().getSizeInBytes());
        }
    }
}
