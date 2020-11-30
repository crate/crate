/*
 * This file is part of a module with proprietary Enterprise Features.
 *
 * Licensed to Crate.io Inc. ("Crate.io") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 *
 * To use this file, Crate.io must have given you permission to enable and
 * use such Enterprise Features and you must have a valid Enterprise or
 * Subscription Agreement with Crate.io.  If you enable or use the Enterprise
 * Features, you represent and warrant that you have a valid Enterprise or
 * Subscription Agreement with Crate.io.  Your use of the Enterprise Features
 * if governed by the terms and conditions of your Enterprise or Subscription
 * Agreement with Crate.io.
 */

package io.crate.beans;

import io.crate.common.collections.Tuple;
import io.crate.metadata.IndexParts;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

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
                    var indexParts = new IndexParts(shardId.getIndexName());
                    result.add(new ShardInfo(shardId.id(),
                                             indexParts.getTable(),
                                             indexParts.getPartitionIdent(),
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
            .filter(index -> !IndexParts.isDangling(index))
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
