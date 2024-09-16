/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.expression.reference.sys.check.node;

import static org.elasticsearch.indices.ShardLimitValidator.SETTING_CLUSTER_MAX_SHARDS_PER_NODE;

import java.util.Arrays;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

import io.crate.metadata.IndexName;

@Singleton
public class MaxShardsPerNodeSysCheck extends AbstractSysNodeCheck {

    static final int ID = 8;
    private static final String DESCRIPTION = ("The amount of shards on the node reached 90 % of the limit of the cluster setting `cluster.max_shards_per_node`. " +
                                               "Creating new tables or partitions which would push the number of shards beyond 100 % of the limit will be rejected.");

    private static final double PERCENTAGE_MAX_SHARDS_PER_NODE_THRESHOLD = 0.90d;

    private final ClusterService clusterService;

    @Inject
    public MaxShardsPerNodeSysCheck(ClusterService clusterService) {
        super(ID, DESCRIPTION, Severity.MEDIUM);
        this.clusterService = clusterService;
    }

    @Override
    public boolean isValid() {
        var maxShardLimitPerNode = clusterService.getClusterSettings().get(SETTING_CLUSTER_MAX_SHARDS_PER_NODE).doubleValue();
        var cs = clusterService.state();
        var localNodeId = cs.nodes().getLocalNodeId();
        var numberOfShardsOnLocalNode = 0d;
        for (var shardRouting : shardsForOpenIndices(cs)) {
            if (localNodeId.equals(shardRouting.currentNodeId())) {
                numberOfShardsOnLocalNode++;
            }
        }
        return numberOfShardsOnLocalNode / maxShardLimitPerNode < PERCENTAGE_MAX_SHARDS_PER_NODE_THRESHOLD;
    }

    private static Iterable<ShardRouting> shardsForOpenIndices(ClusterState clusterState) {
        var concreteIndices = Arrays.stream(clusterState.metadata().getConcreteAllOpenIndices())
            .filter(index -> !IndexName.isDangling(index))
            .toArray(String[]::new);
        return clusterState.routingTable().allShards(concreteIndices);
    }
}
