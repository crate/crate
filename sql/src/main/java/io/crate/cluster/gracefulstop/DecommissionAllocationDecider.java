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

package io.crate.cluster.gracefulstop;

import com.google.common.collect.ImmutableSet;
import io.crate.metadata.settings.CrateSettings;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;

import java.util.Map;
import java.util.Set;


@Singleton
public class DecommissionAllocationDecider extends AllocationDecider {

    public static final String NAME = "decommission";

    private Set<String> decommissioningNodes = ImmutableSet.of();

    private DataAvailability dataAvailability; // set in onRefreshSettings in the CTOR

    @Inject
    public DecommissionAllocationDecider(Settings settings, ClusterService clusterService) {
        super(settings);
        ClusterSettings clusterSettings = clusterService.getClusterSettings();
        updateDecommissioningNodes(settings);
        updateMinAvailability(CrateSettings.GRACEFUL_STOP_MIN_AVAILABILITY.extract(settings));
        clusterSettings.addSettingsUpdateConsumer(
            DecommissioningService.DECOMMISSION_INTERNAL_SETTING_GROUP, this::updateDecommissioningNodes);
        clusterSettings.addSettingsUpdateConsumer(
            CrateSettings.GRACEFUL_STOP_MIN_AVAILABILITY.esSetting(), this::updateMinAvailability);
    }

    private void updateDecommissioningNodes(Settings settings) {
        Map<String, String> decommissionMap = settings.getByPrefix(DecommissioningService.DECOMMISSION_PREFIX).getAsMap();
        decommissioningNodes = decommissionMap.keySet();
    }

    private void updateMinAvailability(String availability) {
        dataAvailability = DataAvailability.of(availability);
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        if (decommissioningNodes.contains(node.nodeId())
            && dataAvailability == DataAvailability.PRIMARIES
            && !shardRouting.primary()) {

            // if primaries are removed from this node it will try to re-balance non-primaries onto this node
            // prevent this - replicas that are already here can remain, but no new replicas should be assigned
            return allocation.decision(Decision.NO, NAME, "dataAvailability=primaries, shard=replica, decommissioned=true");
        }
        return canRemainOrAllocate(node.nodeId(), shardRouting, allocation);
    }

    @Override
    public Decision canRemain(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return canRemainOrAllocate(node.nodeId(), shardRouting, allocation);
    }

    private Decision canRemainOrAllocate(String nodeId, ShardRouting shardRouting, RoutingAllocation allocation) {
        if (dataAvailability == DataAvailability.NONE) {
            return allocation.decision(Decision.YES, NAME, "dataAvailability=none");
        }

        if (decommissioningNodes.contains(nodeId)) {
            if (dataAvailability == DataAvailability.PRIMARIES && !shardRouting.primary()) {
                return allocation.decision(Decision.YES, NAME, "dataAvailability=primaries shard=replica decommissioned=true");
            }
            return allocation.decision(Decision.NO, NAME, "node is being decommissioned");
        }
        return allocation.decision(Decision.YES, NAME, "node isn't decommissioned");
    }
}
