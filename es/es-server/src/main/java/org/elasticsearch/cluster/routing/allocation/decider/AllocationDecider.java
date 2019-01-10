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
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster.routing.allocation.decider;

import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.Decision.Type;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;

/**
 * {@link AllocationDecider} is an abstract base class that allows to make
 * dynamic cluster- or index-wide shard allocation decisions on a per-node
 * basis.
 */
public abstract class AllocationDecider extends AbstractComponent {

    /**
     * Initializes a new {@link AllocationDecider}
     * @param settings {@link Settings} used by this {@link AllocationDecider}
     */
    protected AllocationDecider(Settings settings) {
        super(settings);
    }

    /**
     * Returns a {@link Decision} whether the given shard routing can be
     * re-balanced to the given allocation. The default is
     * {@link Decision#ALWAYS}.
     */
    public Decision canRebalance(ShardRouting shardRouting, RoutingAllocation allocation) {
        return Decision.ALWAYS;
    }

    /**
     * Returns a {@link Decision} whether the given shard routing can be
     * allocated on the given node. The default is {@link Decision#ALWAYS}.
     */
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return Decision.ALWAYS;
    }

    /**
     * Returns a {@link Decision} whether the given shard routing can be remain
     * on the given node. The default is {@link Decision#ALWAYS}.
     */
    public Decision canRemain(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return Decision.ALWAYS;
    }

    /**
     * Returns a {@link Decision} whether the given shard routing can be allocated at all at this state of the
     * {@link RoutingAllocation}. The default is {@link Decision#ALWAYS}.
     */
    public Decision canAllocate(ShardRouting shardRouting, RoutingAllocation allocation) {
        return Decision.ALWAYS;
    }

    /**
     * Returns a {@link Decision} whether the given shard routing can be allocated at all at this state of the
     * {@link RoutingAllocation}. The default is {@link Decision#ALWAYS}.
     */
    public Decision canAllocate(IndexMetaData indexMetaData, RoutingNode node, RoutingAllocation allocation) {
        return Decision.ALWAYS;
    }

    /**
     * Returns a {@link Decision} whether the given node can allow any allocation at all at this state of the
     * {@link RoutingAllocation}. The default is {@link Decision#ALWAYS}.
     */
    public Decision canAllocate(RoutingNode node, RoutingAllocation allocation) {
        return Decision.ALWAYS;
    }

    /**
     * Returns a {@link Decision} whether the cluster can execute
     * re-balanced operations at all.
     * {@link Decision#ALWAYS}.
     */
    public Decision canRebalance(RoutingAllocation allocation) {
        return Decision.ALWAYS;
    }

    /**
     * Returns a {@link Decision} whether the given primary shard can be
     * forcibly allocated on the given node. This method should only be called
     * for unassigned primary shards where the node has a shard copy on disk.
     *
     * Note: all implementations that override this behavior should take into account
     * the results of {@link #canAllocate(ShardRouting, RoutingNode, RoutingAllocation)}
     * before making a decision on force allocation, because force allocation should only
     * be considered if all deciders return {@link Decision#NO}.
     */
    public Decision canForceAllocatePrimary(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        assert shardRouting.primary() : "must not call canForceAllocatePrimary on a non-primary shard " + shardRouting;
        assert shardRouting.unassigned() : "must not call canForceAllocatePrimary on an assigned shard " + shardRouting;
        Decision decision = canAllocate(shardRouting, node, allocation);
        if (decision.type() == Type.NO) {
            // On a NO decision, by default, we allow force allocating the primary.
            return allocation.decision(Decision.YES,
                                       decision.label(),
                                       "primary shard [%s] allowed to force allocate on node [%s]",
                                       shardRouting.shardId(), node.nodeId());
        } else {
            // On a THROTTLE/YES decision, we use the same decision instead of forcing allocation
            return decision;
        }
    }
}
