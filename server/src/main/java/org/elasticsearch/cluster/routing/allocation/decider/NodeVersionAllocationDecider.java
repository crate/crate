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

import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RecoverySource.SnapshotRecoverySource;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;

/**
 * An allocation decider that prevents relocation or allocation from nodes
 * that might not be version compatible. If we relocate from a node that runs
 * a newer version than the node we relocate to this might cause {@link org.apache.lucene.index.IndexFormatTooNewException}
 * on the lowest level since it might have already written segments that use a new postings format or codec that is not
 * available on the target node.
 */
public class NodeVersionAllocationDecider extends AllocationDecider {

    public static final String NAME = "node_version";

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        if (shardRouting.primary()) {
            if (shardRouting.currentNodeId() == null) {
                if (shardRouting.recoverySource() != null && shardRouting.recoverySource().getType() == RecoverySource.Type.SNAPSHOT) {
                    // restoring from a snapshot - check that the node can handle the version
                    return isVersionCompatible((SnapshotRecoverySource)shardRouting.recoverySource(), node, allocation);
                } else {
                    // existing or fresh primary on the node
                    return allocation.decision(Decision.YES, NAME, "the primary shard is new or already existed on the node");
                }
            } else {
                // relocating primary, only migrate to newer host
                return isVersionCompatibleRelocatePrimary(allocation.routingNodes(), shardRouting.currentNodeId(), node, allocation);
            }
        } else {
            final ShardRouting primary = allocation.routingNodes().activePrimary(shardRouting.shardId());
            // check that active primary has a newer version so that peer recovery works
            if (primary != null) {
                return isVersionCompatibleAllocatingReplica(allocation.routingNodes(), primary.currentNodeId(), node, allocation);
            } else {
                // ReplicaAfterPrimaryActiveAllocationDecider should prevent this case from occurring
                return allocation.decision(Decision.YES, NAME, "no active primary shard yet");
            }
        }
    }

    private Decision isVersionCompatibleRelocatePrimary(final RoutingNodes routingNodes, final String sourceNodeId,
                                                        final RoutingNode target, final RoutingAllocation allocation) {
        final RoutingNode source = routingNodes.node(sourceNodeId);
        if (target.node().getVersion().onOrAfter(source.node().getVersion())) {
            return allocation.decision(Decision.YES, NAME,
                "can relocate primary shard from a node with version [%s] to a node with equal-or-newer version [%s]",
                source.node().getVersion(), target.node().getVersion());
        } else {
            return allocation.decision(Decision.NO, NAME,
                "cannot relocate primary shard from a node with version [%s] to a node with older version [%s]",
                source.node().getVersion(), target.node().getVersion());
        }
    }

    private Decision isVersionCompatibleAllocatingReplica(final RoutingNodes routingNodes, final String sourceNodeId,
                                                          final RoutingNode target, final RoutingAllocation allocation) {
        final RoutingNode source = routingNodes.node(sourceNodeId);
        if (target.node().getVersion().onOrAfter(source.node().getVersion())) {
            /* we can allocate if we can recover from a node that is younger or on the same version
             * if the primary is already running on a newer version that won't work due to possible
             * differences in the lucene index format etc.*/
            return allocation.decision(Decision.YES, NAME,
                "can allocate replica shard to a node with version [%s] since this is equal-or-newer than the primary version [%s]",
                target.node().getVersion(), source.node().getVersion());
        } else {
            return allocation.decision(Decision.NO, NAME,
                "cannot allocate replica shard to a node with version [%s] since this is older than the primary version [%s]",
                target.node().getVersion(), source.node().getVersion());
        }
    }

    private Decision isVersionCompatible(SnapshotRecoverySource recoverySource, final RoutingNode target,
                                         final RoutingAllocation allocation) {
        if (target.node().getVersion().onOrAfter(recoverySource.version())) {
            /* we can allocate if we can restore from a snapshot that is older or on the same version */
            return allocation.decision(Decision.YES, NAME, "node version [%s] is the same or newer than snapshot version [%s]",
                target.node().getVersion(), recoverySource.version());
        } else {
            return allocation.decision(Decision.NO, NAME, "node version [%s] is older than the snapshot version [%s]",
                target.node().getVersion(), recoverySource.version());
        }
    }
}
