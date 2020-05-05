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

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.DiskUsage;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;

import java.util.Set;

import static org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING;
import static org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING;

/**
 * The {@link DiskThresholdDecider} checks that the node a shard is potentially
 * being allocated to has enough disk space.
 *
 * It has three configurable settings, all of which can be changed dynamically:
 *
 * <code>cluster.routing.allocation.disk.watermark.low</code> is the low disk
 * watermark. New shards will not allocated to a node with usage higher than this,
 * although this watermark may be passed by allocating a shard. It defaults to
 * 0.85 (85.0%).
 *
 * <code>cluster.routing.allocation.disk.watermark.high</code> is the high disk
 * watermark. If a node has usage higher than this, shards are not allowed to
 * remain on the node. In addition, if allocating a shard to a node causes the
 * node to pass this watermark, it will not be allowed. It defaults to
 * 0.90 (90.0%).
 *
 * Both watermark settings are expressed in terms of used disk percentage, or
 * exact byte values for free space (like "500mb")
 *
 * <code>cluster.routing.allocation.disk.threshold_enabled</code> is used to
 * enable or disable this decider. It defaults to true (enabled).
 */
public class DiskThresholdDecider extends AllocationDecider {

    private static final Logger LOGGER = LogManager.getLogger(DiskThresholdDecider.class);

    public static final String NAME = "disk_threshold";

    private final DiskThresholdSettings diskThresholdSettings;

    public DiskThresholdDecider(Settings settings, ClusterSettings clusterSettings) {
        this.diskThresholdSettings = new DiskThresholdSettings(settings, clusterSettings);
    }

    /**
     * Returns the size of all shards that are currently being relocated to
     * the node, but may not be finished transferring yet.
     *
     * If subtractShardsMovingAway is true then the size of shards moving away is subtracted from the total size of all shards
     */
    public static long sizeOfRelocatingShards(RoutingNode node,
                                              boolean subtractShardsMovingAway,
                                              String dataPath,
                                              ClusterInfo clusterInfo,
                                              MetaData metaData,
                                              RoutingTable routingTable) {
        long totalSize = 0L;
        for (ShardRouting routing : node.shardsWithState(ShardRoutingState.INITIALIZING)) {
            if (routing.relocatingNodeId() == null) {
                // in practice the only initializing-but-not-relocating shards with a nonzero expected shard size will be ones created
                // by a resize (shrink/split/clone) operation which we expect to happen using hard links, so they shouldn't be taking
                // any additional space and can be ignored here
                continue;
            }

            final String actualPath = clusterInfo.getDataPath(routing);
            // if we don't yet know the actual path of the incoming shard then conservatively assume it's going to the path with the least
            // free space
            if (actualPath == null || actualPath.equals(dataPath)) {
                totalSize += getExpectedShardSize(routing, 0L, clusterInfo, metaData, routingTable);
            }
        }

        if (subtractShardsMovingAway) {
            for (ShardRouting routing : node.shardsWithState(ShardRoutingState.RELOCATING)) {
                String actualPath = clusterInfo.getDataPath(routing);
                if (actualPath == null) {
                    // we might know the path of this shard from before when it was relocating
                    actualPath = clusterInfo.getDataPath(routing.cancelRelocation());
                }
                if (dataPath.equals(actualPath)) {
                    totalSize -= getExpectedShardSize(routing, 0L, clusterInfo, metaData, routingTable);
                }
            }
        }

        return totalSize;
    }


    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        ClusterInfo clusterInfo = allocation.clusterInfo();
        ImmutableOpenMap<String, DiskUsage> usages = clusterInfo.getNodeMostAvailableDiskUsages();
        final Decision decision = earlyTerminate(allocation, usages);
        if (decision != null) {
            return decision;
        }

        final double usedDiskThresholdLow = 100.0 - diskThresholdSettings.getFreeDiskThresholdLow();
        final double usedDiskThresholdHigh = 100.0 - diskThresholdSettings.getFreeDiskThresholdHigh();

        // subtractLeavingShards is passed as false here, because they still use disk space, and therefore should we should be extra careful
        // and take the size into account
        final DiskUsageWithRelocations usage = getDiskUsage(node, allocation, usages, false);
        // First, check that the node currently over the low watermark
        // Cache the used disk percentage for displaying disk percentages consistent with documentation
        double usedDiskPercentage = usage.getUsedDiskAsPercentage();
        long freeBytes = usage.getFreeBytes();
        if (freeBytes < 0L) {
            final long sizeOfRelocatingShards = sizeOfRelocatingShards(node, false, usage.getPath(),
                allocation.clusterInfo(), allocation.metaData(), allocation.routingTable());
            LOGGER.debug("fewer free bytes remaining than the size of all incoming shards: " +
                    "usage {} on node {} including {} bytes of relocations, preventing allocation",
                usage, node.nodeId(), sizeOfRelocatingShards);

            return allocation.decision(Decision.NO, NAME,
                "the node has fewer free bytes remaining than the total size of all incoming shards: " +
                    "free space [%sB], relocating shards [%sB]",
                freeBytes + sizeOfRelocatingShards, sizeOfRelocatingShards);
        }

        ByteSizeValue freeBytesValue = new ByteSizeValue(freeBytes);
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("node [{}] has {}% used disk", node.nodeId(), usedDiskPercentage);
        }

        // flag that determines whether the low threshold checks below can be skipped. We use this for a primary shard that is freshly
        // allocated and empty.
        boolean skipLowThresholdChecks = shardRouting.primary() &&
            shardRouting.active() == false && shardRouting.recoverySource().getType() == RecoverySource.Type.EMPTY_STORE;

        // checks for exact byte comparisons
        if (freeBytes < diskThresholdSettings.getFreeBytesThresholdLow().getBytes()) {
            if (skipLowThresholdChecks == false) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("less than the required {} free bytes threshold ({} free) on node {}, preventing allocation",
                            diskThresholdSettings.getFreeBytesThresholdLow(), freeBytesValue, node.nodeId());
                }
                return allocation.decision(Decision.NO, NAME,
                    "the node is above the low watermark cluster setting [%s=%s], having less than the minimum required [%s] free " +
                    "space, actual free: [%s]",
                    CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(),
                    diskThresholdSettings.getLowWatermarkRaw(),
                    diskThresholdSettings.getFreeBytesThresholdLow(), freeBytesValue);
            } else if (freeBytes > diskThresholdSettings.getFreeBytesThresholdHigh().getBytes()) {
                // Allow the shard to be allocated because it is primary that
                // has never been allocated if it's under the high watermark
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("less than the required {} free bytes threshold ({} free) on node {}, " +
                                    "but allowing allocation because primary has never been allocated",
                            diskThresholdSettings.getFreeBytesThresholdLow(), freeBytesValue, node.nodeId());
                }
                return allocation.decision(Decision.YES, NAME,
                        "the node is above the low watermark, but less than the high watermark, and this primary shard has " +
                        "never been allocated before");
            } else {
                // Even though the primary has never been allocated, the node is
                // above the high watermark, so don't allow allocating the shard
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("less than the required {} free bytes threshold ({} free) on node {}, " +
                                    "preventing allocation even though primary has never been allocated",
                            diskThresholdSettings.getFreeBytesThresholdHigh(), freeBytesValue, node.nodeId());
                }
                return allocation.decision(Decision.NO, NAME,
                    "the node is above the high watermark cluster setting [%s=%s], having less than the minimum required [%s] free " +
                    "space, actual free: [%s]",
                    CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(),
                    diskThresholdSettings.getHighWatermarkRaw(),
                    diskThresholdSettings.getFreeBytesThresholdHigh(), freeBytesValue);
            }
        }
        double freeDiskPercentage = usage.getFreeDiskAsPercentage();

        // checks for percentage comparisons
        if (freeDiskPercentage < diskThresholdSettings.getFreeDiskThresholdLow()) {
            // If the shard is a replica or is a non-empty primary, check the low threshold
            if (skipLowThresholdChecks == false) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("more than the allowed {} used disk threshold ({} used) on node [{}], preventing allocation",
                            Strings.format1Decimals(usedDiskThresholdLow, "%"),
                            Strings.format1Decimals(usedDiskPercentage, "%"), node.nodeId());
                }
                return allocation.decision(Decision.NO, NAME,
                    "the node is above the low watermark cluster setting [%s=%s], using more disk space than the maximum allowed " +
                    "[%s%%], actual free: [%s%%]",
                    CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(),
                    diskThresholdSettings.getLowWatermarkRaw(), usedDiskThresholdLow, freeDiskPercentage);
            } else if (freeDiskPercentage > diskThresholdSettings.getFreeDiskThresholdHigh()) {
                // Allow the shard to be allocated because it is primary that
                // has never been allocated if it's under the high watermark
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("more than the allowed {} used disk threshold ({} used) on node [{}], " +
                                    "but allowing allocation because primary has never been allocated",
                            Strings.format1Decimals(usedDiskThresholdLow, "%"),
                            Strings.format1Decimals(usedDiskPercentage, "%"), node.nodeId());
                }
                return allocation.decision(Decision.YES, NAME,
                    "the node is above the low watermark, but less than the high watermark, and this primary shard has " +
                    "never been allocated before");
            } else {
                // Even though the primary has never been allocated, the node is
                // above the high watermark, so don't allow allocating the shard
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("less than the required {} free bytes threshold ({} bytes free) on node {}, " +
                                    "preventing allocation even though primary has never been allocated",
                            Strings.format1Decimals(diskThresholdSettings.getFreeDiskThresholdHigh(), "%"),
                            Strings.format1Decimals(freeDiskPercentage, "%"), node.nodeId());
                }
                return allocation.decision(Decision.NO, NAME,
                    "the node is above the high watermark cluster setting [%s=%s], using more disk space than the maximum allowed " +
                    "[%s%%], actual free: [%s%%]",
                    CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(),
                    diskThresholdSettings.getHighWatermarkRaw(), usedDiskThresholdHigh, freeDiskPercentage);
            }
        }

        // Secondly, check that allocating the shard to this node doesn't put it above the high watermark
        final long shardSize = getExpectedShardSize(shardRouting, 0L,
            allocation.clusterInfo(), allocation.metaData(), allocation.routingTable());
        assert shardSize >= 0 : shardSize;
        double freeSpaceAfterShard = freeDiskPercentageAfterShardAssigned(usage, shardSize);
        long freeBytesAfterShard = freeBytes - shardSize;
        if (freeBytesAfterShard < diskThresholdSettings.getFreeBytesThresholdHigh().getBytes()) {
            LOGGER.warn("after allocating, node [{}] would have less than the required threshold of " +
                    "{} free (currently {} free, estimated shard size is {}), preventing allocation",
                    node.nodeId(), diskThresholdSettings.getFreeBytesThresholdHigh(), freeBytesValue, new ByteSizeValue(shardSize));
            return allocation.decision(Decision.NO, NAME,
                "allocating the shard to this node will bring the node above the high watermark cluster setting [%s=%s] " +
                    "and cause it to have less than the minimum required [%s] of free space (free: [%s], estimated shard size: [%s])",
                CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(),
                diskThresholdSettings.getHighWatermarkRaw(),
                diskThresholdSettings.getFreeBytesThresholdHigh(),
                freeBytesValue, new ByteSizeValue(shardSize));
        }
        if (freeSpaceAfterShard < diskThresholdSettings.getFreeDiskThresholdHigh()) {
            LOGGER.warn("after allocating, node [{}] would have more than the allowed " +
                            "{} free disk threshold ({} free), preventing allocation",
                    node.nodeId(), Strings.format1Decimals(diskThresholdSettings.getFreeDiskThresholdHigh(), "%"),
                                                           Strings.format1Decimals(freeSpaceAfterShard, "%"));
            return allocation.decision(Decision.NO, NAME,
                "allocating the shard to this node will bring the node above the high watermark cluster setting [%s=%s] " +
                    "and cause it to use more disk space than the maximum allowed [%s%%] (free space after shard added: [%s%%])",
                CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(),
                diskThresholdSettings.getHighWatermarkRaw(), usedDiskThresholdHigh, freeSpaceAfterShard);
        }

        assert freeBytesAfterShard >= 0 : freeBytesAfterShard;
        return allocation.decision(Decision.YES, NAME,
                "enough disk for shard on node, free: [%s], shard size: [%s], free after allocating shard: [%s]",
                freeBytesValue,
                new ByteSizeValue(shardSize),
                new ByteSizeValue(freeBytesAfterShard));
    }

    @Override
    public Decision canRemain(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        if (shardRouting.currentNodeId().equals(node.nodeId()) == false) {
            throw new IllegalArgumentException("Shard [" + shardRouting + "] is not allocated on node: [" + node.nodeId() + "]");
        }
        final ClusterInfo clusterInfo = allocation.clusterInfo();
        final ImmutableOpenMap<String, DiskUsage> usages = clusterInfo.getNodeLeastAvailableDiskUsages();
        final Decision decision = earlyTerminate(allocation, usages);
        if (decision != null) {
            return decision;
        }

        // subtractLeavingShards is passed as true here, since this is only for shards remaining, we will *eventually* have enough disk
        // since shards are moving away. No new shards will be incoming since in canAllocate we pass false for this check.
        final DiskUsageWithRelocations usage = getDiskUsage(node, allocation, usages, true);
        final String dataPath = clusterInfo.getDataPath(shardRouting);
        // If this node is already above the high threshold, the shard cannot remain (get it off!)
        final double freeDiskPercentage = usage.getFreeDiskAsPercentage();
        final long freeBytes = usage.getFreeBytes();
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("node [{}] has {}% free disk ({} bytes)", node.nodeId(), freeDiskPercentage, freeBytes);
        }
        if (dataPath == null || usage.getPath().equals(dataPath) == false) {
            return allocation.decision(Decision.YES, NAME,
                    "this shard is not allocated on the most utilized disk and can remain");
        }
        if (freeBytes < 0L) {
            final long sizeOfRelocatingShards = sizeOfRelocatingShards(node, true, usage.getPath(),
                allocation.clusterInfo(), allocation.metaData(), allocation.routingTable());
            LOGGER.debug("fewer free bytes remaining than the size of all incoming shards: " +
                    "usage {} on node {} including {} bytes of relocations, shard cannot remain",
                usage, node.nodeId(), sizeOfRelocatingShards);
            return allocation.decision(Decision.NO, NAME,
                "the shard cannot remain on this node because the node has fewer free bytes remaining than the total size of all " +
                    "incoming shards: free space [%s], relocating shards [%s]",
                freeBytes + sizeOfRelocatingShards, sizeOfRelocatingShards);
        }
        if (freeBytes < diskThresholdSettings.getFreeBytesThresholdHigh().getBytes()) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("less than the required {} free bytes threshold ({} bytes free) on node {}, shard cannot remain",
                        diskThresholdSettings.getFreeBytesThresholdHigh(), freeBytes, node.nodeId());
            }
            return allocation.decision(Decision.NO, NAME,
                "the shard cannot remain on this node because it is above the high watermark cluster setting [%s=%s] " +
                    "and there is less than the required [%s] free space on node, actual free: [%s]",
                CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(),
                diskThresholdSettings.getHighWatermarkRaw(),
                diskThresholdSettings.getFreeBytesThresholdHigh(), new ByteSizeValue(freeBytes));
        }
        if (freeDiskPercentage < diskThresholdSettings.getFreeDiskThresholdHigh()) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("less than the required {}% free disk threshold ({}% free) on node {}, shard cannot remain",
                        diskThresholdSettings.getFreeDiskThresholdHigh(), freeDiskPercentage, node.nodeId());
            }
            return allocation.decision(Decision.NO, NAME,
                "the shard cannot remain on this node because it is above the high watermark cluster setting [%s=%s] " +
                    "and there is less than the required [%s%%] free disk on node, actual free: [%s%%]",
                CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(),
                diskThresholdSettings.getHighWatermarkRaw(),
                diskThresholdSettings.getFreeDiskThresholdHigh(), freeDiskPercentage);
        }

        return allocation.decision(Decision.YES, NAME,
                "there is enough disk on this node for the shard to remain, free: [%s]", new ByteSizeValue(freeBytes));
    }

    private DiskUsageWithRelocations getDiskUsage(RoutingNode node, RoutingAllocation allocation,
                                                  ImmutableOpenMap<String, DiskUsage> usages, boolean subtractLeavingShards) {
        DiskUsage usage = usages.get(node.nodeId());
        if (usage == null) {
            // If there is no usage, and we have other nodes in the cluster,
            // use the average usage for all nodes as the usage for this node
            usage = averageUsage(node, usages);
            LOGGER.debug("unable to determine disk usage for {}, defaulting to average across nodes [{} total] [{} free] [{}% free]",
                    node.nodeId(), usage.getTotalBytes(), usage.getFreeBytes(), usage.getFreeDiskAsPercentage());
        }

        final DiskUsageWithRelocations diskUsageWithRelocations = new DiskUsageWithRelocations(usage,
            sizeOfRelocatingShards(node, subtractLeavingShards, usage.getPath(),
                allocation.clusterInfo(), allocation.metaData(), allocation.routingTable()));
        LOGGER.trace("getDiskUsage(subtractLeavingShards={}) returning {}", subtractLeavingShards, diskUsageWithRelocations);
        return diskUsageWithRelocations;
    }

    /**
     * Returns a {@link DiskUsage} for the {@link RoutingNode} using the
     * average usage of other nodes in the disk usage map.
     * @param node Node to return an averaged DiskUsage object for
     * @param usages Map of nodeId to DiskUsage for all known nodes
     * @return DiskUsage representing given node using the average disk usage
     */
    DiskUsage averageUsage(RoutingNode node, ImmutableOpenMap<String, DiskUsage> usages) {
        if (usages.size() == 0) {
            return new DiskUsage(node.nodeId(), node.node().getName(), "_na_", 0, 0);
        }
        long totalBytes = 0;
        long freeBytes = 0;
        for (ObjectCursor<DiskUsage> du : usages.values()) {
            totalBytes += du.value.getTotalBytes();
            freeBytes += du.value.getFreeBytes();
        }
        return new DiskUsage(node.nodeId(), node.node().getName(), "_na_", totalBytes / usages.size(), freeBytes / usages.size());
    }

    /**
     * Given the DiskUsage for a node and the size of the shard, return the
     * percentage of free disk if the shard were to be allocated to the node.
     * @param usage A DiskUsage for the node to have space computed for
     * @param shardSize Size in bytes of the shard
     * @return Percentage of free space after the shard is assigned to the node
     */
    double freeDiskPercentageAfterShardAssigned(DiskUsageWithRelocations usage, Long shardSize) {
        shardSize = (shardSize == null) ? 0 : shardSize;
        DiskUsage newUsage = new DiskUsage(
            usage.getNodeId(),
            usage.getNodeName(),
            usage.getPath(),
            usage.getTotalBytes(),
            usage.getFreeBytes() - shardSize
        );
        return newUsage.getFreeDiskAsPercentage();
    }

    private Decision earlyTerminate(RoutingAllocation allocation, ImmutableOpenMap<String, DiskUsage> usages) {
        // Always allow allocation if the decider is disabled
        if (diskThresholdSettings.isEnabled() == false) {
            return allocation.decision(Decision.YES, NAME, "the disk threshold decider is disabled");
        }

        // Allow allocation regardless if only a single data node is available
        if (allocation.nodes().getDataNodes().size() <= 1) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("only a single data node is present, allowing allocation");
            }
            return allocation.decision(Decision.YES, NAME, "there is only a single data node present");
        }

        // Fail open there is no info available
        final ClusterInfo clusterInfo = allocation.clusterInfo();
        if (clusterInfo == null) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("cluster info unavailable for disk threshold decider, allowing allocation.");
            }
            return allocation.decision(Decision.YES, NAME, "the cluster info is unavailable");
        }

        // Fail open if there are no disk usages available
        if (usages.isEmpty()) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("unable to determine disk usages for disk-aware allocation, allowing allocation");
            }
            return allocation.decision(Decision.YES, NAME, "disk usages are unavailable");
        }
        return null;
    }

    /**
     * Returns the expected shard size for the given shard or the default value provided if not enough information are available
     * to estimate the shards size.
     */
    public static long getExpectedShardSize(ShardRouting shard,
                                            long defaultValue,
                                            ClusterInfo clusterInfo,
                                            MetaData metaData,
                                            RoutingTable routingTable) {
        final IndexMetaData indexMetaData = metaData.getIndexSafe(shard.index());
        if (indexMetaData.getResizeSourceIndex() != null && shard.active() == false &&
            shard.recoverySource().getType() == RecoverySource.Type.LOCAL_SHARDS) {
            // in the shrink index case we sum up the source index shards since we basically make a copy of the shard in
            // the worst case
            long targetShardSize = 0;
            final Index mergeSourceIndex = indexMetaData.getResizeSourceIndex();
            final IndexMetaData sourceIndexMeta = metaData.index(mergeSourceIndex);
            if (sourceIndexMeta != null) {
                final Set<ShardId> shardIds = IndexMetaData.selectRecoverFromShards(shard.id(),
                    sourceIndexMeta, indexMetaData.getNumberOfShards());
                for (IndexShardRoutingTable shardRoutingTable : routingTable.index(mergeSourceIndex.getName())) {
                    if (shardIds.contains(shardRoutingTable.shardId())) {
                        targetShardSize += clusterInfo.getShardSize(shardRoutingTable.primaryShard(), 0);
                    }
                }
            }
            return targetShardSize == 0 ? defaultValue : targetShardSize;
        } else {
            return clusterInfo.getShardSize(shard, defaultValue);
        }
    }

    static class DiskUsageWithRelocations {

        private final DiskUsage diskUsage;
        private final long relocatingShardSize;

        DiskUsageWithRelocations(DiskUsage diskUsage, long relocatingShardSize) {
            this.diskUsage = diskUsage;
            this.relocatingShardSize = relocatingShardSize;
        }

        @Override
        public String toString() {
            return "DiskUsageWithRelocations{" +
                "diskUsage=" + diskUsage +
                ", relocatingShardSize=" + relocatingShardSize +
                '}';
        }

        double getFreeDiskAsPercentage() {
            if (getTotalBytes() == 0L) {
                return 100.0;
            }
            return 100.0 * ((double)getFreeBytes() / getTotalBytes());
        }

        double getUsedDiskAsPercentage() {
            return 100.0 - getFreeDiskAsPercentage();
        }

        long getFreeBytes() {
            try {
                return Math.subtractExact(diskUsage.getFreeBytes(), relocatingShardSize);
            } catch (ArithmeticException e) {
                return Long.MAX_VALUE;
            }
        }

        String getPath() {
            return diskUsage.getPath();
        }

        String getNodeId() {
            return diskUsage.getNodeId();
        }

        String getNodeName() {
            return diskUsage.getNodeName();
        }

        long getTotalBytes() {
            return diskUsage.getTotalBytes();
        }
    }

}
