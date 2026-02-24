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

package org.elasticsearch.cluster;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.StoreStats;

import com.carrotsearch.hppc.ObjectHashSet;
import com.carrotsearch.hppc.cursors.ObjectCursor;

import io.crate.common.collections.MapBuilder;

/**
 * ClusterInfo is an object representing a map of nodes to {@link DiskUsage}
 * and a map of shard ids to shard sizes, see
 * <code>InternalClusterInfoService.shardIdentifierFromRouting(String)</code>
 * for the key used in the shardSizes map
 */
public class ClusterInfo implements Writeable {

    private final Map<String, DiskUsage> leastAvailableSpaceUsage;
    private final Map<String, DiskUsage> mostAvailableSpaceUsage;
    final Map<String, Long> shardSizes;
    public static final ClusterInfo EMPTY = new ClusterInfo();
    final Map<ShardRouting, String> routingToDataPath;
    final Map<NodeAndPath, ReservedSpace> reservedSpace;

    protected ClusterInfo() {
        this(
            Map.of(),
            Map.of(),
            Map.of(),
            Map.of(),
            Map.of());
    }

    /**
     * Creates a new ClusterInfo instance.
     *
     * @param leastAvailableSpaceUsage a node id to disk usage mapping for the path that has the least available space on the node.
     * @param mostAvailableSpaceUsage  a node id to disk usage mapping for the path that has the most available space on the node.
     * @param shardSizes a shardkey to size in bytes mapping per shard.
     * @param routingToDataPath the shard routing to datapath mapping
     * @param reservedSpace reserved space per shard broken down by node and data path
     * @see #shardIdentifierFromRouting
     */
    public ClusterInfo(Map<String, DiskUsage> leastAvailableSpaceUsage,
                       Map<String, DiskUsage> mostAvailableSpaceUsage,
                       Map<String, Long> shardSizes,
                       Map<ShardRouting, String> routingToDataPath,
                       Map<NodeAndPath, ReservedSpace> reservedSpace) {
        this.leastAvailableSpaceUsage = leastAvailableSpaceUsage;
        this.shardSizes = shardSizes;
        this.mostAvailableSpaceUsage = mostAvailableSpaceUsage;
        this.routingToDataPath = routingToDataPath;
        this.reservedSpace = reservedSpace;
    }

    public ClusterInfo(StreamInput in) throws IOException {
        final Map<String, DiskUsage> leastMap = in.readMap(StreamInput::readString, DiskUsage::new);
        final Map<String, DiskUsage> mostMap = in.readMap(StreamInput::readString, DiskUsage::new);
        final Map<String, Long> sizeMap = in.readMap(StreamInput::readString, StreamInput::readLong);
        final Map<ShardRouting, String> routingMap = in.readMap(ShardRouting::new, StreamInput::readString);
        Map<NodeAndPath, ReservedSpace> reservedSpaceMap;
        if (in.getVersion().onOrAfter(StoreStats.RESERVED_BYTES_VERSION)) {
            reservedSpaceMap = in.readMap(NodeAndPath::new, ReservedSpace::new);
        } else {
            reservedSpaceMap = Map.of();
        }

        MapBuilder<String, DiskUsage> leastBuilder = MapBuilder.newMapBuilder();
        this.leastAvailableSpaceUsage = leastBuilder.putAll(leastMap).immutableMap();
        MapBuilder<String, DiskUsage> mostBuilder = MapBuilder.newMapBuilder();
        this.mostAvailableSpaceUsage = mostBuilder.putAll(mostMap).immutableMap();
        MapBuilder<String, Long> sizeBuilder = MapBuilder.newMapBuilder();
        this.shardSizes = sizeBuilder.putAll(sizeMap).immutableMap();
        MapBuilder<ShardRouting, String> routingBuilder = MapBuilder.newMapBuilder();
        this.routingToDataPath = routingBuilder.putAll(routingMap).immutableMap();
        MapBuilder<NodeAndPath, ReservedSpace> reservedSpaceBuilder = MapBuilder.newMapBuilder();
        this.reservedSpace = reservedSpaceBuilder.putAll(reservedSpaceMap).immutableMap();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(this.leastAvailableSpaceUsage.size());
        for (Map.Entry<String, DiskUsage> c : this.leastAvailableSpaceUsage.entrySet()) {
            out.writeString(c.getKey());
            c.getValue().writeTo(out);
        }
        out.writeMap(this.mostAvailableSpaceUsage, StreamOutput::writeString, (o, v) -> v.writeTo(o));
        out.writeMap(this.shardSizes, StreamOutput::writeString, (o, v) -> out.writeLong(v == null ? -1 : v));
        out.writeMap(this.routingToDataPath, (o, k) -> k.writeTo(o), StreamOutput::writeString);
        if (out.getVersion().onOrAfter(StoreStats.RESERVED_BYTES_VERSION)) {
            out.writeWritableMap(this.reservedSpace);
        }
    }


    /**
     * Returns a node id to disk usage mapping for the path that has the least available space on the node.
     * Note that this does not take account of reserved space: there may be another path with less available _and unreserved_ space.
     */
    public Map<String, DiskUsage> getNodeLeastAvailableDiskUsages() {
        return this.leastAvailableSpaceUsage;
    }

    /**
     * Returns a node id to disk usage mapping for the path that has the most available space on the node.
     * Note that this does not take account of reserved space: there may be another path with more available _and unreserved_ space.
     */
    public Map<String, DiskUsage> getNodeMostAvailableDiskUsages() {
        return this.mostAvailableSpaceUsage;
    }

    /**
     * Returns the shard size for the given shard routing or <code>null</code> it that metric is not available.
     */
    public Long getShardSize(ShardRouting shardRouting) {
        return shardSizes.get(shardIdentifierFromRouting(shardRouting));
    }

    /**
     * Returns the nodes absolute data-path the given shard is allocated on or <code>null</code> if the information is not available.
     */
    public String getDataPath(ShardRouting shardRouting) {
        return routingToDataPath.get(shardRouting);
    }

    /**
     * Returns the shard size for the given shard routing or <code>defaultValue</code> it that metric is not available.
     */
    public long getShardSize(ShardRouting shardRouting, long defaultValue) {
        Long shardSize = getShardSize(shardRouting);
        return shardSize == null ? defaultValue : shardSize;
    }

    /**
     * Returns the reserved space for each shard on the given node/path pair
     */
    public ReservedSpace getReservedSpace(String nodeId, String dataPath) {
        final ReservedSpace result = reservedSpace.get(new NodeAndPath(nodeId, dataPath));
        return result == null ? ReservedSpace.EMPTY : result;
    }

    /**
     * Method that incorporates the ShardId for the shard into a string that
     * includes a 'p' or 'r' depending on whether the shard is a primary.
     */
    static String shardIdentifierFromRouting(ShardRouting shardRouting) {
        return shardRouting.shardId().toString() + "[" + (shardRouting.primary() ? "p" : "r") + "]";
    }

    /**
     * Represents a data path on a node
     */
    public static class NodeAndPath implements Writeable {
        public final String nodeId;
        public final String path;

        public NodeAndPath(String nodeId, String path) {
            this.nodeId = Objects.requireNonNull(nodeId);
            this.path = Objects.requireNonNull(path);
        }

        public NodeAndPath(StreamInput in) throws IOException {
            this.nodeId = in.readString();
            this.path = in.readString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            NodeAndPath that = (NodeAndPath) o;
            return nodeId.equals(that.nodeId) && path.equals(that.path);
        }

        @Override
        public int hashCode() {
            return Objects.hash(nodeId, path);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(nodeId);
            out.writeString(path);
        }
    }

    /**
     * Represents the total amount of "reserved" space on a particular data path, together with the set of shards considered.
     */
    public static class ReservedSpace implements Writeable {

        public static final ReservedSpace EMPTY = new ReservedSpace(0, new ObjectHashSet<>());

        private final long total;
        private final ObjectHashSet<ShardId> shardIds;

        private ReservedSpace(long total, ObjectHashSet<ShardId> shardIds) {
            this.total = total;
            this.shardIds = shardIds;
        }

        ReservedSpace(StreamInput in) throws IOException {
            total = in.readVLong();
            final int shardIdCount = in.readVInt();
            shardIds = new ObjectHashSet<>(shardIdCount);
            for (int i = 0; i < shardIdCount; i++) {
                shardIds.add(new ShardId(in));
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(total);
            out.writeVInt(shardIds.size());
            for (ObjectCursor<ShardId> shardIdCursor : shardIds) {
                shardIdCursor.value.writeTo(out);
            }
        }

        public long getTotal() {
            return total;
        }

        public boolean containsShardId(ShardId shardId) {
            return shardIds.contains(shardId);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ReservedSpace that = (ReservedSpace) o;
            return total == that.total &&
                shardIds.equals(that.shardIds);
        }

        @Override
        public int hashCode() {
            return Objects.hash(total, shardIds);
        }


        public static class Builder {
            private long total;
            private ObjectHashSet<ShardId> shardIds = new ObjectHashSet<>();

            public ReservedSpace build() {
                assert shardIds != null : "already built";
                final ReservedSpace reservedSpace = new ReservedSpace(total, shardIds);
                shardIds = null;
                return reservedSpace;
            }

            public Builder add(ShardId shardId, long reservedBytes) {
                assert shardIds != null : "already built";
                assert reservedBytes >= 0 : reservedBytes;
                shardIds.add(shardId);
                total += reservedBytes;
                return this;
            }
        }
    }
}
