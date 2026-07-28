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
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;

import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.StoreStats;

import com.carrotsearch.hppc.ObjectLongHashMap;
import com.carrotsearch.hppc.ObjectLongMap;

/**
 * ClusterInfo is an object representing a map of nodes to {@link DiskUsage}
 * and a map of shard ids to shard sizes, see
 * <code>InternalClusterInfoService.shardIdentifierFromRouting(String)</code>
 * for the key used in the shardSizes map
 */
public class ClusterInfo implements Writeable {

    private final Map<String, DiskUsage> leastAvailableSpaceUsage;
    private final Map<String, DiskUsage> mostAvailableSpaceUsage;
    final ObjectLongMap<String> shardSizes;
    public static final ClusterInfo EMPTY = new ClusterInfo();
    final Map<ShardRouting, String> routingToDataPath;
    final Map<NodeAndPath, ReservedSpace> reservedSpace;

    protected ClusterInfo() {
        this(
            Map.of(),
            Map.of(),
            new ObjectLongHashMap<>(0),
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
                       ObjectLongMap<String> shardSizes,
                       Map<ShardRouting, String> routingToDataPath,
                       Map<NodeAndPath, ReservedSpace> reservedSpace) {
        this.leastAvailableSpaceUsage = leastAvailableSpaceUsage;
        this.shardSizes = shardSizes;
        this.mostAvailableSpaceUsage = mostAvailableSpaceUsage;
        this.routingToDataPath = routingToDataPath;
        this.reservedSpace = reservedSpace;
    }

    public ClusterInfo(StreamInput in) throws IOException {
        final Map<String, DiskUsage> leastMap = in.readMap(StreamInput::readString, DiskUsage::of);
        final Map<String, DiskUsage> mostMap = in.readMap(StreamInput::readString, DiskUsage::of);
        int numShardSizes = in.readVInt();
        this.shardSizes = new ObjectLongHashMap<>(numShardSizes);
        for (int i = 0; i < numShardSizes; i++) {
            String key = in.readString();
            long size = in.readLong();
            shardSizes.put(key, size);
        }
        final Map<ShardRouting, String> routingMap = in.readMap(ShardRouting::new, StreamInput::readString);
        Map<NodeAndPath, ReservedSpace> reservedSpaceMap;
        if (in.getVersion().onOrAfter(StoreStats.RESERVED_BYTES_VERSION)) {
            reservedSpaceMap = in.readMap(NodeAndPath::new, ReservedSpace::of);
        } else {
            reservedSpaceMap = Map.of();
        }

        this.leastAvailableSpaceUsage = Collections.unmodifiableMap(leastMap);
        this.mostAvailableSpaceUsage = Collections.unmodifiableMap(mostMap);
        this.routingToDataPath = Collections.unmodifiableMap(routingMap);
        this.reservedSpace = Collections.unmodifiableMap(reservedSpaceMap);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(this.leastAvailableSpaceUsage.size());
        for (Map.Entry<String, DiskUsage> c : this.leastAvailableSpaceUsage.entrySet()) {
            out.writeString(c.getKey());
            c.getValue().writeTo(out);
        }
        out.writeMap(this.mostAvailableSpaceUsage, StreamOutput::writeString, (o, v) -> v.writeTo(o));
        out.writeVInt(this.shardSizes.size());
        for (var cursor : shardSizes) {
            String key = cursor.key;
            long size = cursor.value;
            out.writeString(key);
            out.writeLong(size);
        }
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
     * Returns the shard size for the given shard routing or <code>0</code> it that metric is not available.
     */
    public long getShardSize(ShardRouting shardRouting) {
        return shardSizes.get(shardIdentifierFromRouting(shardRouting));
    }

    /**
     * Returns the shard size for the given shard routing or <code>defaultValue</code> it that metric is not available.
     */
    public long getShardSize(ShardRouting shardRouting, long defaultValue) {
        return shardSizes.getOrDefault(shardIdentifierFromRouting(shardRouting), defaultValue);
    }

    /**
     * Returns the nodes absolute data-path the given shard is allocated on or <code>null</code> if the information is not available.
     */
    public String getDataPath(ShardRouting shardRouting) {
        return routingToDataPath.get(shardRouting);
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
    public record NodeAndPath(String nodeId, String path) implements Writeable {

        public NodeAndPath(StreamInput in) throws IOException {
            this(in.readString(), in.readString());
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
    public record ReservedSpace(long total, HashSet<ShardId> shardIds) implements Writeable {

        public static final ReservedSpace EMPTY = new ReservedSpace(0, new HashSet<>());

        public static ReservedSpace of(StreamInput in) throws IOException {
            long total = in.readVLong();
            final int shardIdCount = in.readVInt();
            HashSet<ShardId> shardIds = HashSet.newHashSet(shardIdCount);
            for (int i = 0; i < shardIdCount; i++) {
                shardIds.add(new ShardId(in));
            }
            return new ReservedSpace(total, shardIds);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(total);
            out.writeVInt(shardIds.size());
            for (ShardId shardId : shardIds) {
                shardId.writeTo(out);
            }
        }

        public boolean containsShardId(ShardId shardId) {
            return shardIds.contains(shardId);
        }

        public static class Builder {
            private long total;
            private HashSet<ShardId> shardIds = new HashSet<>();

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
