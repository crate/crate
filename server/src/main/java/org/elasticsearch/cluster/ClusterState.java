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
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.coordination.CoordinationMetadata;
import org.elasticsearch.cluster.coordination.CoordinationMetadata.VotingConfigExclusion;
import org.elasticsearch.cluster.coordination.CoordinationMetadata.VotingConfiguration;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.VersionedNamedWriteable;
import org.elasticsearch.common.settings.Settings;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;

/**
 * Represents the current state of the cluster.
 * <p>
 * The cluster state object is immutable with the exception of the {@link RoutingNodes} structure, which is
 * built on demand from the {@link RoutingTable}.
 * The cluster state can be updated only on the master node. All updates are performed by on a
 * single thread and controlled by the {@link ClusterService}. After every update the
 * {@link Discovery#publish} method publishes a new version of the cluster state to all other nodes in the
 * cluster. The actual publishing mechanism is delegated to the {@link Discovery#publish} method and depends on
 * the type of discovery.
 * <p>
 * The cluster state implements the {@link Diffable} interface in order to support publishing of cluster state
 * differences instead of the entire state on each change. The publishing mechanism should only send differences
 * to a node if this node was present in the previous version of the cluster state. If a node was
 * not present in the previous version of the cluster state, this node is unlikely to have the previous cluster
 * state version and should be sent a complete version. In order to make sure that the differences are applied to the
 * correct version of the cluster state, each cluster state version update generates {@link #stateUUID} that uniquely
 * identifies this version of the state. This uuid is verified by the {@link ClusterStateDiff#apply} method to
 * make sure that the correct diffs are applied. If uuids don’t match, the {@link ClusterStateDiff#apply} method
 * throws the {@link IncompatibleClusterStateVersionException}, which causes the publishing mechanism to send
 * a full version of the cluster state to the node on which this exception was thrown.
 */
public class ClusterState implements Diffable<ClusterState> {

    public static final ClusterState EMPTY_STATE = builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY)).build();

    public interface Custom extends NamedDiffable<Custom> {

        /**
         * Returns <code>true</code> iff this {@link Custom} is private to the cluster and should never be send to a client.
         * The default is <code>false</code>;
         */
        default boolean isPrivate() {
            return false;
        }

    }

    private static final NamedDiffableValueSerializer<Custom> CUSTOM_VALUE_SERIALIZER = new NamedDiffableValueSerializer<>(Custom.class);

    public static final String UNKNOWN_UUID = "_na_";

    public static final long UNKNOWN_VERSION = -1;

    private final long version;

    private final String stateUUID;

    private final RoutingTable routingTable;

    private final DiscoveryNodes nodes;

    private final Metadata metadata;

    private final ClusterBlocks blocks;

    private final ImmutableOpenMap<String, Custom> customs;

    private final ClusterName clusterName;

    private final boolean wasReadFromDiff;

    // built on demand
    private volatile RoutingNodes routingNodes;

    public ClusterState(long version, String stateUUID, ClusterState state) {
        this(state.clusterName, version, stateUUID, state.metadata(), state.routingTable(), state.nodes(), state.blocks(),
                state.customs(), false);
    }

    public ClusterState(ClusterName clusterName, long version, String stateUUID, Metadata metadata, RoutingTable routingTable,
                        DiscoveryNodes nodes, ClusterBlocks blocks, ImmutableOpenMap<String, Custom> customs,
                        boolean wasReadFromDiff) {
        this.version = version;
        this.stateUUID = stateUUID;
        this.clusterName = clusterName;
        this.metadata = metadata;
        this.routingTable = routingTable;
        this.nodes = nodes;
        this.blocks = blocks;
        this.customs = customs;
        this.wasReadFromDiff = wasReadFromDiff;
    }

    public long term() {
        return coordinationMetadata().term();
    }

    public long version() {
        return this.version;
    }

    /**
     * This stateUUID is automatically generated for for each version of cluster state. It is used to make sure that
     * we are applying diffs to the right previous state.
     */
    public String stateUUID() {
        return this.stateUUID;
    }

    public DiscoveryNodes nodes() {
        return this.nodes;
    }

    public Metadata metadata() {
        return this.metadata;
    }

    public CoordinationMetadata coordinationMetadata() {
        return metadata.coordinationMetadata();
    }

    public RoutingTable routingTable() {
        return routingTable;
    }

    public ClusterBlocks blocks() {
        return this.blocks;
    }

    public ImmutableOpenMap<String, Custom> customs() {
        return this.customs;
    }

    @SuppressWarnings("unchecked")
    public <T extends Custom> T custom(String type) {
        return (T) customs.get(type);
    }

    @SuppressWarnings("unchecked")
    public <T extends Custom> T custom(String type, T defaultValue) {
        return (T) customs.getOrDefault(type, defaultValue);
    }

    public ClusterName getClusterName() {
        return this.clusterName;
    }

    public VotingConfiguration getLastAcceptedConfiguration() {
        return coordinationMetadata().getLastAcceptedConfiguration();
    }

    public VotingConfiguration getLastCommittedConfiguration() {
        return coordinationMetadata().getLastCommittedConfiguration();
    }

    public Set<VotingConfigExclusion> getVotingConfigExclusions() {
        return coordinationMetadata().getVotingConfigExclusions();
    }

    /**
     * Returns a built (on demand) routing nodes view of the routing table.
     */
    public RoutingNodes getRoutingNodes() {
        if (routingNodes != null) {
            return routingNodes;
        }
        routingNodes = new RoutingNodes(this);
        return routingNodes;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        final String TAB = "   ";
        sb.append("cluster uuid: ").append(metadata.clusterUUID())
            .append(" [committed: ").append(metadata.clusterUUIDCommitted()).append("]").append("\n");
        sb.append("version: ").append(version).append("\n");
        sb.append("state uuid: ").append(stateUUID).append("\n");
        sb.append("from_diff: ").append(wasReadFromDiff).append("\n");
        sb.append("meta data version: ").append(metadata.version()).append("\n");
        sb.append(TAB).append("coordination_metadata:\n");
        sb.append(TAB).append(TAB).append("term: ").append(coordinationMetadata().term()).append("\n");
        sb.append(TAB).append(TAB)
                .append("last_committed_config: ").append(coordinationMetadata().getLastCommittedConfiguration()).append("\n");
        sb.append(TAB).append(TAB)
                .append("last_accepted_config: ").append(coordinationMetadata().getLastAcceptedConfiguration()).append("\n");
        sb.append(TAB).append(TAB)
                .append("voting tombstones: ").append(coordinationMetadata().getVotingConfigExclusions()).append("\n");
        for (IndexMetadata indexMetadata : metadata) {
            sb.append(TAB).append(indexMetadata.getIndex());
            sb.append(": v[").append(indexMetadata.getVersion())
                    .append("], mv[").append(indexMetadata.getMappingVersion())
                    .append("], sv[").append(indexMetadata.getSettingsVersion())
                    .append("]\n");
            for (int shard = 0; shard < indexMetadata.getNumberOfShards(); shard++) {
                sb.append(TAB).append(TAB).append(shard).append(": ");
                sb.append("p_term [").append(indexMetadata.primaryTerm(shard)).append("], ");
                sb.append("isa_ids ").append(indexMetadata.inSyncAllocationIds(shard)).append("\n");
            }
        }
        if (metadata.customs().isEmpty() == false) {
            sb.append("metadata customs:\n");
            for (final ObjectObjectCursor<String, Metadata.Custom> cursor : metadata.customs()) {
                final String type = cursor.key;
                final Metadata.Custom custom = cursor.value;
                sb.append(TAB).append(type).append(": ").append(custom);
            }
            sb.append("\n");
        }
        sb.append(blocks());
        sb.append(nodes());
        sb.append(routingTable());
        sb.append(getRoutingNodes());
        if (customs.isEmpty() == false) {
            sb.append("customs:\n");
            for (ObjectObjectCursor<String, Custom> cursor : customs) {
                final String type = cursor.key;
                final Custom custom = cursor.value;
                sb.append(TAB).append(type).append(": ").append(custom);
            }
        }
        return sb.toString();
    }

    /**
     * a cluster state supersedes another state if they are from the same master and the version of this state is higher than that of the
     * other state.
     * <p>
     * In essence that means that all the changes from the other cluster state are also reflected by the current one
     */
    public boolean supersedes(ClusterState other) {
        return this.nodes().getMasterNodeId() != null && this.nodes().getMasterNodeId().equals(other.nodes().getMasterNodeId())
            && this.version() > other.version();

    }

    public enum Metric {
        VERSION("version"),
        MASTER_NODE("master_node"),
        BLOCKS("blocks"),
        NODES("nodes"),
        METADATA("metadata"),
        ROUTING_TABLE("routing_table"),
        ROUTING_NODES("routing_nodes"),
        CUSTOMS("customs");

        private static Map<String, Metric> valueToEnum;

        static {
            valueToEnum = new HashMap<>();
            for (Metric metric : Metric.values()) {
                valueToEnum.put(metric.value, metric);
            }
        }

        private final String value;

        Metric(String value) {
            this.value = value;
        }

        public static EnumSet<Metric> parseString(String param, boolean ignoreUnknown) {
            String[] metrics = Strings.splitStringByCommaToArray(param);
            EnumSet<Metric> result = EnumSet.noneOf(Metric.class);
            for (String metric : metrics) {
                if ("_all".equals(metric)) {
                    result = EnumSet.allOf(Metric.class);
                    break;
                }
                Metric m = valueToEnum.get(metric);
                if (m == null) {
                    if (!ignoreUnknown) {
                        throw new IllegalArgumentException("Unknown metric [" + metric + "]");
                    }
                } else {
                    result.add(m);
                }
            }
            return result;
        }

        @Override
        public String toString() {
            return value;
        }
    }

    public static Builder builder(ClusterName clusterName) {
        return new Builder(clusterName);
    }

    public static Builder builder(ClusterState state) {
        return new Builder(state);
    }

    public static class Builder {

        private final ClusterName clusterName;
        private long version = 0;
        private String uuid = UNKNOWN_UUID;
        private Metadata metadata = Metadata.EMPTY_METADATA;
        private RoutingTable routingTable = RoutingTable.EMPTY_ROUTING_TABLE;
        private DiscoveryNodes nodes = DiscoveryNodes.EMPTY_NODES;
        private ClusterBlocks blocks = ClusterBlocks.EMPTY_CLUSTER_BLOCK;
        private final ImmutableOpenMap.Builder<String, Custom> customs;
        private boolean fromDiff;

        public Builder(ClusterState state) {
            this.clusterName = state.clusterName;
            this.version = state.version();
            this.uuid = state.stateUUID();
            this.nodes = state.nodes();
            this.routingTable = state.routingTable();
            this.metadata = state.metadata();
            this.blocks = state.blocks();
            this.customs = ImmutableOpenMap.builder(state.customs());
            this.fromDiff = false;
        }

        public Builder(ClusterName clusterName) {
            customs = ImmutableOpenMap.builder();
            this.clusterName = clusterName;
        }

        public Builder nodes(DiscoveryNodes.Builder nodesBuilder) {
            return nodes(nodesBuilder.build());
        }

        public Builder nodes(DiscoveryNodes nodes) {
            this.nodes = nodes;
            return this;
        }

        public DiscoveryNodes nodes() {
            return nodes;
        }

        public Builder routingTable(RoutingTable routingTable) {
            this.routingTable = routingTable;
            return this;
        }

        public Builder metadata(Metadata.Builder metadataBuilder) {
            return metadata(metadataBuilder.build());
        }

        public Builder metadata(Metadata metadata) {
            this.metadata = metadata;
            return this;
        }

        public Builder blocks(ClusterBlocks.Builder blocksBuilder) {
            return blocks(blocksBuilder.build());
        }

        public Builder blocks(ClusterBlocks blocks) {
            this.blocks = blocks;
            return this;
        }

        public Builder version(long version) {
            this.version = version;
            return this;
        }

        public Builder incrementVersion() {
            this.version = version + 1;
            this.uuid = UNKNOWN_UUID;
            return this;
        }

        public Builder stateUUID(String uuid) {
            this.uuid = uuid;
            return this;
        }

        public Builder putCustom(String type, Custom custom) {
            customs.put(type, custom);
            return this;
        }

        public Builder removeCustom(String type) {
            customs.remove(type);
            return this;
        }

        public Builder customs(ImmutableOpenMap<String, Custom> customs) {
            this.customs.putAll(customs);
            return this;
        }

        public Builder fromDiff(boolean fromDiff) {
            this.fromDiff = fromDiff;
            return this;
        }

        public ClusterState build() {
            if (UNKNOWN_UUID.equals(uuid)) {
                uuid = UUIDs.randomBase64UUID();
            }
            return new ClusterState(clusterName, version, uuid, metadata, routingTable, nodes, blocks, customs.build(), fromDiff);
        }
    }

    @Override
    public Diff<ClusterState> diff(ClusterState previousState) {
        return new ClusterStateDiff(previousState, this);
    }

    public static Diff<ClusterState> readDiffFrom(StreamInput in, DiscoveryNode localNode) throws IOException {
        return new ClusterStateDiff(in, localNode);
    }

    public static ClusterState readFrom(StreamInput in, DiscoveryNode localNode) throws IOException {
        ClusterName clusterName = new ClusterName(in);
        Builder builder = new Builder(clusterName);
        builder.version = in.readLong();
        builder.uuid = in.readString();
        builder.metadata = Metadata.readFrom(in);
        builder.routingTable = RoutingTable.readFrom(in);
        builder.nodes = DiscoveryNodes.readFrom(in, localNode);
        builder.blocks = ClusterBlocks.readFrom(in);
        int customSize = in.readVInt();
        for (int i = 0; i < customSize; i++) {
            Custom customIndexMetadata = in.readNamedWriteable(Custom.class);
            builder.putCustom(customIndexMetadata.getWriteableName(), customIndexMetadata);
        }
        return builder.build();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        clusterName.writeTo(out);
        out.writeLong(version);
        out.writeString(stateUUID);
        metadata.writeTo(out);
        routingTable.writeTo(out);
        nodes.writeTo(out);
        blocks.writeTo(out);
        // filter out custom states not supported by the other node
        int numberOfCustoms = 0;
        for (final ObjectCursor<Custom> cursor : customs.values()) {
            if (VersionedNamedWriteable.shouldSerialize(out, cursor.value)) {
                numberOfCustoms++;
            }
        }
        out.writeVInt(numberOfCustoms);
        for (final ObjectCursor<Custom> cursor : customs.values()) {
            if (VersionedNamedWriteable.shouldSerialize(out, cursor.value)) {
                out.writeNamedWriteable(cursor.value);
            }
        }
    }

    private static class ClusterStateDiff implements Diff<ClusterState> {

        private final long toVersion;

        private final String fromUuid;

        private final String toUuid;

        private final ClusterName clusterName;

        private final Diff<RoutingTable> routingTable;

        private final Diff<DiscoveryNodes> nodes;

        private final Diff<Metadata> metadata;

        private final Diff<ClusterBlocks> blocks;

        private final Diff<ImmutableOpenMap<String, Custom>> customs;

        ClusterStateDiff(ClusterState before, ClusterState after) {
            fromUuid = before.stateUUID;
            toUuid = after.stateUUID;
            toVersion = after.version;
            clusterName = after.clusterName;
            routingTable = after.routingTable.diff(before.routingTable);
            nodes = after.nodes.diff(before.nodes);
            metadata = after.metadata.diff(before.metadata);
            blocks = after.blocks.diff(before.blocks);
            customs = Diffs.diff(before.customs, after.customs, Diffs.stringKeySerializer(), CUSTOM_VALUE_SERIALIZER);
        }

        ClusterStateDiff(StreamInput in, DiscoveryNode localNode) throws IOException {
            clusterName = new ClusterName(in);
            fromUuid = in.readString();
            toUuid = in.readString();
            toVersion = in.readLong();
            routingTable = RoutingTable.readDiffFrom(in);
            nodes = DiscoveryNodes.readDiffFrom(in, localNode);
            metadata = Metadata.readDiffFrom(in);
            blocks = ClusterBlocks.readDiffFrom(in);
            customs = Diffs.readMapDiff(in, Diffs.stringKeySerializer(), CUSTOM_VALUE_SERIALIZER);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            clusterName.writeTo(out);
            out.writeString(fromUuid);
            out.writeString(toUuid);
            out.writeLong(toVersion);
            routingTable.writeTo(out);
            nodes.writeTo(out);
            metadata.writeTo(out);
            blocks.writeTo(out);
            customs.writeTo(out);
        }

        @Override
        public ClusterState apply(ClusterState state) {
            Builder builder = new Builder(clusterName);
            if (toUuid.equals(state.stateUUID)) {
                // no need to read the rest - cluster state didn't change
                return state;
            }
            if (fromUuid.equals(state.stateUUID) == false) {
                throw new IncompatibleClusterStateVersionException(state.version, state.stateUUID, toVersion, fromUuid);
            }
            builder.stateUUID(toUuid);
            builder.version(toVersion);
            builder.routingTable(routingTable.apply(state.routingTable));
            builder.nodes(nodes.apply(state.nodes));
            builder.metadata(metadata.apply(state.metadata));
            builder.blocks(blocks.apply(state.blocks));
            builder.customs(customs.apply(state.customs));
            builder.fromDiff(true);
            return builder.build();
        }
    }
}
