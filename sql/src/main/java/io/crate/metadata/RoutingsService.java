package io.crate.metadata;

import com.google.common.collect.ImmutableMap;
import io.crate.metadata.sys.SystemReferences;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.support.IgnoreIndices;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.index.shard.ShardId;

import java.util.*;

@Singleton
public class RoutingsService implements Routings {

    private final ClusterService clusterService;
    private static final Map<String, Set<String>> esRouting = Collections.emptyMap();

    @Inject
    public RoutingsService(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    @Override
    public Routing getRouting(TableIdent tableIdent) {
        if (SystemReferences.NODES_IDENT.equals(tableIdent)) {
            return allNodes();
        }
        if (SystemReferences.CLUSTER_IDENT.equals(tableIdent)) {
            return new Routing(null);
        }

        if (SystemReferences.SHARDS_IDENT.equals(tableIdent)) {
            return allShards();
        }

        Map<String, Map<String, Set<Integer>>> routing = new HashMap<>();

        final ClusterState state = clusterService.state();
        final String[] indices = new String[] { tableIdent.name() };
        GroupShardsIterator shardIterators = clusterService.operationRouting().searchShards(
                state,
                indices,
                state.metaData().concreteIndices(indices, IgnoreIndices.NONE, true),
                esRouting,
                null
        );

        ShardRouting shardRouting;
        for (ShardIterator shardIterator : shardIterators.iterators()) {

            shardRouting = shardIterator.firstOrNull();
            processShardRouting(routing, shardRouting, shardIterator.shardId());
        }

        return new Routing(routing);
    }

    private void processShardRouting(Map<String, Map<String, Set<Integer>>> routing, ShardRouting shardRouting, ShardId shardId) {
        String node;
        if (shardRouting == null) {
            throw new NoShardAvailableActionException(shardId);
        }

        node = shardRouting.currentNodeId();

        Map<String, Set<Integer>> nodeMap = routing.get(node);
        if (nodeMap == null) {
            nodeMap = new HashMap<>();
            routing.put(shardRouting.currentNodeId(), nodeMap);
        }

        Set<Integer> shards = nodeMap.get(shardRouting.getIndex());
        if (shards == null) {
            shards = new HashSet<>();
            nodeMap.put(shardRouting.getIndex(), shards);
        }

        shards.add(shardRouting.id());
    }

    private Routing allShards() {
        Map<String, Map<String, Set<Integer>>> routing = new HashMap<>();
        for (ShardRouting shardRouting : clusterService.state().routingTable().allShards()) {
            processShardRouting(routing, shardRouting, null);
        }

        return new Routing(routing);
    }

    private Routing allNodes() {
        DiscoveryNodes nodes = clusterService.state().nodes();
        ImmutableMap.Builder<String, Map<String, Set<Integer>>> builder = ImmutableMap.builder();

        for (DiscoveryNode node : nodes) {
            builder.put(node.id(), ImmutableMap.<String, Set<Integer>>of());
        }

        return new Routing(builder.build());
    }
}
