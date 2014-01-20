package io.crate.metadata;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import io.crate.metadata.sys.SystemReferences;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

import java.util.Map;

@Singleton
public class RoutingsService implements Routings {


    private final ClusterService clusterService;

    @Inject
    public RoutingsService(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    @Override
    public Routing getRouting(TableIdent tableIdent) {
        Preconditions.checkState(SystemReferences.NODES_IDENT.equals(tableIdent),
                "Table ident not supported", tableIdent);
        DiscoveryNodes nodes = clusterService.state().nodes();
        ImmutableMap.Builder<String, Map<String, Integer>> builder = ImmutableMap.<String, Map<String, Integer>>builder();
        for (DiscoveryNode node : nodes) {
            builder.put(node.id(), ImmutableMap.<String, Integer>of());
        }
        return new Routing(builder.build());
    }

}
