package crate.elasticsearch.blob.v2;

import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.operation.OperationRouting;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.ShardId;

public class BlobIndex extends AbstractIndexComponent {


    private final OperationRouting operationRouting;
    private final ClusterService clusterService;

    @Inject
    BlobIndex(Index index, @IndexSettings Settings indexSettings,
            OperationRouting operationRouting,
            ClusterService clusterService) {
        super(index, indexSettings);
        this.operationRouting = operationRouting;
        this.clusterService = clusterService;
    }

    public ShardId shardId(String digest) {
        ShardIterator si = operationRouting.getShards(clusterService.state(), index.getName(), null, null, digest, "_only_local");
        // TODO: check null and raise
        return si.shardId();
    }

}
