package io.crate.metadata.shard.unassigned;

import io.crate.PartitionName;
import io.crate.blob.v2.BlobIndices;
import io.crate.metadata.blob.BlobSchemaInfo;
import io.crate.metadata.doc.DocSchemaInfo;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.index.shard.ShardId;

public class UnassignedShard {

    private final String schemaName;
    private final String tableName;
    private final Boolean primary;
    private final int id;
    private final String partitionIdent;
    private final BytesRef state;

    private static final BytesRef UNASSIGNED = new BytesRef("UNASSIGNED");
    private static final BytesRef INITIALIZING = new BytesRef("INITIALIZING");

    private Boolean orphanedPartition = false;

    public UnassignedShard(ShardId shardId,
                           ClusterService clusterService,
                           Boolean primary,
                           ShardRoutingState state) {
        String index = shardId.index().name();
        boolean isBlobIndex = BlobIndices.isBlobIndex(index);
        String tableName;
        String ident = "";
        if (isBlobIndex) {
            this.schemaName = BlobSchemaInfo.NAME;
            tableName = BlobIndices.stripPrefix.apply(index);
        } else if (PartitionName.isPartition(index)) {
            schemaName = PartitionName.schemaName(index);
            tableName = PartitionName.tableName(index);
            ident = PartitionName.ident(index);
            if (!clusterService.state().metaData().hasConcreteIndex(tableName)) {
                orphanedPartition = true;
            }
        } else if (index.contains(".")) {
            String[] split = index.split("\\.");
            this.schemaName = split[0];
            tableName = split[1];
        } else {
            this.schemaName = DocSchemaInfo.NAME;
            tableName = index;
        }

        this.tableName = tableName;
        partitionIdent = ident;
        this.primary = primary;
        this.id = shardId.id();
        this.state = state == ShardRoutingState.UNASSIGNED ? UNASSIGNED : INITIALIZING;
    }

    public String tableName() {
        return tableName;
    }

    public int id() {
        return id;
    }

    public String schemaName() {
        return schemaName;
    }

    public String partitionIdent() {
        return partitionIdent;
    }

    public Boolean primary() {
        return primary;
    }

    public BytesRef state() {
        return state;
    }

    public Boolean orphanedPartition() {
        return orphanedPartition;
    }
}
