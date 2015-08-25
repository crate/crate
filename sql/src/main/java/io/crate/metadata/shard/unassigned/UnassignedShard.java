package io.crate.metadata.shard.unassigned;

import io.crate.metadata.PartitionName;
import io.crate.blob.v2.BlobIndices;
import io.crate.metadata.Schemas;
import io.crate.metadata.blob.BlobSchemaInfo;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.index.shard.ShardId;

import java.util.regex.Matcher;

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
            tableName = BlobIndices.STRIP_PREFIX.apply(index);
        } else if (PartitionName.isPartition(index)) {
            schemaName = PartitionName.schemaName(index);
            tableName = PartitionName.tableName(index);
            ident = PartitionName.ident(index);
            if (!clusterService.state().metaData().hasConcreteIndex(tableName)) {
                orphanedPartition = true;
            }
        } else {
            Matcher matcher = Schemas.SCHEMA_PATTERN.matcher(index);
            if (matcher.matches()) {
                this.schemaName = matcher.group(1);
                tableName = matcher.group(2);
            } else {
                this.schemaName = Schemas.DEFAULT_SCHEMA_NAME;
                tableName = index;
            }
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
