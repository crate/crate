package io.crate.metadata.shard.unassigned;

import io.crate.PartitionName;
import io.crate.blob.v2.BlobIndices;
import io.crate.metadata.blob.BlobSchemaInfo;
import io.crate.metadata.doc.DocSchemaInfo;
import org.elasticsearch.index.shard.ShardId;

public class UnassignedShard {

    private final String schemaName;
    private final String tableName;
    private final Boolean primary;
    private final int id;
    private final String partitionIdent;

    public UnassignedShard(ShardId shardId, Boolean primary) {
        String index = shardId.index().name();
        boolean isBlobIndex = BlobIndices.isBlobIndex(index);
        String tableName = null;
        if (isBlobIndex) {
            this.schemaName = BlobSchemaInfo.NAME;
            tableName = BlobIndices.stripPrefix.apply(index);
        } else {
            this.schemaName = DocSchemaInfo.NAME;
            tableName = index;
        }

        try {
            tableName = PartitionName.tableName(index);
        } catch (IllegalArgumentException e) {
            // no partition
        }
        this.tableName = tableName;

        String ident = "";
        try {
            ident = PartitionName.ident(tableName);
        } catch (IllegalArgumentException e) {
            // no partition
        }

        partitionIdent = ident;
        this.primary = primary;
        this.id = shardId.id();
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
}
