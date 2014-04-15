package io.crate.metadata.shard.unassigned;

import io.crate.PartitionName;

public class UnassignedShard {

    private final String schemaName;
    private final String tableName;
    private final int id;
    private final String partitionIdent;

    public UnassignedShard(String schemaName, String tableName, int id) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.id = id;
        String ident = "";
        try {
            ident = PartitionName.ident(tableName);
        } catch (IllegalArgumentException e) {
            // no partition
        }
        partitionIdent = ident;
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
}
