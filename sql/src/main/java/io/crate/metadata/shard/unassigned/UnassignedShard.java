package io.crate.metadata.shard.unassigned;

public class UnassignedShard {

    private final String schemaName;
    private final String tableName;
    private final int id;

    public UnassignedShard(String schemaName, String tableName, int id) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.id = id;
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
}
