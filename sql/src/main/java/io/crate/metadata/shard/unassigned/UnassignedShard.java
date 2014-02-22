package io.crate.metadata.shard.unassigned;

public class UnassignedShard {

    private final String tableName;
    private final int id;

    public UnassignedShard(String tableName, int id) {
        this.tableName = tableName;
        this.id = id;
    }

    public String tableName() {
        return tableName;
    }

    public int id() {
        return id;
    }
}
