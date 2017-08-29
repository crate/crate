package io.crate.metadata.shard.unassigned;

import io.crate.blob.v2.BlobIndex;
import io.crate.metadata.PartitionName;
import io.crate.metadata.Schemas;
import io.crate.metadata.blob.BlobSchemaInfo;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.index.shard.ShardId;

/**
 * This class represents an unassigned shard
 * <p>
 * An UnassignedShard is a shard that is not yet assigned to any node and therefore doesn't really exist anywhere.
 * <p>
 * The {@link io.crate.metadata.sys.SysShardsTableInfo} will encode any shards that aren't assigned to a node
 * by negating them using {@link #markUnassigned(int)}
 * <p>
 * The {@link io.crate.operation.collect.sources.ShardCollectSource} will then collect UnassignedShard
 * instances for all shardIds that are negative.
 * <p>
 * This is only for "select ... from sys.shards" queries.
 */
public class UnassignedShard {

    public static boolean isUnassigned(int shardId) {
        return shardId < 0;
    }

    /**
     * valid shard ids are vom 0 to int.max
     * this method negates a shard id (0 becomes -1, 1 becomes -2, etc.)
     * <p>
     * if the given id is already negative it is returned as is
     */
    public static int markUnassigned(int id) {
        if (id >= 0) {
            return (id + 1) * -1;
        }
        return id;
    }

    /**
     * converts negative shard ids back to positive
     * (-1 becomes 0, -2 becomes 1 - the opposite of {@link #markUnassigned(int)}
     */
    public static int markAssigned(int shard) {
        if (shard < 0) {
            return (shard * -1) - 1;
        }
        return shard;
    }

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
        String index = shardId.getIndexName();
        boolean isBlobIndex = BlobIndex.isBlobIndex(index);
        final String tableName;
        final String ident;
        if (isBlobIndex) {
            this.schemaName = BlobSchemaInfo.NAME;
            tableName = BlobIndex.stripPrefix(index);
            ident = "";
        } else if (PartitionName.isPartition(index)) {
            PartitionName partitionName = PartitionName.fromIndexOrTemplate(index);
            schemaName = partitionName.tableIdent().schema();
            tableName = partitionName.tableIdent().name();
            ident = partitionName.ident();
            if (!clusterService.state().metaData().hasConcreteIndex(tableName)) {
                orphanedPartition = true;
            }
        } else {
            Schemas.SchemaAndTableName schemaAndTableName = Schemas.getSchemaAndTableName(index);
            this.schemaName = schemaAndTableName.schemaName;
            tableName = schemaAndTableName.tableName;
            ident = "";
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
