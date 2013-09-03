package crate.elasticsearch.blob.stats;

import org.elasticsearch.action.support.broadcast.BroadcastShardOperationResponse;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.elasticsearch.cluster.routing.ImmutableShardRouting.readShardRoutingEntry;

public class BlobStatsShardResponse extends BroadcastShardOperationResponse {

    private BlobStats blobStats;
    private ShardRouting shardRouting;

    public BlobStatsShardResponse() {
        super();
        blobStats = new BlobStats();
    }

    public BlobStats blobStats() {
        return this.blobStats;
    }

    public ShardRouting shardRouting() {
        return this.shardRouting;
    }

    public BlobStatsShardResponse(String index, int shardId, BlobStats blobStats, ShardRouting shardRouting) {
        super(index, shardId);
        this.blobStats = blobStats;
        this.shardRouting = shardRouting;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        blobStats.readFrom(in);
        shardRouting = readShardRoutingEntry(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        blobStats.writeTo(out);
        shardRouting.writeTo(out);
    }
}
