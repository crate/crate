package org.elasticsearch.indices.recovery;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;

public class BlobStartRecoveryRequest extends BlobRecoveryRequest {

    private ShardId shardId;

    public BlobStartRecoveryRequest(long recoveryId, ShardId shardId) {
        super(recoveryId);
        this.shardId = shardId;
    }

    protected BlobStartRecoveryRequest() {
    }

    public ShardId shardId() {
        return shardId;
    }


    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        shardId = ShardId.readShardId(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        shardId.writeTo(out);
    }
}
