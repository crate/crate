package org.elasticsearch.indices.recovery;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;

public class BlobStartPrefixSyncRequest extends BlobRecoveryRequest {

    private byte prefix;
    private ShardId shardId;

    public BlobStartPrefixSyncRequest() {
    }

    public BlobStartPrefixSyncRequest(long recoveryId, ShardId shardId, byte prefix) {
        super(recoveryId);
        this.prefix = prefix;
        this.shardId = shardId;
    }

    public byte prefix() {
        return prefix;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        prefix = in.readByte();
        ShardId.readShardId(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeByte(prefix);
        shardId.writeTo(out);
    }

}


