package org.elasticsearch.indices.recovery;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;

public abstract class BlobRecoveryRequest extends TransportRequest {

    protected long recoveryId;

    public BlobRecoveryRequest(long recoveryId) {
        this.recoveryId = recoveryId;
    }

    protected BlobRecoveryRequest() {
    }

    public long recoveryId() {
        return this.recoveryId;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        recoveryId = in.readLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeLong(recoveryId);
    }
}
