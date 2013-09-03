package org.elasticsearch.indices.recovery;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class BlobRecoveryChunkRequest extends BlobRecoveryRequest {

    private long transferId;
    private BytesReference content;
    private boolean isLast;

    public BlobRecoveryChunkRequest() {

    }

    public BlobRecoveryChunkRequest(long requestId, long transferId, BytesArray content, boolean isLast) {
        super(requestId);
        this.transferId = transferId;
        this.content = content;
        this.isLast = isLast;
    }

    public BytesReference content() {
        return content;
    }

    public long transferId() {
        return transferId;
    }

    public boolean isLast() {
        return isLast;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        transferId = in.readVLong();
        content = in.readBytesReference();
        isLast = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVLong(transferId);
        out.writeBytesReference(content);
        out.writeBoolean(isLast);
    }
}
