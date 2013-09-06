package org.cratedb.blob.pending_transfer;

import org.elasticsearch.common.UUID;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;

public class PutBlobHeadChunkRequest extends TransportRequest {

    public UUID transferId;
    public BytesReference content;

    public PutBlobHeadChunkRequest() {}

    public PutBlobHeadChunkRequest(UUID transferId, BytesArray bytesArray) {
        this.transferId = transferId;
        this.content = bytesArray;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        transferId = new UUID(in.readLong(), in.readLong());
        content = in.readBytesReference();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeLong(transferId.getMostSignificantBits());
        out.writeLong(transferId.getLeastSignificantBits());
        out.writeBytesReference(content);
    }
}
