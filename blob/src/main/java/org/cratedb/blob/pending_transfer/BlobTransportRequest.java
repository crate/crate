package org.cratedb.blob.pending_transfer;

import org.elasticsearch.common.UUID;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;

public abstract class BlobTransportRequest extends TransportRequest{

    public UUID transferId;
    public String senderNodeId;

    public BlobTransportRequest() {}

    public BlobTransportRequest(String senderNodeId, UUID transferId) {
        this.senderNodeId = senderNodeId;
        this.transferId = transferId;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        senderNodeId = in.readString();
        transferId = new UUID(in.readLong(), in.readLong());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(senderNodeId);
        out.writeLong(transferId.getMostSignificantBits());
        out.writeLong(transferId.getLeastSignificantBits());
    }
}
