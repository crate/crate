package org.cratedb.blob;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.UUID;

public class PutChunkReplicaRequest extends ActionRequest<PutChunkReplicaRequest> implements  IPutChunkRequest {


    public String sourceNodeId;
    public UUID transferId;
    public long currentPos;
    public BytesReference content;
    public boolean isLast;

    public PutChunkReplicaRequest() {
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        sourceNodeId = in.readString();
        transferId = new UUID(in.readLong(), in.readLong());
        currentPos = in.readVInt();
        content = in.readBytesReference();
        isLast = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(sourceNodeId);
        out.writeLong(transferId.getMostSignificantBits());
        out.writeLong(transferId.getLeastSignificantBits());
        out.writeVLong(currentPos);
        out.writeBytesReference(content);
        out.writeBoolean(isLast);
    }

    public BytesReference content() {
        return content;
    }

    public UUID transferId() {
        return transferId;
    }

    public boolean isLast() {
        return isLast;
    }
}
