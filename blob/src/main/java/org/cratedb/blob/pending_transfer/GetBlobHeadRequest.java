package org.cratedb.blob.pending_transfer;

import org.elasticsearch.common.UUID;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class GetBlobHeadRequest extends BlobTransportRequest {

    public long endPos;

    public GetBlobHeadRequest() {
    }

    public GetBlobHeadRequest(String targetNodeId, UUID transferId, long endPos) {
        super(targetNodeId, transferId);
        this.endPos = endPos;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        endPos = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVLong(endPos);
    }
}
