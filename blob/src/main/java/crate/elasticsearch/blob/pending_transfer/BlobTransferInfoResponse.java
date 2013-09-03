package crate.elasticsearch.blob.pending_transfer;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.TransportResponse;

import java.io.IOException;

public class BlobTransferInfoResponse extends TransportResponse {

    public String digest;
    public String index;

    public BlobTransferInfoResponse() {

    }

    public BlobTransferInfoResponse(String index, String digest) {
        this.index = index;
        this.digest = digest;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        digest = in.readString();
        index = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(digest);
        out.writeString(index);
    }
}
