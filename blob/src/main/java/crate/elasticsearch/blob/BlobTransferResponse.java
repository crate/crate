package crate.elasticsearch.blob;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class BlobTransferResponse extends ActionResponse {

    // current size of the file on the target
    private long size;
    private RemoteDigestBlob.Status status;

    public RemoteDigestBlob.Status status() {
        return status;
    }

    public BlobTransferResponse status(RemoteDigestBlob.Status status) {
        this.status = status;
        return this;
    }

    public long size() {
        return size;
    }

    public BlobTransferResponse size(long size) {
        this.size = size;
        return this;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        status = RemoteDigestBlob.Status.fromId(in.readByte());
        size = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeByte(status.id());
        out.writeVLong(size);
    }
}
