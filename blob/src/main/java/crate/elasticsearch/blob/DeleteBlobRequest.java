package crate.elasticsearch.blob;

import crate.elasticsearch.common.Hex;
import org.elasticsearch.action.support.replication.ShardReplicationOperationRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class DeleteBlobRequest extends ShardReplicationOperationRequest<DeleteBlobRequest> {

    private byte[] digest;

    public DeleteBlobRequest() {
    }

    public DeleteBlobRequest(String index, byte[] digest) {
        this.digest = digest;
        this.index = index;
    }

    public String id() {
        return Hex.encodeHexString(digest);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        digest = new byte[20];
        in.read(digest);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.write(digest);
    }
}
