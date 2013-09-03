package crate.elasticsearch.blob;

import crate.elasticsearch.common.Hex;
import org.elasticsearch.common.UUID;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class StartBlobRequest extends BlobTransferRequest<StartBlobRequest> {

    private byte[] digest;

    public StartBlobRequest() {
    }

    public StartBlobRequest(String index, byte[] digest, BytesArray content, boolean last) {
        super(index, UUID.randomUUID(), content, last);
        this.digest = digest;
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
