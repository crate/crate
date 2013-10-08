package org.cratedb.blob;

import org.cratedb.common.Hex;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.UUID;

public class PutChunkRequest extends BlobTransferRequest<PutChunkRequest> implements  IPutChunkRequest {

    private byte[] digest;
    private long currentPos;

    public PutChunkRequest() {
    }

    public PutChunkRequest(String index, byte[] digest, UUID transferId,
                           BytesArray content, long currentPos, boolean last) {
        super(index, transferId, content, last);
        this.digest = digest;
        this.currentPos = currentPos;
    }

    public String digest(){
        return Hex.encodeHexString(digest);
    }

    public long currentPos() {
        return currentPos;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        digest = new byte[20];
        in.read(digest);
        currentPos = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.write(digest);
        out.writeVLong(currentPos);
    }
}
