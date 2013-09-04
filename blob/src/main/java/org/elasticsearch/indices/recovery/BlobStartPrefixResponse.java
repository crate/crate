package org.elasticsearch.indices.recovery;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.TransportResponse;

import java.io.IOException;

public class BlobStartPrefixResponse extends TransportResponse {
    public byte[][] existingDigests;

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        int size = in.readInt();
        existingDigests = new byte[size][20];
        for (int i=0; i<size; i++){
            in.read(existingDigests[i]);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeInt(existingDigests.length);
        for (byte[] digest: existingDigests){
            out.write(digest);
        }
    }
}
