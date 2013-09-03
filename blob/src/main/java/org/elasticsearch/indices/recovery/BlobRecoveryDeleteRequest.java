package org.elasticsearch.indices.recovery;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class BlobRecoveryDeleteRequest extends BlobRecoveryRequest {

    public BytesReference[] digests;

    public BlobRecoveryDeleteRequest() {
    }

    public BlobRecoveryDeleteRequest(long recoveryId, BytesArray[] digests) {
        super(recoveryId);
        this.digests = digests;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        digests = new BytesReference[in.readVInt()];
        for (int i = 0; i < digests.length; i++) {
            digests[i] = in.readBytesReference();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(digests.length);
        for (int i = 0; i < digests.length; i++) {
            out.writeBytesReference(digests[i]);
        }
    }
}
