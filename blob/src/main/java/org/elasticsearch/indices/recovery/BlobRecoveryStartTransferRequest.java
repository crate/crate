package org.elasticsearch.indices.recovery;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

public class BlobRecoveryStartTransferRequest extends BlobRecoveryRequest {

    private static final AtomicLong transferIdGenerator = new AtomicLong();
    private String path;
    private BytesReference content;
    private long size;
    private long transferId;

    public BlobRecoveryStartTransferRequest() {
    }

    public BlobRecoveryStartTransferRequest(long recoveryId, String path, BytesArray content, long size) {
        super(recoveryId);
        this.path = path;
        this.content = content;
        this.size = size;
        this.transferId = transferIdGenerator.incrementAndGet();
    }

    public String path() {
        return path;
    }

    public BytesReference content() {
        return content;
    }

    public long size() {
        return size;
    }

    public long transferId() {
        return transferId;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        path = in.readString();
        content = in.readBytesReference();
        size = in.readVLong();
        transferId = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(path);
        out.writeBytesReference(content);
        out.writeVLong(size);
        out.writeVLong(transferId);
    }
}
