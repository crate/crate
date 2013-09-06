package org.cratedb.blob;

import org.elasticsearch.action.support.replication.ShardReplicationOperationRequest;
import org.elasticsearch.common.UUID;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Base Request Class for Blob Transfers
 */
public abstract class BlobTransferRequest<T extends ShardReplicationOperationRequest>
    extends ShardReplicationOperationRequest<T>
    implements IPutChunkRequest
{

    private boolean last;
    private UUID transferId;
    private BytesReference content;

    public BlobTransferRequest() {
    }

    public BytesReference content() {
        return content;
    }

    public boolean isLast(){
        return last;
    }

    public BlobTransferRequest(String index, UUID transferId, BytesArray content, boolean last) {
        this.index = index;
        this.transferId = transferId;
        this.content = content;
        this.last = last;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        transferId = new UUID(in.readLong(), in.readLong());
        content = in.readBytesReference();
        last = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeLong(transferId.getMostSignificantBits());
        out.writeLong(transferId.getLeastSignificantBits());
        out.writeBytesReference(content);
        out.writeBoolean(last);
    }

    public UUID transferId() {
        return transferId;
    }
}
