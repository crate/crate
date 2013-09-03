package crate.elasticsearch.blob;

import org.elasticsearch.common.UUID;

import java.util.concurrent.atomic.AtomicBoolean;

public class BlobTransferStatus {

    private final String index;
    private final UUID transferId;
    private final DigestBlob digestBlob;

    public BlobTransferStatus(String index, UUID transferId, DigestBlob digestBlob) {
        this.index = index;
        this.transferId = transferId;
        this.digestBlob = digestBlob;
    }

    public String index() {
        return index;
    }

    public DigestBlob digestBlob() {
        return digestBlob;
    }

    public UUID transferId() {
        return transferId;
    }
}
