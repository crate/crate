package crate.elasticsearch.blob.pending_transfer;

import org.elasticsearch.common.UUID;

public class BlobInfoRequest extends BlobTransportRequest {

    public BlobInfoRequest() {

    }

    public BlobInfoRequest(String senderNodeId, UUID transferId) {
        super(senderNodeId, transferId);
    }
}
