package org.cratedb.blob.pending_transfer;


import java.util.UUID;

public class BlobInfoRequest extends BlobTransportRequest {

    public BlobInfoRequest() {

    }

    public BlobInfoRequest(String senderNodeId, UUID transferId) {
        super(senderNodeId, transferId);
    }
}
