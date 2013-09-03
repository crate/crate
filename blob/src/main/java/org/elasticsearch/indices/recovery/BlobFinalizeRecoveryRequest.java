package org.elasticsearch.indices.recovery;

public class BlobFinalizeRecoveryRequest extends BlobRecoveryRequest {

    public BlobFinalizeRecoveryRequest() {

    }

    public BlobFinalizeRecoveryRequest(long recoveryId) {
        super(recoveryId);
    }
}
