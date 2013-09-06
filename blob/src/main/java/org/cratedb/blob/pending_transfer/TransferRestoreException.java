package org.cratedb.blob.pending_transfer;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.UUID;

public class TransferRestoreException extends ElasticSearchException{

    public UUID transferId;

    public TransferRestoreException(String msg, UUID transferId, Throwable cause) {
        super(msg, cause);
        this.transferId = transferId;
    }
}
