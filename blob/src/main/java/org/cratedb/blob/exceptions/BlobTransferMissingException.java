package org.cratedb.blob.exceptions;

import org.elasticsearch.ElasticSearchException;

public class BlobTransferMissingException extends ElasticSearchException {

    public BlobTransferMissingException(long transferId) {
        super(Long.toString(transferId));
    }
}
