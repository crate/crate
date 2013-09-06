package org.cratedb.blob.pending_transfer;

import org.elasticsearch.ElasticSearchException;

public class HeadChunkFileTooSmallException extends ElasticSearchException {

    public HeadChunkFileTooSmallException(String msg) {
        super(msg);
    }
}
