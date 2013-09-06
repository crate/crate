package org.cratedb.blob.exceptions;

import org.elasticsearch.ElasticSearchException;

public class IllegalBlobRecoveryStateException extends ElasticSearchException {

    public IllegalBlobRecoveryStateException(String msg) {
        super(msg);
    }
}
