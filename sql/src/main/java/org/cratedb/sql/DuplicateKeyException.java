package org.cratedb.sql;

import org.elasticsearch.rest.RestStatus;

public class DuplicateKeyException extends CrateException {

    public DuplicateKeyException(String msg, Throwable e) {
        super(msg, e);
    }

    @Override
    public int errorCode() {
        return 4091;
    }

    @Override
    public RestStatus status() {
        return RestStatus.CONFLICT;
    }
}
