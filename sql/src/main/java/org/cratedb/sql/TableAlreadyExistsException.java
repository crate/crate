package org.cratedb.sql;

import org.elasticsearch.rest.RestStatus;

public class TableAlreadyExistsException extends CrateException {

    public TableAlreadyExistsException(String msg, Throwable e) {
        super(msg, e);
    }

    @Override
    public int errorCode() {
        return 4093;
    }

    @Override
    public RestStatus status() {
        return RestStatus.CONFLICT;
    }

}
