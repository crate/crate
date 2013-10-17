package org.cratedb.sql;

import org.elasticsearch.rest.RestStatus;

public class TableAlreadyExistsException extends CrateException {

    public TableAlreadyExistsException(Throwable e) {
        super("A table with the same name exists already", e);
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
