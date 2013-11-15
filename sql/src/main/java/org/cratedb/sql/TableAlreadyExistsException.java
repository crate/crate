package org.cratedb.sql;

import org.elasticsearch.rest.RestStatus;

public class TableAlreadyExistsException extends CrateException {

    String tableName;

    public TableAlreadyExistsException(String tableName, Throwable e) {
        super("A table with the same name already exists", e);
        this.tableName = tableName;
    }

    @Override
    public int errorCode() {
        return 4093;
    }

    @Override
    public RestStatus status() {
        return RestStatus.CONFLICT;
    }

    @Override
    public Object[] args() {
        return new Object[]{tableName};
    }
}
