package org.cratedb.sql;

import org.elasticsearch.rest.RestStatus;

public class InvalidTableNameException extends CrateException {

    private String tableName;

    public InvalidTableNameException(String tableName, Throwable e) {
        super("Invalid table name", e);
        this.tableName = tableName;
    }

    public InvalidTableNameException(String tableName) {
        super("Invalid table name");
        this.tableName = tableName;
    }

    @Override
    public int errorCode() {
        return 4002;
    }

    @Override
    public RestStatus status() {
        return RestStatus.BAD_REQUEST;
    }

    @Override
    public Object[] args() {
        return new Object[]{tableName};
    }
}
