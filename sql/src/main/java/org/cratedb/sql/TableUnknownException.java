package org.cratedb.sql;

import org.elasticsearch.rest.RestStatus;

public class TableUnknownException extends CrateException {

    private String tableName;

    public TableUnknownException(String tableName, Throwable e) {
        super("Unknown table", e);
        this.tableName = tableName;
    }

    public TableUnknownException(String tableName) {
        super("Unknown table");
        this.tableName = tableName;
    }

    @Override
    public int errorCode() {
        return 4041;
    }

    @Override
    public RestStatus status() {
        return RestStatus.NOT_FOUND;
    }

    @Override
    public Object[] args() {
        return new Object[]{tableName};
    }
}
