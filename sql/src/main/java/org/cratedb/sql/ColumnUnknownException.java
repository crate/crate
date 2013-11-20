package org.cratedb.sql;

import org.elasticsearch.rest.RestStatus;

public class ColumnUnknownException extends CrateException {
    private final String tableName;
    private final String columnName;

    public ColumnUnknownException(String columnName) {
        super("Column unknwon");
        this.tableName = null;
        this.columnName = columnName;
    }

    public ColumnUnknownException(String columnName, Throwable e) {
        super("Column unknown", e);
        this.tableName = null;
        this.columnName = columnName;
    }

    public ColumnUnknownException(String tableName, String columnName) {
        super("Column unknown");
        this.tableName = tableName;
        this.columnName = columnName;
    }

    public ColumnUnknownException(String tableName, String columnName, Throwable e) {
        super("Column unknown", e);
        this.tableName = tableName;
        this.columnName = columnName;
    }

    @Override
    public int errorCode() {
        return 4043;
    }

    @Override
    public RestStatus status() {
        return RestStatus.BAD_REQUEST;
    }

    @Override
    public Object[] args() {
        return new Object[]{tableName, columnName};
    }
}
