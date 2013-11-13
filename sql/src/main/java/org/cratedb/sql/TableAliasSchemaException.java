package org.cratedb.sql;

import org.elasticsearch.rest.RestStatus;

public class TableAliasSchemaException extends CrateException {

    private String tableName;

    public TableAliasSchemaException(String tableName, Throwable e) {
        super("Table alias contains tables with different schema", e);
        this.tableName = tableName;
    }

    public TableAliasSchemaException(String tableName) {
        super("Table alias contains tables with different schema");
        this.tableName = tableName;
    }

    @Override
    public int errorCode() {
        return 4094;
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
