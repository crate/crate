package org.cratedb.sql;

import org.elasticsearch.rest.RestStatus;

public class GroupByOnArrayUnsupportedException extends CrateException {

    private final String columnName;

    public GroupByOnArrayUnsupportedException(String columnName) {
        super("Column \"" + columnName + "\" has a value that is an array. Group by doesn't work on Arrays");
        this.columnName = columnName;
    }

    @Override
    public int errorCode() {
        return 4000;
    }

    @Override
    public RestStatus status() {
        return RestStatus.BAD_REQUEST;
    }
}
