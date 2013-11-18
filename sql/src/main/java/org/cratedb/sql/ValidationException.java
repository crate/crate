package org.cratedb.sql;

import org.elasticsearch.rest.RestStatus;

public class ValidationException extends CrateException {

    private final String column;

    public ValidationException(String column, String message) {
        super(String.format("Validation failed for %s: %s", column, message));
        this.column = column;
    }

    public ValidationException(String column, Throwable e) {
        super(String.format("Validation failed for %s: %s", column, e.getMessage()));
        this.column = column;
    }

    @Override
    public int errorCode() {
        return 4003;
    }

    @Override
    public RestStatus status() {
        return RestStatus.BAD_REQUEST;
    }

    @Override
    public Object[] args() {
        return new Object[]{column};
    }
}
