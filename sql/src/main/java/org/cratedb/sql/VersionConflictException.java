package org.cratedb.sql;

import org.elasticsearch.rest.RestStatus;

public class VersionConflictException extends CrateException {

    public VersionConflictException(Throwable e) {
        super(e);
    }

    @Override
    public int errorCode() {
        return 4092;
    }

    @Override
    public RestStatus status() {
        return RestStatus.CONFLICT;
    }
}
