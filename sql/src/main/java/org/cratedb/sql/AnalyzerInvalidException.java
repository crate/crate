package org.cratedb.sql;

import org.elasticsearch.rest.RestStatus;

public class AnalyzerInvalidException extends CrateException {

    public AnalyzerInvalidException(String reason, Throwable e) {
        super(reason, e);
    }

    public AnalyzerInvalidException(String reason) {
        super(reason);
    }

    @Override
    public int errorCode() {
        return 4001;
    }

    @Override
    public RestStatus status() {
        return RestStatus.BAD_REQUEST;
    }

}
