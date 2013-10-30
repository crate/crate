package org.cratedb.sql;

import org.elasticsearch.rest.RestStatus;

public class AnalyzerUnknownException extends CrateException {

    private String analyzerName;

    public AnalyzerUnknownException(String analyzerName, Throwable e) {
        super("Unknown analyzer", e);
        this.analyzerName = analyzerName;
    }

    public AnalyzerUnknownException(String analyzerName) {
        super("Unknown analyzer");
        this.analyzerName = analyzerName;
    }

    @Override
    public int errorCode() {
        return 4042;
    }

    @Override
    public RestStatus status() {
        return RestStatus.NOT_FOUND;
    }

    @Override
    public Object[] args() {
        return new Object[]{analyzerName};
    }
}
