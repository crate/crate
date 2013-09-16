package org.cratedb.sql;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.rest.RestStatus;

public class SQLParseException extends ElasticSearchException {

    public SQLParseException(String msg) {
        super(msg);
    }

    public SQLParseException(String msg, Exception e) {
        super(msg, e);
    }

    @Override
    public RestStatus status() {
        return RestStatus.BAD_REQUEST;
    }
}
