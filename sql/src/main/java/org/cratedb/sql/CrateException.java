package org.cratedb.sql;

import org.elasticsearch.ElasticSearchException;

public abstract class CrateException extends ElasticSearchException {

    public CrateException(String msg) {
        super(msg);
    }

    public CrateException(String msg, Exception e) {
        super(msg, e);
    }

    public abstract int errorCode();
    public Object[] args() {
        return new Object[0];
    }
}
