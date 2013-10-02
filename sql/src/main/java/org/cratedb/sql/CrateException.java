package org.cratedb.sql;

import org.elasticsearch.ElasticSearchException;

public class CrateException extends ElasticSearchException {

    public CrateException(String msg) {
        super(msg);
    }

    public CrateException(String msg, Throwable e) {
        super(msg, e);
    }

    public CrateException(Throwable e) {
         super(e.getMessage(), e);
    }

    public int errorCode() {
        return 1000;
    }

    public Object[] args() {
        return new Object[0];
    }
}
