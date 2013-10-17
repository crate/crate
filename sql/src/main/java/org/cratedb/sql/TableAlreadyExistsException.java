package org.cratedb.sql;

import org.elasticsearch.rest.RestStatus;

/**
 * Created with IntelliJ IDEA.
 * User: max
 * Date: 10/17/13
 * Time: 8:38 AM
 * To change this template use File | Settings | File Templates.
 */
public class TableAlreadyExistsException extends CrateException {

    public TableAlreadyExistsException(String msg, Throwable e) {
        super(msg, e);
    }

    @Override
    public int errorCode() {
        return 4090;
    }

    @Override
    public RestStatus status() {
        return RestStatus.CONFLICT;
    }

}
