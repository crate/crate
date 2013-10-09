package org.cratedb.sql;

public class SQLReduceJobTimeoutException extends CrateException {

    public SQLReduceJobTimeoutException() {
        super("Reduce Job didn't finish within the specified timeout.");
    }
}
