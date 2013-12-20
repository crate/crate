package org.cratedb.sql;

import org.cratedb.action.ReduceJobContext;

import java.util.UUID;

public class SQLReduceJobFailedException extends CrateException {

    public SQLReduceJobFailedException(UUID contextId, ReduceJobContext status) {
        super(String.format("context: %s failed mappers: %s}", contextId, status.failures()));
    }
}
