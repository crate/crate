package org.cratedb.sql;

import org.cratedb.action.SQLReduceJobStatus;

import java.util.UUID;

public class SQLReduceJobFailedException extends CrateException {

    public SQLReduceJobFailedException(UUID contextId, SQLReduceJobStatus status) {
        super(String.format("context: %s failed mappers: %s}", contextId, status.failures()));
    }
}
