package org.cratedb.action;

import java.util.concurrent.CountDownLatch;

public class SQLReduceJobStatus {

    CountDownLatch shardsToProcess;
    SQLGroupByResult groupByResult;

    public SQLReduceJobStatus(int shardsToProcess) {
        this.groupByResult = new SQLGroupByResult();
        this.shardsToProcess = new CountDownLatch(shardsToProcess);
    }
}
