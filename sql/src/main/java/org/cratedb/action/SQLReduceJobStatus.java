package org.cratedb.action;

import org.cratedb.action.groupby.GroupByRow;
import org.cratedb.action.groupby.GroupByRowComparator;
import org.cratedb.action.sql.OrderByColumnIdx;

import java.util.PriorityQueue;
import java.util.concurrent.CountDownLatch;

public class SQLReduceJobStatus {

    public final Integer limit;
    public final GroupByRowComparator comparator;
    public final Object lock = new Object();
    CountDownLatch shardsToProcess;
    SQLGroupByResult groupByResult;

    public SQLReduceJobStatus(int shardsToProcess,
                              Integer limit,
                              OrderByColumnIdx[] orderByIndices)
    {
        this.limit = limit;
        this.groupByResult = new SQLGroupByResult();
        this.shardsToProcess = new CountDownLatch(shardsToProcess);
        this.comparator = new GroupByRowComparator(orderByIndices);
    }

    public GroupByRow[] toSortedArray(SQLGroupByResult groupByResult,
                                                   GroupByRowComparator comparator)
    {
        PriorityQueue<GroupByRow> q;
        if (limit != null) {
            q = new PriorityQueue<>(limit, comparator);
        } else {
            q = new PriorityQueue<>(1000, comparator);
        }

        int pos = -1;
        for (GroupByRow groupByRow : groupByResult.result.values()) {
            pos++;
            q.add(groupByRow);
            if (limit != null && pos == limit) {
                break;
            }
        }

        return q.toArray(new GroupByRow[q.size()]);
    }
}
