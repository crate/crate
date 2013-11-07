package org.cratedb.action;

import com.google.common.collect.MinMaxPriorityQueue;
import org.cratedb.action.groupby.GroupByRow;
import org.cratedb.action.groupby.GroupByRowComparator;
import org.cratedb.action.sql.OrderByColumnIdx;

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

    public GroupByRow[] toSortedArray(SQLGroupByResult groupByResult)
    {
        MinMaxPriorityQueue.Builder<GroupByRow> rowBuilder = MinMaxPriorityQueue.orderedBy(this.comparator);
        if (limit != null) {
            rowBuilder.maximumSize(limit);
        }

        MinMaxPriorityQueue<GroupByRow> q = rowBuilder.create();
        for (GroupByRow groupByRow : groupByResult.result.values()) {
            q.add(groupByRow);
        }

        return q.toArray(new GroupByRow[q.size()]);
    }
}
