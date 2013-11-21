package org.cratedb.action;

import com.google.common.collect.MinMaxPriorityQueue;
import org.cratedb.action.groupby.GroupByRow;
import org.cratedb.action.groupby.GroupByRowComparator;
import org.cratedb.action.groupby.aggregate.AggExpr;
import org.cratedb.action.groupby.aggregate.AggFunction;
import org.cratedb.action.sql.OrderByColumnIdx;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class SQLReduceJobStatus {

    public final Integer limit;
    public final GroupByRowComparator comparator;
    public final Object lock = new Object();

    CountDownLatch shardsToProcess;
    SQLGroupByResult groupByResult;

    public SQLReduceJobStatus(int shardsToProcess,
                              Integer limit,
                              Integer[] idxMap,
                              OrderByColumnIdx[] orderByIndices,
                              Map<String, AggFunction> aggFunctionMap,
                              List<AggExpr> aggExprs)
    {
        this.limit = limit;
        this.groupByResult = new SQLGroupByResult(aggFunctionMap, aggExprs);
        this.shardsToProcess = new CountDownLatch(shardsToProcess);
        this.comparator = new GroupByRowComparator(idxMap, orderByIndices);
    }


    public GroupByRow[] toSortedArray(SQLGroupByResult groupByResult)
    {
        MinMaxPriorityQueue.Builder<GroupByRow> rowBuilder = MinMaxPriorityQueue.orderedBy(this.comparator);
        if (limit != null) {
            rowBuilder.maximumSize(limit);
        }

        MinMaxPriorityQueue<GroupByRow> q = rowBuilder.create();
        for (GroupByRow groupByRow : groupByResult.result) {
            q.add(groupByRow);
        }

        return q.toArray(new GroupByRow[q.size()]);
    }
}
