package org.cratedb.action;

import com.google.common.collect.MinMaxPriorityQueue;
import org.cratedb.action.groupby.GroupByRow;
import org.cratedb.action.groupby.GroupByRowComparator;
import org.cratedb.action.groupby.aggregate.AggExpr;
import org.cratedb.action.groupby.aggregate.AggFunction;
import org.cratedb.action.sql.OrderByColumnIdx;
import org.cratedb.action.sql.ParsedStatement;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class SQLReduceJobStatus {

    public final GroupByRowComparator comparator;
    public final Object lock = new Object();
    public final ParsedStatement parsedStatement;
    public final Map<String, AggFunction> aggFunctionMap;

    CountDownLatch shardsToProcess;
    SQLGroupByResult groupByResult;

    public SQLReduceJobStatus(ParsedStatement parsedStatement,
                              int shardsToProcess,
                              Map<String, AggFunction> aggFunctionMap)
    {
        this.parsedStatement = parsedStatement;
        this.aggFunctionMap = aggFunctionMap;
        this.groupByResult = new SQLGroupByResult(aggFunctionMap, parsedStatement.aggregateExpressions);
        this.shardsToProcess = new CountDownLatch(shardsToProcess);
        this.comparator = new GroupByRowComparator(parsedStatement.idxMap, parsedStatement.orderByIndices());
    }

    public Collection<GroupByRow> sortGroupByResult(SQLGroupByResult groupByResult)
    {
        MinMaxPriorityQueue.Builder<GroupByRow> rowBuilder = MinMaxPriorityQueue.orderedBy(this.comparator);
        if (parsedStatement.limit != null) {
            int limit = parsedStatement.limit;
            if (parsedStatement.offset != null) {
                limit += parsedStatement.offset;
            }
            rowBuilder.maximumSize(limit);
        }

        MinMaxPriorityQueue<GroupByRow> q = rowBuilder.create();
        for (GroupByRow groupByRow : groupByResult.result) {
            q.add(groupByRow);
        }

        return q;
    }
}
