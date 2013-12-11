package org.cratedb.action;

import org.cratedb.action.groupby.GroupByHelper;
import org.cratedb.action.groupby.GroupByKey;
import org.cratedb.action.groupby.GroupByRow;
import org.cratedb.action.groupby.GroupByRowComparator;
import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.core.concurrent.FutureConcurrentMap;
import org.cratedb.sql.CrateException;
import org.cratedb.sql.SQLReduceJobTimeoutException;
import org.elasticsearch.action.support.PlainListenableActionFuture;
import org.elasticsearch.cache.recycler.CacheRecycler;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class SQLReduceJobStatus extends PlainListenableActionFuture<SQLReduceJobResponse> {

    public final GroupByRowComparator comparator;
    public final Object lock = new Object();
    public final ParsedStatement parsedStatement;
    public final List<Integer> seenIdxMapper;
    public ConcurrentMap<GroupByKey, GroupByRow> reducedResult;
    private ReduceJobStatusContext reduceJobStatusContext;
    private UUID contextId;

    AtomicInteger shardsToProcess;

    public SQLReduceJobStatus(ParsedStatement parsedStatement,
                              ThreadPool threadPool,
                              ConcurrentMap<GroupByKey, GroupByRow> reducedResult,
                              int shardsToProcess,
                              UUID contextId,
                              ReduceJobStatusContext reduceJobStatusContext)
    {
        this(parsedStatement, threadPool, reducedResult);
        this.shardsToProcess = new AtomicInteger(shardsToProcess);
        this.reduceJobStatusContext = reduceJobStatusContext;
        this.contextId = contextId;
    }

    public SQLReduceJobStatus(ParsedStatement parsedStatement,
                              ThreadPool threadPool,
                              ConcurrentMap<GroupByKey, GroupByRow> reducedResult)
    {
        super(true, threadPool);
        this.parsedStatement = parsedStatement;
        this.seenIdxMapper = GroupByHelper.getSeenIdxMap(parsedStatement.aggregateExpressions);
        this.reducedResult = reducedResult;
        this.comparator = new GroupByRowComparator(
            GroupByHelper.buildFieldExtractor(parsedStatement, null),
            parsedStatement.orderByIndices()
        );
    }

    public Collection<GroupByRow> trimRows(Collection<GroupByRow> rows)
    {
        List<GroupByRow> rowList = new ArrayList<>(rows.size());
        for (GroupByRow row : rows) {
            row.terminatePartial();
        }
        rowList.addAll(rows);

        return GroupByHelper.trimRows(
            rowList, comparator, parsedStatement.totalLimit());
    }

    public void merge(SQLGroupByResult groupByResult) {
        GroupByRow existingRow;
        for (GroupByRow row : groupByResult.result()) {
            existingRow = reducedResult.putIfAbsent(row.key, row);
            if (existingRow == null) {
                continue;
            }
            existingRow.merge(row);
        }

        countDown();
    }

    private void countDown() {
        if (reduceJobStatusContext != null) {
            if (shardsToProcess.decrementAndGet() == 0) {
                reduceJobStatusContext.remove(contextId);
                set(new SQLReduceJobResponse(trimRows(reducedResult.values())));
            }
        }
    }

    public int getCount() {
        return shardsToProcess.get();
    }

    public void timeout() {
        if (!isDone()) {
            setException(new SQLReduceJobTimeoutException());
        }
    }
}
