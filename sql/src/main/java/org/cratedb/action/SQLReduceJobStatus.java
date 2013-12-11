package org.cratedb.action;

import org.cratedb.action.groupby.GroupByHelper;
import org.cratedb.action.groupby.GroupByKey;
import org.cratedb.action.groupby.GroupByRow;
import org.cratedb.action.groupby.GroupByRowComparator;
import org.cratedb.action.groupby.key.Rows;
import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.core.collections.LimitingCollectionIterator;
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
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class SQLReduceJobStatus extends PlainListenableActionFuture<SQLReduceJobResponse> {

    public final GroupByRowComparator comparator;
    public final Object lock = new Object();
    public final ParsedStatement parsedStatement;

    private Rows reducedRows;
    private final AtomicInteger failures = new AtomicInteger(0);

    private ReduceJobStatusContext reduceJobStatusContext;
    private UUID contextId;

    AtomicInteger shardsToProcess;

    public SQLReduceJobStatus(ParsedStatement parsedStatement,
                              ThreadPool threadPool,
                              int shardsToProcess,
                              UUID contextId,
                              ReduceJobStatusContext reduceJobStatusContext)
    {
        this(parsedStatement, threadPool);
        this.shardsToProcess = new AtomicInteger(shardsToProcess);
        this.reduceJobStatusContext = reduceJobStatusContext;
        this.contextId = contextId;
    }

    public SQLReduceJobStatus(ParsedStatement parsedStatement,
                              ThreadPool threadPool)
    {
        super(true, threadPool);
        this.parsedStatement = parsedStatement;
        this.comparator = new GroupByRowComparator(
            GroupByHelper.buildFieldExtractor(parsedStatement, null),
            parsedStatement.orderByIndices()
        );
    }

    public Collection<GroupByRow> terminate(){
        final List<GroupByRow> rowList = new ArrayList<>();
        reducedRows.walk(new Rows.RowVisitor() {
            @Override
            public void visit(GroupByRow row) {
                row.terminatePartial();
                rowList.add(row);
            }
        });
        return GroupByHelper.trimRows(
                rowList, comparator, parsedStatement.totalLimit());
    }

    public synchronized void merge(SQLGroupByResult groupByResult) {
        if (reducedRows==null){
            reducedRows = groupByResult.rows();
        } else {
            reducedRows.merge(groupByResult.rows());
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

    public void failure() {
        failures.incrementAndGet();
    }

    public int failures(){
        return failures.get();
    }

    public boolean hasFailures(){
        return failures.get()>0;
    }

}
