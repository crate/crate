package org.cratedb.action;

import org.cratedb.action.groupby.GroupByHelper;
import org.cratedb.action.groupby.GroupByRow;
import org.cratedb.action.groupby.GroupByRowComparator;
import org.cratedb.action.groupby.key.Rows;
import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.sql.SQLReduceJobTimeoutException;
import org.elasticsearch.action.support.PlainListenableActionFuture;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.UUID;

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
        if (reducedRows != null) {
            reducedRows.walk(new Rows.RowVisitor() {
                @Override
                public void visit(GroupByRow row) {
                    row.terminatePartial();
                    rowList.add(row);
                }
            });
        }
        return GroupByHelper.trimRows(rowList, comparator, parsedStatement.totalLimit());
    }

    public synchronized void merge(SQLGroupByResult groupByResult) {
        if (reducedRows==null){
            reducedRows = groupByResult.rows();
        } else {
            reducedRows.merge(groupByResult.rows());
        }

        countDown();
    }

    /**
     * countdown the number of shards to process.
     * called implicitly from {@link #merge(SQLGroupByResult)}
     */
    public void countDown() {
        if (reduceJobStatusContext != null) {
            if (shardsToProcess.decrementAndGet() == 0) {
                reduceJobStatusContext.remove(contextId);
                set(new SQLReduceJobResponse(terminate(), parsedStatement));
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

    public int failures() {
        return failures.get();
    }
}
