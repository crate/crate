package org.cratedb.action;

import org.cratedb.action.groupby.GroupByHelper;
import org.cratedb.action.groupby.GroupByRow;
import org.cratedb.action.groupby.GroupByRowComparator;
import org.cratedb.action.groupby.key.Rows;
import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.sql.SQLReduceJobTimeoutException;
import org.elasticsearch.action.support.PlainListenableActionFuture;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.UUID;

public class ReduceJobContext extends PlainListenableActionFuture<SQLReduceJobResponse> {

    public final GroupByRowComparator comparator;
    public final Object lock = new Object();
    public final ParsedStatement parsedStatement;

    private Rows reducedRows;
    private final AtomicInteger failures = new AtomicInteger(0);

    private ReduceJobRequestContext reduceJobRequestContext;
    private UUID contextId;

    AtomicInteger shardsToProcess;

    public ReduceJobContext(ParsedStatement parsedStatement, ThreadPool threadPool, int shardsToProcess) {
        this(parsedStatement, threadPool, shardsToProcess, null, null);
    }

    public ReduceJobContext(ParsedStatement parsedStatement,
                            ThreadPool threadPool,
                            int shardsToProcess,
                            @Nullable UUID contextId,
                            @Nullable ReduceJobRequestContext reduceJobRequestContext) {
        super(false, threadPool);
        this.parsedStatement = parsedStatement;
        this.shardsToProcess = new AtomicInteger(shardsToProcess);
        this.contextId = contextId;
        this.reduceJobRequestContext = reduceJobRequestContext;
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
                    if (parsedStatement.reducerHasRowAuthority()) {
                        row.terminatePartial();
                    }
                    rowList.add(row);
                }
            });
        }
        return GroupByHelper.trimRows(rowList, comparator, parsedStatement.totalLimit());
    }

    public void merge(SQLGroupByResult groupByResult) {
        synchronized (lock) {
            if (reducedRows==null){
                reducedRows = groupByResult.rows();
            } else {
                reducedRows.merge(groupByResult.rows());
            }
        }

        countDown();
    }

    private void countDown() {
        if (shardsToProcess.decrementAndGet() == 0) {
            if (reduceJobRequestContext != null) {
                reduceJobRequestContext.remove(contextId);
            }
            set(new SQLReduceJobResponse(terminate(), parsedStatement));
        }
    }

    public void timeout() {
        if (!isDone()) {
            setException(new SQLReduceJobTimeoutException());
        }
    }

    public void countFailure() {
        failures.incrementAndGet();
        countDown();
    }

    public int failures() {
        return failures.get();
    }
}
