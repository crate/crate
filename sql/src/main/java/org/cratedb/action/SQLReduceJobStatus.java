package org.cratedb.action;

import org.cratedb.action.groupby.GroupByHelper;
import org.cratedb.action.groupby.GroupByKey;
import org.cratedb.action.groupby.GroupByRow;
import org.cratedb.action.groupby.GroupByRowComparator;
import org.cratedb.action.groupby.key.Rows;
import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.core.collections.LimitingCollectionIterator;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class SQLReduceJobStatus {

    public final GroupByRowComparator comparator;
    public final Object lock = new Object();
    public final ParsedStatement parsedStatement;
    private Rows reducedRows;
    private final AtomicInteger failures = new AtomicInteger(0);
    CountDownLatch shardsToProcess;

    public SQLReduceJobStatus(ParsedStatement parsedStatement,
                              int shardsToProcess)
    {
        this.parsedStatement = parsedStatement;
        //this.reducedResult = ConcurrentCollections.newConcurrentMap();
        this.shardsToProcess = new CountDownLatch(shardsToProcess);
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
