package org.cratedb.action;

import org.cratedb.action.groupby.*;
import org.cratedb.action.sql.ParsedStatement;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;

public class SQLReduceJobStatus {

    public final GroupByRowComparator comparator;
    public final Object lock = new Object();
    public final ParsedStatement parsedStatement;
    public final RowSerializationContext rowSerializationContext;
    public final ConcurrentMap<GroupByKey, GroupByRow> reducedResult;

    CountDownLatch shardsToProcess;

    public SQLReduceJobStatus(ParsedStatement parsedStatement,
                              int shardsToProcess)
    {
        this(parsedStatement);
        this.shardsToProcess = new CountDownLatch(shardsToProcess);
    }

    public SQLReduceJobStatus(ParsedStatement parsedStatement)
    {
        this.parsedStatement = parsedStatement;
        this.rowSerializationContext = new RowSerializationContext(parsedStatement.aggregateExpressions);
        this.reducedResult = ConcurrentCollections.newConcurrentMap();
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
    }
}
