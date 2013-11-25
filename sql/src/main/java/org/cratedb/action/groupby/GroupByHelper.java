package org.cratedb.action.groupby;

import com.google.common.collect.MinMaxPriorityQueue;
import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.service.SQLParseService;

import java.util.Collection;
import java.util.Comparator;

public class GroupByHelper {

    public static MinMaxPriorityQueue<GroupByRow> sortRows(Collection<GroupByRow> rows,
                                                  Comparator<GroupByRow> comparator,
                                                  Integer limit,
                                                  Integer offset) {

        MinMaxPriorityQueue.Builder<GroupByRow> rowBuilder = MinMaxPriorityQueue.orderedBy(comparator);
        int nonNullOffset = (offset != null) ? offset : 0;

        if (limit != null) {
            rowBuilder.maximumSize(limit + nonNullOffset);
        } else {
            rowBuilder.maximumSize(SQLParseService.DEFAULT_SELECT_LIMIT + nonNullOffset);
        }

        MinMaxPriorityQueue<GroupByRow> q = rowBuilder.create();
        q.addAll(rows);

        return q;
    }

    public static Object[][] sortedRowsToObjectArray(MinMaxPriorityQueue<GroupByRow> rows,
                                                     ParsedStatement parsedStatement,
                                                     int offset) {
        Object[][] result = new Object[rows.size() - offset][parsedStatement.outputFields().size()];
        int currentRow = -1;
        int remainingOffset = offset;

        GroupByRow row;
        while ((row = rows.pollFirst()) != null) {
            if (remainingOffset > 0) {
                remainingOffset--;
                continue;
            }

            currentRow++;
            for (int i = 0; i < result[currentRow].length; i++) {
                result[currentRow][i] = row.get(parsedStatement.idxMap[i]);
            }
        }

        return result;
    }
}
